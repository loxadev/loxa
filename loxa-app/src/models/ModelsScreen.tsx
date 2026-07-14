import { useEffect, useMemo, useRef, useState } from "react";

import type {
  cancelOperation as defaultCancelOperation,
  downloadModel as defaultDownloadModel,
  getControlNode as defaultGetControlNode,
  getInventory as defaultGetInventory,
  getOperation as defaultGetOperation,
  loadModel as defaultLoadModel,
  unloadModel as defaultUnloadModel,
} from "../control/client";
import type { ControlStreamHandle, streamControlEvents as defaultStreamControlEvents } from "../control/events";
import type {
  ArtifactState,
  ArtifactInvalidReason,
  ModelInventoryEntry,
  NodeSnapshot,
  OperationStatus,
  OperationView,
} from "../control/contracts";
import styles from "./ModelsScreen.module.css";

export type ModelsScreenServices = {
  readControlToken(endpoint: string): Promise<string>;
  getControlNode: typeof defaultGetControlNode;
  getInventory: typeof defaultGetInventory;
  downloadModel: typeof defaultDownloadModel;
  loadModel: typeof defaultLoadModel;
  unloadModel: typeof defaultUnloadModel;
  getOperation: typeof defaultGetOperation;
  cancelOperation: typeof defaultCancelOperation;
  createControlEventStream: typeof defaultStreamControlEvents;
};

type LiveState = "connecting" | "live" | "reconnecting" | "error";

export function ModelsScreen({
  endpoint,
  services,
  reconnectDelayMs = 1_000,
  verificationPollMs = 2_000,
  verificationPollLimit = 6,
  reconnectLimit = 6,
  onModelMutationStart,
  onModelMutationSettled,
}: {
  endpoint: string;
  services: ModelsScreenServices;
  reconnectDelayMs?: number;
  verificationPollMs?: number;
  verificationPollLimit?: number;
  reconnectLimit?: number;
  onModelMutationStart?: (operationId: string) => void;
  onModelMutationSettled?: (operationId: string) => void | Promise<void>;
}) {
  const [models, setModels] = useState<ModelInventoryEntry[]>([]);
  const [inventoryLoaded, setInventoryLoaded] = useState(false);
  const [node, setNode] = useState<NodeSnapshot | null>(null);
  const [operations, setOperations] = useState<Record<string, OperationView>>({});
  const [pendingModels, setPendingModels] = useState<Set<string>>(() => new Set());
  const [liveState, setLiveState] = useState<LiveState>("connecting");
  const [notice, setNotice] = useState("Connecting to model controls");
  const [error, setError] = useState("");
  const [retryNonce, setRetryNonce] = useState(0);
  const cursorRef = useRef(0);
  const streamRef = useRef<ControlStreamHandle | null>(null);
  const lifetimeSignalRef = useRef<AbortSignal | null>(null);
  const activeRef = useRef(true);
  const nodeRevisionRef = useRef(0);

  useEffect(() => {
    const controller = new AbortController();
    let disposed = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let verificationTimer: ReturnType<typeof setTimeout> | undefined;
    let verificationPending = false;
    let verificationAttempts = 0;
    let reconnectAttempts = 0;
    activeRef.current = true;
    lifetimeSignalRef.current = controller.signal;

    const replaceOperations = (next: OperationView[]) => {
      if (disposed) return;
      setOperations(Object.fromEntries(next.map((operation) => [operation.id, operation])));
    };
    const applyOperation = (operation: OperationView) => {
      if (disposed) return;
      setOperations((current) => ({ ...current, [operation.id]: operation }));
    };

    const scheduleVerificationPoll = () => {
      if (disposed || controller.signal.aborted || !verificationPending) return;
      if (verificationTimer !== undefined) clearTimeout(verificationTimer);
      if (verificationAttempts >= verificationPollLimit) {
        setNotice("Model verification is still pending. Refresh Models or wait for a new node update to check again");
        return;
      }
      const backoff = Math.min(30_000, verificationPollMs * 2 ** Math.min(verificationAttempts, 4));
      verificationTimer = setTimeout(() => {
        verificationTimer = undefined;
        if (disposed || !verificationPending) return;
        verificationAttempts += 1;
        void services
          .readControlToken(endpoint)
          .then((token) => services.getInventory(endpoint, token, { signal: controller.signal }))
          .then((next) => {
            if (disposed) return;
            publishModels(next);
          })
          .catch(() => {
            if (disposed || controller.signal.aborted) return;
            setNotice("Model verification is still pending. Checking again shortly");
            scheduleVerificationPoll();
          });
      }, backoff);
    };

    const publishModels = (next: ModelInventoryEntry[]) => {
      if (disposed) return;
      setModels(next);
      setInventoryLoaded(true);
      verificationPending = next.some(
        (entry) => entry.artifact.kind === "invalid" && entry.artifact.reason === "verification_required",
      );
      if (!verificationPending && verificationTimer !== undefined) {
        clearTimeout(verificationTimer);
        verificationTimer = undefined;
        verificationAttempts = 0;
      } else if (verificationPending) {
        scheduleVerificationPoll();
      }
    };

    const refreshTruth = async (version: number) => {
      const [nextModels, nextNode] = await Promise.all([
        services
          .readControlToken(endpoint)
          .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
        services
          .readControlToken(endpoint)
          .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
      ]);
      if (disposed || version !== nodeRevisionRef.current) return;
      verificationAttempts = 0;
      publishModels(nextModels);
      setNode(nextNode);
    };

    const connectEvents = async () => {
      const token = await services.readControlToken(endpoint);
      if (disposed) return;
      setLiveState((current) => (current === "reconnecting" ? "reconnecting" : "connecting"));
      streamRef.current = services.createControlEventStream(
        endpoint,
        token,
        cursorRef.current,
        {
          onSnapshot: (snapshot) => {
            if (disposed) return;
            cursorRef.current = snapshot.cursor;
            replaceOperations(snapshot.operations);
            snapshot.operations
              .filter(isActiveLifecycleOperation)
              .forEach((operation) => onModelMutationStart?.(operation.id));
            snapshot.operations.filter(isTerminalLifecycleOperation).forEach((operation) => {
              void onModelMutationSettled?.(operation.id);
            });
            setLiveState("live");
            setError("");
            setNotice(
              snapshot.cursorGap
                ? "Live updates restored; older compacted events were replaced by the current snapshot"
                : "Live model updates connected",
            );
          },
          onEvent: (event) => {
            if (disposed) return;
            if (event.sequence <= cursorRef.current) return;
            cursorRef.current = event.sequence;
            applyOperation(event.operation);
            setNotice(operationAnnouncement(event.operation));
            if (isActiveLifecycleOperation(event.operation)) onModelMutationStart?.(event.operation.id);
            if (isTerminal(event.operation.status)) {
              const nodeVersion = ++nodeRevisionRef.current;
              void refreshTruth(nodeVersion)
                .catch((reason: unknown) => {
                  if (!disposed && !controller.signal.aborted) setError(message(reason));
                })
                .finally(() => {
                  if (!disposed && (event.operation.kind === "load" || event.operation.kind === "unload")) {
                    void onModelMutationSettled?.(event.operation.id);
                  }
                });
            }
          },
          onTerminal: (terminal) => {
            if (disposed || terminal.kind === "cancelled") return;
            cursorRef.current = terminal.cursor;
            setLiveState("reconnecting");
            setNotice("Live model updates disconnected. Reconnecting");
            scheduleReconnect();
          },
        },
        controller.signal,
      );
    };

    function scheduleReconnect() {
      if (disposed || controller.signal.aborted) return;
      if (reconnectTimer !== undefined) clearTimeout(reconnectTimer);
      if (reconnectAttempts >= reconnectLimit) {
        setLiveState("error");
        setNotice("Live model updates stopped after repeated failures. Retry when the node is available");
        return;
      }
      const delay = Math.min(30_000, reconnectDelayMs * 2 ** Math.min(reconnectAttempts, 4));
      reconnectTimer = setTimeout(() => {
        if (disposed) return;
        reconnectAttempts += 1;
        void Promise.all([
          services
            .readControlToken(endpoint)
            .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
          services
            .readControlToken(endpoint)
            .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
        ])
          .then(([nextNode, nextModels]) => {
            if (disposed) return;
            setNode(nextNode);
            nodeRevisionRef.current += 1;
            verificationAttempts = 0;
            publishModels(nextModels);
            setError("");
            void connectEvents().catch(handleReconnectFailure);
          })
          .catch((reason: unknown) => {
            handleReconnectFailure(reason);
          });
      }, delay);
    }

    function handleReconnectFailure(reason: unknown) {
      if (disposed || controller.signal.aborted) return;
      setError(message(reason));
      setLiveState("reconnecting");
      setNotice("Node is temporarily unavailable. Reconnecting");
      scheduleReconnect();
    }

    void Promise.all([
      services
        .readControlToken(endpoint)
        .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
      services
        .readControlToken(endpoint)
        .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
    ])
      .then(async ([nextNode, nextModels]) => {
        if (disposed) return;
        setNode(nextNode);
        nodeRevisionRef.current += 1;
        publishModels(nextModels);
        await connectEvents();
      })
      .catch((reason: unknown) => {
        if (disposed || controller.signal.aborted) return;
        setError(message(reason));
        setLiveState("error");
        setNotice("Model controls unavailable");
      });

    const disposeWork = () => {
      if (disposed) return;
      disposed = true;
      activeRef.current = false;
      controller.abort();
      if (reconnectTimer !== undefined) clearTimeout(reconnectTimer);
      if (verificationTimer !== undefined) clearTimeout(verificationTimer);
      streamRef.current?.dispose();
      streamRef.current = null;
      lifetimeSignalRef.current = null;
    };
    window.addEventListener("beforeunload", disposeWork);
    return () => {
      window.removeEventListener("beforeunload", disposeWork);
      disposeWork();
    };
  }, [
    endpoint,
    onModelMutationSettled,
    onModelMutationStart,
    reconnectDelayMs,
    reconnectLimit,
    retryNonce,
    services,
    verificationPollLimit,
    verificationPollMs,
  ]);

  const latestByModel = useMemo(() => {
    const latest = new Map<string, OperationView>();
    for (const operation of Object.values(operations)) {
      if (operation.modelId === null) continue;
      const current = latest.get(operation.modelId);
      if (current === undefined || operation.updatedAtUnixMs >= current.updatedAtUnixMs) {
        latest.set(operation.modelId, operation);
      }
    }
    return latest;
  }, [operations]);

  const download = async (modelId: string) => {
    const signal = lifetimeSignalRef.current;
    if (signal === null || signal.aborted) return;
    setPendingModels((current) => withValue(current, modelId, true));
    setError("");
    try {
      const downloadToken = await services.readControlToken(endpoint);
      const accepted = await services.downloadModel(endpoint, downloadToken, modelId, { signal });
      if (!activeRef.current || signal.aborted) return;
      const operationToken = await services.readControlToken(endpoint);
      const authoritative = await services.getOperation(endpoint, operationToken, accepted.operationId, { signal });
      if (!activeRef.current || signal.aborted) return;
      setOperations((current) => ({ ...current, [authoritative.id]: authoritative }));
      setNotice(operationAnnouncement(authoritative));
    } catch (reason) {
      if (activeRef.current && !signal.aborted) setError(message(reason));
    } finally {
      if (activeRef.current) setPendingModels((current) => withValue(current, modelId, false));
    }
  };

  const cancel = async (operation: OperationView, modelId: string) => {
    const signal = lifetimeSignalRef.current;
    if (signal === null || signal.aborted) return;
    setPendingModels((current) => withValue(current, modelId, true));
    setError("");
    try {
      const token = await services.readControlToken(endpoint);
      const authoritative = await services.cancelOperation(endpoint, token, operation.id, { signal });
      if (!activeRef.current || signal.aborted) return;
      setOperations((current) => ({ ...current, [authoritative.id]: authoritative }));
      setNotice(operationAnnouncement(authoritative));
    } catch (reason) {
      if (activeRef.current && !signal.aborted) setError(message(reason));
    } finally {
      if (activeRef.current) {
        setPendingModels((current) => withValue(current, modelId, false));
      }
    }
  };

  const startLifecycle = async (kind: "load" | "unload", modelId: string) => {
    const signal = lifetimeSignalRef.current;
    if (signal === null || signal.aborted || node?.status === "recovery_required") return;
    setPendingModels((current) => withValue(current, modelId, true));
    setError("");
    try {
      const nodeRevision = nodeRevisionRef.current;
      const token = await services.readControlToken(endpoint);
      const accepted =
        kind === "load"
          ? await services.loadModel(endpoint, token, modelId, { signal })
          : await services.unloadModel(endpoint, token, { signal });
      onModelMutationStart?.(accepted.operationId);
      if (!activeRef.current || signal.aborted) return;
      const operationToken = await services.readControlToken(endpoint);
      const nodeToken = await services.readControlToken(endpoint);
      const [authoritative, nextNode] = await Promise.all([
        services.getOperation(endpoint, operationToken, accepted.operationId, { signal }),
        services.getControlNode(endpoint, nodeToken, { signal }),
      ]);
      if (!activeRef.current || signal.aborted) return;
      setOperations((current) => {
        const existing = current[authoritative.id];
        return existing !== undefined && existing.updatedAtUnixMs >= authoritative.updatedAtUnixMs
          ? current
          : { ...current, [authoritative.id]: authoritative };
      });
      if (nodeRevisionRef.current === nodeRevision) setNode(nextNode);
      setNotice(operationAnnouncement(authoritative));
      if (isTerminal(authoritative.status)) await onModelMutationSettled?.(authoritative.id);
    } catch (reason) {
      if (activeRef.current && !signal.aborted) setError(message(reason));
    } finally {
      if (activeRef.current) setPendingModels((current) => withValue(current, modelId, false));
    }
  };

  const activeUnload =
    node?.activeModelId === null
      ? undefined
      : Object.values(operations)
          .filter((operation) => operation.kind === "unload" && operation.id === node?.operationId)
          .sort((left, right) => right.updatedAtUnixMs - left.updatedAtUnixMs)[0];
  const mutationBusy =
    pendingModels.size > 0 ||
    node?.operationId !== null ||
    Object.values(operations).some((operation) => operation.status === "queued" || operation.status === "running");

  return (
    <section className={styles.screen} aria-labelledby="models-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Verified registry</p>
          <h1 id="models-heading">Models</h1>
          <p className="screen-summary">Download Loxa-tested recipes. Downloading never loads or switches a model.</p>
        </div>
        <p className={`status-badge live-${liveState}`} role="status" aria-live="polite">
          {liveLabel(liveState)}
        </p>
      </header>

      <div className={styles.toolbar} aria-label="Model control summary">
        <span>
          Node <strong>{node === null ? "Checking" : nodeStatusLabel(node)}</strong>
        </span>
        <span>
          Endpoint <span className="technical-value">{endpoint}</span>
        </span>
        <span>{models.length} verified recipes</span>
      </div>

      {error && (
        <p className={styles.panel} role="alert">
          {error}
        </p>
      )}
      {node?.status === "recovery_required" && (
        <p className={styles.panel} role="alert">
          Recovery required. Model and chat controls are blocked until the node is safely restarted.
        </p>
      )}
      {liveState === "error" && (
        <button
          className="secondary-button interactive-target"
          type="button"
          onClick={() => setRetryNonce((value) => value + 1)}
        >
          Retry live updates
        </button>
      )}
      {!error && models.length === 0 && (
        <p className={styles.empty}>
          {inventoryLoaded ? "No verified recipes are available in this build." : "Checking the known model registry…"}
        </p>
      )}
      <div className={styles.list}>
        {models.map((entry) => (
          <ModelRow
            key={entry.id}
            entry={entry}
            operation={latestByModel.get(entry.id)}
            unloadOperation={node?.activeModelId === entry.id ? activeUnload : undefined}
            pending={pendingModels.has(entry.id)}
            active={node?.activeModelId === entry.id}
            node={node}
            mutationBusy={mutationBusy}
            onDownload={() => void download(entry.id)}
            onLoad={() => void startLifecycle("load", entry.id)}
            onUnload={() => void startLifecycle("unload", entry.id)}
            onCancel={(operation) => void cancel(operation, entry.id)}
          />
        ))}
      </div>
      <p className="visually-hidden" aria-live="polite">
        {notice}
      </p>
    </section>
  );
}

function isActiveLifecycleOperation(operation: OperationView): boolean {
  return (
    (operation.kind === "load" || operation.kind === "unload") &&
    (operation.status === "queued" || operation.status === "running")
  );
}

function isTerminalLifecycleOperation(operation: OperationView): boolean {
  return (operation.kind === "load" || operation.kind === "unload") && isTerminal(operation.status);
}

function ModelRow({
  entry,
  operation,
  unloadOperation,
  pending,
  active,
  node,
  mutationBusy,
  onDownload,
  onLoad,
  onUnload,
  onCancel,
}: {
  entry: ModelInventoryEntry;
  operation?: OperationView;
  unloadOperation?: OperationView;
  pending: boolean;
  active: boolean;
  node: NodeSnapshot | null;
  mutationBusy: boolean;
  onDownload(): void;
  onLoad(): void;
  onUnload(): void;
  onCancel(operation: OperationView): void;
}) {
  const headingId = `model-${entry.id}`;
  const reasonId = `model-reason-${entry.id}`;
  const actionable = entry.compatibility.compatible && entry.engine.eligible;
  const displayedOperation = unloadOperation ?? operation;
  const inProgress = displayedOperation?.status === "queued" || displayedOperation?.status === "running";
  const status =
    inProgress && displayedOperation
      ? operationLabel(displayedOperation)
      : artifactLabel(entry.artifact, entry.sizeBytes);
  const showDownload =
    !inProgress &&
    entry.artifact.kind !== "downloaded" &&
    !(entry.artifact.kind === "invalid" && entry.artifact.reason === "verification_required");
  const actionLabel =
    entry.artifact.kind === "partial"
      ? `Resume ${entry.id}`
      : entry.artifact.kind === "invalid"
        ? `Repair ${entry.id}`
        : `Download ${entry.id}`;

  return (
    <article className={styles.row} aria-labelledby={headingId}>
      <div className={styles.main}>
        <div className={styles.headingLine}>
          <h2 id={headingId}>{entry.id}</h2>
          {active && <span className={`${styles.chip} ${styles.activeChip}`}>Active</span>}
          <span className={styles.chip}>{status}</span>
        </div>
        <p className={styles.metadata}>
          <span>{entry.params}</span>
          <span>{entry.quant}</span>
          <span>{formatBytes(entry.sizeBytes)}</span>
          <span>{entry.license}</span>
          <span>{entry.engine.engine}</span>
        </p>
        <p className={`technical-value ${styles.repository}`}>
          {entry.repo}@{entry.revision}
        </p>
        <p id={reasonId} className={actionable ? styles.reason : `${styles.reason} ${styles.reasonBlocking}`}>
          {entry.compatibility.reason} {entry.engine.reason}
        </p>
        {displayedOperation?.progress && (
          <div className={styles.progress}>
            {displayedOperation.progress.totalBytes === null ? (
              <progress aria-label={`Download progress for ${entry.id}`} />
            ) : (
              <progress
                aria-label={`Download progress for ${entry.id}`}
                value={displayedOperation.progress.completedBytes}
                max={displayedOperation.progress.totalBytes}
              />
            )}
            <span className="technical-value">
              {formatBytes(displayedOperation.progress.completedBytes)}
              {displayedOperation.progress.totalBytes === null
                ? " downloaded"
                : ` of ${formatBytes(displayedOperation.progress.totalBytes)}`}
            </span>
          </div>
        )}
        {displayedOperation?.error && <p className={styles.operationError}>{displayedOperation.error}</p>}
        {displayedOperation && !inProgress && (
          <p className={styles.operationHistory}>Last operation: {operationLabel(displayedOperation)}</p>
        )}
      </div>
      <div className={styles.actions}>
        {inProgress && displayedOperation?.kind === "download" ? (
          <button
            className="secondary-button interactive-target"
            type="button"
            disabled={pending}
            onClick={() => onCancel(displayedOperation)}
            aria-label={`Cancel ${displayedOperation.kind} ${entry.id}`}
          >
            Cancel
          </button>
        ) : inProgress && displayedOperation ? (
          <span className={styles.actionLabel}>{operationLabel(displayedOperation)}</span>
        ) : showDownload ? (
          <button
            className="primary-button interactive-target"
            type="button"
            disabled={!actionable || pending || mutationBusy}
            aria-describedby={reasonId}
            aria-label={actionLabel}
            onClick={onDownload}
          >
            {entry.artifact.kind === "partial" ? "Resume" : entry.artifact.kind === "invalid" ? "Repair" : "Download"}
          </button>
        ) : entry.artifact.kind === "downloaded" && actionable && node?.status !== "recovery_required" ? (
          <button
            className={active ? "secondary-button interactive-target" : "primary-button interactive-target"}
            type="button"
            disabled={pending || mutationBusy}
            onClick={active ? onUnload : onLoad}
            aria-label={
              active ? `Unload ${entry.id}` : node?.activeModelId ? `Switch to ${entry.id}` : `Load ${entry.id}`
            }
          >
            {active ? "Unload" : node?.activeModelId ? "Switch" : "Load"}
          </button>
        ) : (
          <span className={styles.actionLabel}>
            {entry.artifact.kind === "downloaded" ? "Unavailable to load" : "Awaiting verification"}
          </span>
        )}
      </div>
    </article>
  );
}

function artifactLabel(artifact: ArtifactState, sizeBytes: number): string {
  if (artifact.kind === "not_downloaded") return "Not downloaded";
  if (artifact.kind === "partial") return `Partial — ${formatBytes(artifact.bytes)} of ${formatBytes(sizeBytes)}`;
  if (artifact.kind === "downloaded") return "Downloaded and verified";
  const labels: Record<ArtifactInvalidReason, string> = {
    size_mismatch: "Size mismatch",
    checksum_mismatch: "Checksum invalid",
    unreadable: "Unreadable artifact",
    verification_required: "Verification required",
  };
  return labels[artifact.reason];
}

function operationLabel(operation: OperationView): string {
  const action = operation.kind === "download" ? "Download" : operation.kind === "load" ? "Load" : "Unload";
  if (operation.status === "running") return `${action} in progress`;
  if (operation.status === "succeeded") return `${action} completed`;
  if (operation.status === "failed") return `${action} failed`;
  return `${action} ${operation.status}`;
}

function operationAnnouncement(operation: OperationView): string {
  const model = operation.modelId ?? "model";
  return `${model}: ${operationLabel(operation)}`;
}

function nodeStatusLabel(node: NodeSnapshot): string {
  if (node.status === "recovery_required") return "Recovery required";
  return node.status[0].toUpperCase() + node.status.slice(1);
}

function liveLabel(state: LiveState): string {
  if (state === "live") return "Live updates connected";
  if (state === "reconnecting") return "Reconnecting";
  if (state === "error") return "Controls unavailable";
  return "Connecting";
}

function isTerminal(status: OperationStatus): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

function withValue(source: Set<string>, value: string, present: boolean): Set<string> {
  const next = new Set(source);
  if (present) next.add(value);
  else next.delete(value);
  return next;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  const formatted = value >= 10 || Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1);
  return `${formatted} ${units[index]}`;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
