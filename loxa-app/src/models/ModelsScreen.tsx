import { useEffect, useMemo, useRef, useState } from "react";
import { Search } from "lucide-react";

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
import type { ModelInventoryEntry, NodeSnapshot, OperationStatus, OperationView } from "../control/contracts";
import { Input } from "../components/ui/input";
import { ModelRow } from "./ModelRow";
import { operationLabel } from "./modelRowLabels";
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
  const [searchQuery, setSearchQuery] = useState("");
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
  const normalizedSearchQuery = searchQuery.trim().toLowerCase();
  const visibleModels = useMemo(() => {
    if (normalizedSearchQuery === "") return models;
    return models.filter((entry) =>
      [entry.id, entry.repo, entry.engine.engine, entry.params, entry.quant, entry.license].some((value) =>
        value.toLowerCase().includes(normalizedSearchQuery),
      ),
    );
  }, [models, normalizedSearchQuery]);

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
        <span>
          {normalizedSearchQuery === ""
            ? `${models.length} verified recipes`
            : `${visibleModels.length} of ${models.length} verified recipes`}
        </span>
      </div>

      <div className={styles.searchControl}>
        <Search aria-hidden="true" size={16} strokeWidth={1.8} />
        <Input
          type="search"
          aria-label="Search models"
          placeholder="Search models"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.currentTarget.value)}
        />
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
      {!error && inventoryLoaded && models.length > 0 && visibleModels.length === 0 && normalizedSearchQuery !== "" && (
        <p className={styles.empty}>No models match “{searchQuery.trim()}”.</p>
      )}
      <div className={styles.list}>
        {visibleModels.map((entry) => (
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

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
