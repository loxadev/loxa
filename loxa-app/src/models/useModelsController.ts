import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { getInventory as defaultGetInventory } from "../control/client";
import type { ModelInventoryEntry, OperationView, V2Operation, V2Slot } from "../control/contracts";
import { useNodeSession } from "../node/NodeSession";

// NodeSession owns all control authority. This screen-level seam is metadata-only.
export type ModelsScreenServices = {
  getInventory: typeof defaultGetInventory;
};
export type ModelsLiveState = "connecting" | "live" | "reconnecting" | "error";

type UseModelsControllerOptions = {
  endpoint: string;
  services: ModelsScreenServices;
  reconnectDelayMs: number;
  verificationPollMs: number;
  verificationPollLimit: number;
  reconnectLimit: number;
  onModelMutationStart?: (operationId: string) => void;
  onModelMutationSettled?: (operationId: string) => void | Promise<void>;
};

type AcceptedMutation = {
  kind: "download" | "load" | "unload";
  operationModelId: string | null;
  uiModelId: string;
};

const MAX_PENDING_OPERATIONS = 128;

export function useModelsController({
  endpoint,
  services,
  reconnectDelayMs,
  verificationPollMs,
  verificationPollLimit,
  reconnectLimit,
  onModelMutationStart,
  onModelMutationSettled,
}: UseModelsControllerOptions) {
  void services;
  void reconnectDelayMs;
  void reconnectLimit;
  const session = useNodeSession();
  const [models, setModels] = useState<ModelInventoryEntry[]>([]);
  const [inventoryLoaded, setInventoryLoaded] = useState(false);
  const [requestPendingModels, setRequestPendingModels] = useState<Set<string>>(() => new Set());
  const [acceptedMutations, setAcceptedMutations] = useState<Map<V2Operation["operation_id"], AcceptedMutation>>(
    () => new Map(),
  );
  const [notice, setNotice] = useState("Connecting to model controls");
  const [error, setError] = useState("");
  const [retryNonce, setRetryNonce] = useState(0);
  const activeRef = useRef(true);
  const acceptedMutationsRef = useRef(acceptedMutations);
  const terminalRefreshControllers = useRef(new Set<AbortController>());
  const verificationBudgetRef = useRef<{ key: string | null; attempts: number }>({ key: null, attempts: 0 });
  const getInventory = session.getInventory;

  const rememberAccepted = useCallback(
    (operationId: V2Operation["operation_id"], mutation: AcceptedMutation) => {
      const next = new Map(acceptedMutationsRef.current);
      next.delete(operationId);
      next.set(operationId, mutation);
      while (next.size > MAX_PENDING_OPERATIONS) {
        const oldest = next.keys().next().value as V2Operation["operation_id"] | undefined;
        if (oldest === undefined) break;
        next.delete(oldest);
        void onModelMutationSettled?.(oldest);
      }
      acceptedMutationsRef.current = next;
      setAcceptedMutations(next);
    },
    [onModelMutationSettled],
  );

  const operations = useMemo(
    () => (session.control?.operations ?? []).map(projectOperation),
    [session.control?.operations],
  );
  const node = useMemo(() => projectSlot(session.control?.slots[0]), [session.control?.slots]);
  const verificationKey = useMemo(
    () =>
      models
        .filter((entry) => entry.artifact.kind === "invalid" && entry.artifact.reason === "verification_required")
        .map((entry) => entry.id)
        .sort()
        .join("\u0000"),
    [models],
  );
  const liveState: ModelsLiveState =
    session.control !== null
      ? "live"
      : session.phase === "disconnected"
        ? "reconnecting"
        : session.phase === "error" || session.phase === "recovery-required"
          ? "error"
          : "connecting";

  const refreshInventory = useCallback(
    async (signal: AbortSignal) => {
      const next = await getInventory(signal);
      if (signal.aborted || !activeRef.current) return;
      setModels(next);
      setInventoryLoaded(true);
      setError("");
      setNotice("Live model updates connected");
    },
    [getInventory],
  );

  useEffect(() => {
    const controller = new AbortController();
    activeRef.current = true;
    if (session.proven) {
      void refreshInventory(controller.signal).catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          setError(message(reason));
          setNotice("Model controls unavailable");
        }
      });
    }
    return () => {
      activeRef.current = false;
      controller.abort();
    };
  }, [endpoint, refreshInventory, retryNonce, session.proven]);

  useEffect(() => {
    if (!session.proven || verificationKey === "" || verificationPollLimit <= 0) {
      verificationBudgetRef.current = { key: null, attempts: 0 };
      return;
    }
    if (verificationBudgetRef.current.key !== verificationKey) {
      verificationBudgetRef.current = { key: verificationKey, attempts: 0 };
    }
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = () => {
      const budget = verificationBudgetRef.current;
      if (controller.signal.aborted || budget.key !== verificationKey || budget.attempts >= verificationPollLimit)
        return;
      const delay = Math.min(30_000, verificationPollMs * 2 ** Math.min(budget.attempts, 4));
      timer = setTimeout(() => {
        verificationBudgetRef.current.attempts += 1;
        void refreshInventory(controller.signal).then(poll, poll);
      }, delay);
    };
    poll();
    return () => {
      controller.abort();
      if (timer !== undefined) clearTimeout(timer);
    };
  }, [refreshInventory, session.proven, verificationKey, verificationPollLimit, verificationPollMs]);

  useEffect(() => {
    const operationsById = new Map(
      (session.control?.operations ?? []).map((operation) => [operation.operation_id, operation]),
    );
    const next = new Map(acceptedMutationsRef.current);
    let changed = false;
    for (const [operationId, accepted] of acceptedMutationsRef.current) {
      const operation = operationsById.get(operationId);
      if (!session.pendingOperationIds.has(operationId)) {
        next.delete(operationId);
        changed = true;
        void onModelMutationSettled?.(operationId);
        continue;
      }
      if (
        operation === undefined ||
        !isTerminalV2(operation) ||
        operation.kind !== accepted.kind ||
        operation.model_id !== accepted.operationModelId
      ) {
        continue;
      }
      next.delete(operationId);
      changed = true;
      void onModelMutationSettled?.(operationId);
      if (operation.kind === "download") {
        const controller = new AbortController();
        terminalRefreshControllers.current.add(controller);
        void refreshInventory(controller.signal)
          .catch((reason: unknown) => {
            if (!controller.signal.aborted) setError(message(reason));
          })
          .finally(() => terminalRefreshControllers.current.delete(controller));
      }
    }
    if (changed) {
      acceptedMutationsRef.current = next;
      setAcceptedMutations(next);
    }
  }, [
    acceptedMutations,
    onModelMutationSettled,
    refreshInventory,
    session.control?.operations,
    session.pendingOperationIds,
  ]);

  useEffect(
    () => () => {
      for (const controller of terminalRefreshControllers.current) controller.abort();
      terminalRefreshControllers.current.clear();
    },
    [],
  );

  const latestByModel = useMemo(() => {
    const latest = new Map<string, OperationView>();
    for (const operation of operations) {
      if (operation.modelId === null) continue;
      const current = latest.get(operation.modelId);
      if (current === undefined || operation.updatedAtUnixMs >= current.updatedAtUnixMs) {
        latest.set(operation.modelId, operation);
      }
    }
    return latest;
  }, [operations]);

  const download = async (modelId: string) => {
    setRequestPendingModels((current) => withValue(current, modelId, true));
    setError("");
    try {
      const accepted = await session.downloadModel(modelId);
      if (!activeRef.current) return;
      rememberAccepted(accepted.operation_id, {
        kind: "download",
        operationModelId: modelId,
        uiModelId: modelId,
      });
      onModelMutationStart?.(accepted.operation_id);
      setNotice(`${modelId}: Download queued`);
    } catch (reason) {
      setError(message(reason));
    } finally {
      if (activeRef.current) setRequestPendingModels((current) => withValue(current, modelId, false));
    }
  };

  const cancel = async (operation: OperationView, modelId: string) => {
    setRequestPendingModels((current) => withValue(current, modelId, true));
    setError("");
    try {
      const accepted = await session.cancelOperation(operation.id);
      if (!activeRef.current) return;
      rememberAccepted(accepted.operation_id, {
        kind: operation.kind,
        operationModelId: operation.modelId,
        uiModelId: modelId,
      });
      onModelMutationStart?.(accepted.operation_id);
      setNotice(`${modelId}: Cancellation requested`);
    } catch (reason) {
      setError(message(reason));
    } finally {
      if (activeRef.current) setRequestPendingModels((current) => withValue(current, modelId, false));
    }
  };

  const startLifecycle = async (kind: "load" | "unload", modelId: string) => {
    if (node?.status === "recovery_required") return;
    setRequestPendingModels((current) => withValue(current, modelId, true));
    setError("");
    try {
      const accepted = kind === "load" ? await session.loadModel(modelId) : await session.unloadModel();
      if (!activeRef.current) return;
      rememberAccepted(accepted.operation_id, {
        kind,
        operationModelId: kind === "load" ? modelId : null,
        uiModelId: modelId,
      });
      onModelMutationStart?.(accepted.operation_id);
      setNotice(`${modelId}: ${kind === "load" ? "Load" : "Unload"} queued`);
    } catch (reason) {
      setError(message(reason));
    } finally {
      if (activeRef.current) setRequestPendingModels((current) => withValue(current, modelId, false));
    }
  };

  const activeUnload =
    node?.activeModelId === null
      ? undefined
      : operations
          .filter((operation) => operation.kind === "unload" && operation.id === node?.operationId)
          .sort((left, right) => right.updatedAtUnixMs - left.updatedAtUnixMs)[0];
  const pendingModels = useMemo(() => {
    const next = new Set(requestPendingModels);
    for (const accepted of acceptedMutations.values()) next.add(accepted.uiModelId);
    return next;
  }, [acceptedMutations, requestPendingModels]);
  const mutationBusy =
    session.control === null ||
    pendingModels.size > 0 ||
    node?.operationId !== null ||
    (session.control?.operations ?? []).some(
      (operation) =>
        operation.status === "queued" || operation.status === "running" || operation.status === "cancelling",
    );

  return {
    activeUnload,
    cancel,
    download,
    error,
    inventoryLoaded,
    latestByModel,
    liveState,
    models,
    mutationBusy,
    node,
    notice,
    pendingModels,
    retry: () => {
      verificationBudgetRef.current = { key: null, attempts: 0 };
      setRetryNonce((value) => value + 1);
      void session.retry();
    },
    startLifecycle,
  };
}

function projectSlot(slot: V2Slot | undefined) {
  if (slot === undefined) return null;
  return {
    status: slot.status === "recovery" ? ("recovery_required" as const) : slot.status,
    activeModelId: slot.model_id,
    operationId: slot.operation_id,
    error: slot.error?.message ?? null,
  };
}

function projectOperation(operation: V2Operation): OperationView {
  return {
    id: operation.operation_id,
    kind: operation.kind,
    status: operation.status === "cancelling" ? "running" : operation.status,
    modelId: operation.model_id,
    progress:
      operation.progress === null
        ? null
        : {
            completedBytes: decimalToUiNumber(operation.progress.completed_bytes),
            totalBytes:
              operation.progress.total_bytes === null ? null : decimalToUiNumber(operation.progress.total_bytes),
          },
    error: operation.error?.message ?? null,
    createdAtUnixMs: decimalToUiNumber(operation.created_at_unix_ms),
    updatedAtUnixMs: decimalToUiNumber(operation.updated_at_unix_ms),
  };
}

function decimalToUiNumber(value: string): number {
  const integer = BigInt(value);
  return integer > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(integer);
}

function isTerminalV2(operation: V2Operation): boolean {
  return operation.status === "succeeded" || operation.status === "failed" || operation.status === "cancelled";
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
