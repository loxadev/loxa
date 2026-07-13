import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import type { getStatus as defaultGetStatus } from "./client";
import type { NodeStatus } from "./contracts";
import type { NodeOwnership } from "./machine";
import type { streamControlEvents as defaultStreamControlEvents } from "../control/events";

export type BootstrapSnapshot = {
  ownership: NodeOwnership;
  endpoint: string;
  childRunning: boolean;
  error: string | null;
};

export type StartNodeRequest = {
  endpoint: string;
};

export type BootstrapApi = {
  snapshot(): Promise<BootstrapSnapshot>;
  start(request: StartNodeRequest): Promise<BootstrapSnapshot>;
  attach(endpoint: string): Promise<BootstrapSnapshot>;
  stop(): Promise<BootstrapSnapshot>;
};

export type NodeSessionServices = {
  bootstrap: BootstrapApi;
  getStatus: typeof defaultGetStatus;
  readControlToken(endpoint: string): Promise<string>;
  createControlEventStream: typeof defaultStreamControlEvents;
};

export type NodeSessionPhase =
  | "checking"
  | "starting"
  | "unloaded"
  | "ready"
  | "reconciling"
  | "stopping"
  | "disconnected"
  | "error"
  | "recovery-required";

export type NodeSessionValue = {
  phase: NodeSessionPhase;
  ownership: NodeOwnership;
  endpoint: string;
  status: NodeStatus | null;
  error: string | null;
  proven: boolean;
  invalidateModelTruth(operationId?: string): void;
  settleModelMutation(operationId: string): Promise<void>;
  refreshStatus(): Promise<boolean>;
  retry(): Promise<void>;
  stop(): Promise<void>;
};

type SessionState = Omit<
  NodeSessionValue,
  "invalidateModelTruth" | "settleModelMutation" | "refreshStatus" | "retry" | "stop"
>;

const NodeSessionContext = createContext<NodeSessionValue | null>(null);
const pendingEnsures = new WeakMap<BootstrapApi, Map<string, Promise<BootstrapSnapshot>>>();
const MAX_SETTLED_OPERATIONS_PER_EPOCH = 128;
const MAX_RETRYABLE_TERMINALS_PER_EPOCH = 128;
const STREAM_RECONNECT_BASE_DELAY_MS = 100;
const STREAM_RECONNECT_LIMIT = 6;
type TrackedLifecycleState = "active" | "retryable-terminal";

function ensureNode(bootstrap: BootstrapApi, endpoint: string) {
  let byEndpoint = pendingEnsures.get(bootstrap);
  if (!byEndpoint) {
    byEndpoint = new Map();
    pendingEnsures.set(bootstrap, byEndpoint);
  }
  const current = byEndpoint.get(endpoint);
  if (current) return current;

  const pending = bootstrap.start({ endpoint });
  byEndpoint.set(endpoint, pending);
  void pending.finally(() => {
    if (byEndpoint?.get(endpoint) === pending) byEndpoint.delete(endpoint);
  }).catch(() => undefined);
  return pending;
}

function initialState(endpoint: string): SessionState {
  return {
    phase: "checking",
    ownership: "none",
    endpoint,
    status: null,
    error: null,
    proven: false,
  };
}

export function NodeSessionProvider({
  children,
  endpoint,
  services,
}: {
  children: ReactNode;
  endpoint: string;
  services: NodeSessionServices;
}) {
  const [state, setState] = useState<SessionState>(() => initialState(endpoint));
  const stateRef = useRef(state);
  const bootstrapRun = useRef(0);
  const bootstrapController = useRef<AbortController | null>(null);
  const modelProbeController = useRef<AbortController | null>(null);
  const closingGeneration = useRef(0);
  const stopping = useRef(false);
  const trackedLifecycleOperations = useRef(new Map<string, TrackedLifecycleState>());
  const settledLifecycleOperations = useRef(new Map<string, true>());
  const pendingLifecycleSettlements = useRef(new Map<string, Promise<void>>());

  const resetLifecycleEpoch = useCallback(() => {
    trackedLifecycleOperations.current.clear();
    settledLifecycleOperations.current.clear();
    pendingLifecycleSettlements.current.clear();
  }, []);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  const connect = useCallback(async () => {
    stopping.current = false;
    const generation = ++closingGeneration.current;
    resetLifecycleEpoch();
    const run = ++bootstrapRun.current;
    bootstrapController.current?.abort();
    modelProbeController.current?.abort();
    const controller = new AbortController();
    bootstrapController.current = controller;
    setState((current) => ({
      ...current,
      phase: "starting",
      endpoint,
      status: null,
      error: null,
      proven: false,
    }));

    try {
      const bootstrap = await ensureNode(services.bootstrap, endpoint);
      if (controller.signal.aborted || run !== bootstrapRun.current || generation !== closingGeneration.current) return;
      if (bootstrap.error) throw new Error(bootstrap.error);

      setState((current) => ({
        ...current,
        phase: "starting",
        ownership: bootstrap.ownership,
        endpoint: bootstrap.endpoint,
        status: null,
        error: null,
        proven: false,
      }));

      const status = await services.getStatus(bootstrap.endpoint, { signal: controller.signal });
      if (controller.signal.aborted || run !== bootstrapRun.current || generation !== closingGeneration.current) return;
      setState({
        phase: status.health === "ready" ? "ready" : "unloaded",
        ownership: bootstrap.ownership,
        endpoint: bootstrap.endpoint,
        status,
        error: null,
        proven: true,
      });
    } catch (error) {
      if (controller.signal.aborted || run !== bootstrapRun.current || generation !== closingGeneration.current) return;
      const detail = message(error);
      setState((current) => ({
        ...current,
        phase: detail.toLowerCase().includes("recovery required")
          ? "recovery-required"
          : "error",
        status: null,
        error: detail,
        proven: false,
      }));
    }
  }, [endpoint, resetLifecycleEpoch, services]);

  useEffect(() => {
    void connect();
    return () => {
      stopping.current = true;
      closingGeneration.current += 1;
      bootstrapRun.current += 1;
      bootstrapController.current?.abort();
      modelProbeController.current?.abort();
    };
  }, [connect]);

  const invalidateModelTruth = useCallback((operationId?: string) => {
    if (stopping.current) return;
    if (operationId) rememberActiveOperation(trackedLifecycleOperations.current, operationId);
    setState((current) => {
      if (!current.proven && current.phase !== "reconciling") return current;
      const next = {
        ...current,
        phase: "reconciling",
        status: null,
        error: null,
        proven: true,
      } satisfies SessionState;
      stateRef.current = next;
      return next;
    });
  }, []);

  const refreshStatus = useCallback(async () => {
    if (stopping.current) return false;
    const generation = closingGeneration.current;
    modelProbeController.current?.abort();
    const controller = new AbortController();
    modelProbeController.current = controller;
    const currentEndpoint = stateRef.current.endpoint;
    setState((current) => ({
      ...current,
      phase: "reconciling",
      status: null,
      error: null,
      proven: true,
    }));
    try {
      const status = await services.getStatus(currentEndpoint, { signal: controller.signal });
      if (controller.signal.aborted || stopping.current || generation !== closingGeneration.current) return false;
      setState((current) => {
        const next = {
          ...current,
          phase: status.health === "ready" ? "ready" : "unloaded",
          status,
          error: null,
          proven: true,
        } satisfies SessionState;
        stateRef.current = next;
        return next;
      });
      return true;
    } catch (error) {
      if (controller.signal.aborted || stopping.current || generation !== closingGeneration.current) return false;
      const detail = message(error);
      setState((current) => {
        const next = {
          ...current,
          phase: detail.toLowerCase().includes("recovery required")
            ? "recovery-required"
            : "error",
          status: null,
          error: detail,
          proven: false,
        } satisfies SessionState;
        stateRef.current = next;
        return next;
      });
      return false;
    }
  }, [services]);

  const settleModelMutation = useCallback((operationId: string) => {
    if (stopping.current || settledLifecycleOperations.current.has(operationId)) {
      return Promise.resolve();
    }
    const current = pendingLifecycleSettlements.current.get(operationId);
    if (current) return current;
    if (!trackedLifecycleOperations.current.has(operationId)) {
      return Promise.resolve();
    }
    rememberTerminalOperation(
      trackedLifecycleOperations.current,
      pendingLifecycleSettlements.current,
      operationId,
    );

    const generation = closingGeneration.current;
    const pending = (async () => {
      if (stopping.current || generation !== closingGeneration.current) return;
      const proven = await refreshStatus();
      if (proven && !stopping.current && generation === closingGeneration.current) {
        rememberSettledOperation(settledLifecycleOperations.current, operationId);
        trackedLifecycleOperations.current.delete(operationId);
      }
    })();
    pendingLifecycleSettlements.current.set(operationId, pending);
    void pending.finally(() => {
      if (pendingLifecycleSettlements.current.get(operationId) === pending) {
        pendingLifecycleSettlements.current.delete(operationId);
      }
    });
    return pending;
  }, [refreshStatus]);

  useEffect(() => {
    if (!state.proven) return;
    const controller = new AbortController();
    let disposed = false;
    let stream: ReturnType<typeof services.createControlEventStream> | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let reconnectAttempts = 0;

    const connect = async (cursor: number) => {
      const token = await services.readControlToken(state.endpoint);
      if (disposed || controller.signal.aborted) return;
      const previous = stream;
      stream = services.createControlEventStream(state.endpoint, token, cursor, {
        onSnapshot: (snapshot) => {
          if (disposed) return;
          reconnectAttempts = 0;
          const lifecycle = snapshot.operations.filter(isLifecycleOperation);
          lifecycle.filter((operation) => isActiveOperation(operation.status))
            .forEach((operation) => invalidateModelTruth(operation.id));
          lifecycle.filter((operation) => isTerminalOperation(operation.status))
            .forEach((operation) => { void settleModelMutation(operation.id); });
        },
        onEvent: (event) => {
          if (disposed || stopping.current || !isLifecycleOperation(event.operation)) return;
          if (isActiveOperation(event.operation.status)) invalidateModelTruth(event.operation.id);
          else if (isTerminalOperation(event.operation.status)) void settleModelMutation(event.operation.id);
        },
        onTerminal: (terminal) => {
          if (disposed || controller.signal.aborted) return;
          scheduleReconnect(terminal.cursor);
        },
      }, controller.signal);
      previous?.dispose();
    };

    const scheduleReconnect = (cursor: number) => {
      if (disposed || controller.signal.aborted || reconnectTimer !== undefined) return;
      if (reconnectAttempts >= STREAM_RECONNECT_LIMIT) return;
      const delay = Math.min(
        3_000,
        STREAM_RECONNECT_BASE_DELAY_MS * (2 ** Math.min(reconnectAttempts, 4)),
      );
      reconnectAttempts += 1;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = undefined;
        if (disposed || controller.signal.aborted) return;
        void connect(cursor).catch(() => scheduleReconnect(cursor));
      }, delay);
    };

    void connect(0).catch(() => scheduleReconnect(0));
    return () => {
      disposed = true;
      controller.abort();
      if (reconnectTimer !== undefined) clearTimeout(reconnectTimer);
      stream?.dispose();
    };
  }, [invalidateModelTruth, services, settleModelMutation, state.endpoint, state.proven]);

  const stop = useCallback(async () => {
    if (stateRef.current.ownership !== "owned") return;
    stopping.current = true;
    closingGeneration.current += 1;
    resetLifecycleEpoch();
    const run = ++bootstrapRun.current;
    bootstrapController.current?.abort();
    modelProbeController.current?.abort();
    setState((current) => ({
      ...current,
      phase: "stopping",
      status: null,
      error: null,
      proven: false,
    }));
    try {
      const snapshot = await services.bootstrap.stop();
      if (run !== bootstrapRun.current) return;
      if (snapshot.error) throw new Error(snapshot.error);
      setState({
        phase: "disconnected",
        ownership: snapshot.ownership,
        endpoint: snapshot.endpoint,
        status: null,
        error: null,
        proven: false,
      });
    } catch (error) {
      if (run !== bootstrapRun.current) return;
      setState((current) => ({
        ...current,
        phase: message(error).toLowerCase().includes("recovery required")
          ? "recovery-required"
          : "error",
        error: message(error),
      }));
    }
  }, [resetLifecycleEpoch, services.bootstrap]);

  const value = useMemo<NodeSessionValue>(() => ({
    ...state,
    invalidateModelTruth,
    settleModelMutation,
    refreshStatus,
    retry: connect,
    stop,
  }), [connect, invalidateModelTruth, refreshStatus, settleModelMutation, state, stop]);

  return <NodeSessionContext.Provider value={value}>{children}</NodeSessionContext.Provider>;
}

// The provider and its required hook intentionally share one private context.
// eslint-disable-next-line react-refresh/only-export-components
export function useNodeSession() {
  const session = useContext(NodeSessionContext);
  if (!session) throw new Error("useNodeSession must be used within NodeSessionProvider");
  return session;
}

function message(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function isLifecycleOperation(operation: { kind: string }) {
  return operation.kind === "load" || operation.kind === "unload";
}

function isActiveOperation(status: string) {
  return status === "queued" || status === "running";
}

function isTerminalOperation(status: string) {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

function rememberSettledOperation(operations: Map<string, true>, operationId: string) {
  operations.delete(operationId);
  operations.set(operationId, true);
  while (operations.size > MAX_SETTLED_OPERATIONS_PER_EPOCH) {
    const oldest = operations.keys().next().value as string | undefined;
    if (oldest === undefined) break;
    operations.delete(oldest);
  }
}

function rememberActiveOperation(
  operations: Map<string, TrackedLifecycleState>,
  operationId: string,
) {
  operations.delete(operationId);
  operations.set(operationId, "active");
}

function rememberTerminalOperation(
  operations: Map<string, TrackedLifecycleState>,
  pending: Map<string, Promise<void>>,
  operationId: string,
) {
  operations.delete(operationId);
  operations.set(operationId, "retryable-terminal");
  let retryable = 0;
  for (const state of operations.values()) {
    if (state === "retryable-terminal") retryable += 1;
  }
  if (retryable <= MAX_RETRYABLE_TERMINALS_PER_EPOCH) return;
  for (const [candidate, state] of operations) {
    if (
      state === "retryable-terminal" &&
      candidate !== operationId &&
      !pending.has(candidate)
    ) {
      operations.delete(candidate);
      return;
    }
  }
}
