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
};

export type NodeSessionPhase =
  | "checking"
  | "starting"
  | "unloaded"
  | "ready"
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
  retry(): Promise<void>;
  stop(): Promise<void>;
};

type SessionState = Omit<NodeSessionValue, "retry" | "stop">;

const NodeSessionContext = createContext<NodeSessionValue | null>(null);
const pendingEnsures = new WeakMap<BootstrapApi, Map<string, Promise<BootstrapSnapshot>>>();

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
  const activeRun = useRef(0);
  const probeController = useRef<AbortController | null>(null);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  const connect = useCallback(async () => {
    const run = ++activeRun.current;
    probeController.current?.abort();
    const controller = new AbortController();
    probeController.current = controller;
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
      if (controller.signal.aborted || run !== activeRun.current) return;
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
      if (controller.signal.aborted || run !== activeRun.current) return;
      setState({
        phase: status.health === "ready" ? "ready" : "unloaded",
        ownership: bootstrap.ownership,
        endpoint: bootstrap.endpoint,
        status,
        error: null,
        proven: true,
      });
    } catch (error) {
      if (controller.signal.aborted || run !== activeRun.current) return;
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
  }, [endpoint, services]);

  useEffect(() => {
    void connect();
    return () => {
      activeRun.current += 1;
      probeController.current?.abort();
    };
  }, [connect]);

  const stop = useCallback(async () => {
    if (stateRef.current.ownership !== "owned") return;
    const run = ++activeRun.current;
    probeController.current?.abort();
    setState((current) => ({
      ...current,
      phase: "stopping",
      status: null,
      error: null,
      proven: false,
    }));
    try {
      const snapshot = await services.bootstrap.stop();
      if (run !== activeRun.current) return;
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
      if (run !== activeRun.current) return;
      setState((current) => ({
        ...current,
        phase: message(error).toLowerCase().includes("recovery required")
          ? "recovery-required"
          : "error",
        error: message(error),
      }));
    }
  }, [services.bootstrap]);

  const value = useMemo<NodeSessionValue>(() => ({
    ...state,
    retry: connect,
    stop,
  }), [connect, state, stop]);

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
