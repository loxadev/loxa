import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import {
  type ProvenControlPeer,
  type cancelV2Operation as defaultCancelV2Operation,
  type downloadV2Model as defaultDownloadV2Model,
  type getInventory as defaultGetInventory,
  type loadV2Slot as defaultLoadV2Slot,
  type proveV2ControlPeer as defaultProveV2ControlPeer,
  type unloadV2Slot as defaultUnloadV2Slot,
} from "../control/client";
import {
  applyV2Event,
  applyV2Snapshot,
  type ResumeCursor,
  type V2ControlState,
  type openV2Events as defaultOpenV2Events,
} from "../control/events";
import type { ModelInventoryEntry, V2Operation, V2OperationAccepted } from "../control/contracts";
import type { NodeStatus } from "./contracts";
import type { getStatus as defaultGetStatus } from "./client";
import type { streamControlEvents as defaultStreamControlEvents } from "../control/events";
import type { NodeOwnership } from "./machine";

export type BootstrapSnapshot = {
  ownership: NodeOwnership;
  endpoint: string;
  childRunning: boolean;
  error: string | null;
};

export type StartNodeRequest = { endpoint: string };

export type BootstrapApi = {
  snapshot(): Promise<BootstrapSnapshot>;
  start(request: StartNodeRequest): Promise<BootstrapSnapshot>;
  attach(endpoint: string): Promise<BootstrapSnapshot>;
  stop(): Promise<BootstrapSnapshot>;
};

export type NodeSessionServices = {
  bootstrap: BootstrapApi;
  readControlToken(endpoint: string): Promise<string>;
  proveV2ControlPeer?: typeof defaultProveV2ControlPeer;
  openV2Events?: typeof defaultOpenV2Events;
  getInventory?: typeof defaultGetInventory;
  downloadV2Model?: typeof defaultDownloadV2Model;
  loadV2Slot?: typeof defaultLoadV2Slot;
  unloadV2Slot?: typeof defaultUnloadV2Slot;
  cancelV2Operation?: typeof defaultCancelV2Operation;
  getStatus: typeof defaultGetStatus;
  createControlEventStream: typeof defaultStreamControlEvents;
};

export type NodeSessionPhase =
  | "checking"
  | "starting"
  | "unloaded"
  | "ready"
  | "reconciling"
  | "stopping"
  | "stopped"
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
  control: V2ControlState | null;
  pendingOperationIds: ReadonlySet<string>;
  getInventory(signal?: AbortSignal): Promise<ModelInventoryEntry[]>;
  downloadModel(modelId: string): Promise<V2OperationAccepted>;
  loadModel(modelId: string): Promise<V2OperationAccepted>;
  unloadModel(): Promise<V2OperationAccepted>;
  cancelOperation(operationId: string): Promise<V2OperationAccepted>;
  invalidateModelTruth(operationId?: string): void;
  settleModelMutation(operationId: string): Promise<void>;
  refreshStatus(): Promise<boolean>;
  retry(): Promise<void>;
  stop(): Promise<void>;
};

type SessionState = Pick<
  NodeSessionValue,
  "phase" | "ownership" | "endpoint" | "status" | "error" | "proven" | "control"
>;

type Authority = { peer: ProvenControlPeer; token: string; endpoint: string };
type PendingOperation = {
  kind?: V2Operation["kind"];
  modelId?: string | null;
  epoch?: string;
  revision?: string;
};

const NodeSessionContext = createContext<NodeSessionValue | null>(null);
const pendingEnsures = new WeakMap<BootstrapApi, Map<string, Promise<BootstrapSnapshot>>>();
const STREAM_RECONNECT_BASE_DELAY_MS = 100;
const STREAM_RECONNECT_LIMIT = 6;
const STREAM_RECONNECT_STABLE_MS = 5_000;
const MAX_PENDING_OPERATIONS = 128;

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
  void pending
    .finally(() => {
      if (byEndpoint?.get(endpoint) === pending) byEndpoint.delete(endpoint);
    })
    .catch(() => undefined);
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
    control: null,
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
  const [peer, setPeer] = useState<ProvenControlPeer | null>(null);
  const stateRef = useRef(state);
  const authorityRef = useRef<Authority | null>(null);
  const controlRef = useRef<V2ControlState | null>(null);
  const snapshotBoundaryRef = useRef<{ epoch: string; revision: string; version: number } | null>(null);
  const snapshotVersionRef = useRef(0);
  const pendingOperationsRef = useRef(new Map<string, PendingOperation>());
  const bootstrapRun = useRef(0);
  const bootstrapController = useRef<AbortController | null>(null);
  const closingGeneration = useRef(0);
  const stopping = useRef(false);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  const publishControl = useCallback((control: V2ControlState) => {
    controlRef.current = control;
    for (const [operationId, pending] of pendingOperationsRef.current) {
      const operation = control.operations.find((candidate) => candidate.operation_id === operationId);
      if (
        operation !== undefined &&
        isTerminal(operation) &&
        (pending.kind === undefined || operation.kind === pending.kind) &&
        (pending.modelId === undefined || operation.model_id === pending.modelId)
      ) {
        pendingOperationsRef.current.delete(operationId);
      }
    }
    setState((current) => {
      const projection = projectControl(control);
      const requiresReconciliation = [...pendingOperationsRef.current.values()].some(
        (pending) => pending.kind !== "download",
      );
      const next: SessionState = {
        ...current,
        ...(requiresReconciliation ? { phase: "reconciling" as const, status: null, error: null } : projection),
        proven: true,
        control: requiresReconciliation ? null : control,
      };
      stateRef.current = next;
      return next;
    });
  }, []);

  const reconcileReplacementSnapshot = useCallback((control: V2ControlState, replaceLineage: boolean) => {
    if (replaceLineage) {
      pendingOperationsRef.current.clear();
      return;
    }
    for (const [operationId, pending] of pendingOperationsRef.current) {
      if (pending.epoch !== undefined && pending.epoch !== control.epoch) {
        pendingOperationsRef.current.delete(operationId);
        continue;
      }
      const operation = control.operations.find((candidate) => candidate.operation_id === operationId);
      if (
        operation === undefined &&
        pending.revision !== undefined &&
        BigInt(control.revision) >= BigInt(pending.revision)
      ) {
        pendingOperationsRef.current.delete(operationId);
        continue;
      }
      if (
        operation !== undefined &&
        isTerminal(operation) &&
        (pending.kind === undefined || operation.kind === pending.kind) &&
        (pending.modelId === undefined || operation.model_id === pending.modelId)
      ) {
        pendingOperationsRef.current.delete(operationId);
      }
    }
  }, []);

  const connect = useCallback(async () => {
    stopping.current = false;
    const generation = ++closingGeneration.current;
    const run = ++bootstrapRun.current;
    bootstrapController.current?.abort();
    const controller = new AbortController();
    bootstrapController.current = controller;
    authorityRef.current = null;
    controlRef.current = null;
    snapshotBoundaryRef.current = null;
    pendingOperationsRef.current.clear();
    setPeer(null);
    setState((current) => ({
      ...current,
      phase: "starting",
      endpoint,
      status: null,
      error: null,
      proven: false,
      control: null,
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
        control: null,
      }));
      const token = await services.readControlToken(bootstrap.endpoint);
      if (controller.signal.aborted || run !== bootstrapRun.current || generation !== closingGeneration.current) return;
      const proveV2ControlPeer = requireService(services.proveV2ControlPeer, "v2 control proof");
      const provedPeer = await proveV2ControlPeer(bootstrap.endpoint, token, { signal: controller.signal });
      if (controller.signal.aborted || run !== bootstrapRun.current || generation !== closingGeneration.current) return;
      authorityRef.current = { peer: provedPeer, token, endpoint: bootstrap.endpoint };
      setState((current) => ({
        ...current,
        phase: "reconciling",
        ownership: bootstrap.ownership,
        endpoint: bootstrap.endpoint,
        status: null,
        error: null,
        proven: true,
        control: null,
      }));
      setPeer(provedPeer);
    } catch (error) {
      if (controller.signal.aborted || run !== bootstrapRun.current || generation !== closingGeneration.current) return;
      authorityRef.current = null;
      controlRef.current = null;
      snapshotBoundaryRef.current = null;
      pendingOperationsRef.current.clear();
      setPeer(null);
      const detail = message(error);
      setState((current) => ({
        ...current,
        phase: detail.toLowerCase().includes("recovery required") ? "recovery-required" : "error",
        status: null,
        error: detail,
        proven: false,
        control: null,
      }));
    }
  }, [endpoint, services]);

  useEffect(() => {
    const pendingOperations = pendingOperationsRef.current;
    void connect();
    return () => {
      stopping.current = true;
      closingGeneration.current += 1;
      bootstrapRun.current += 1;
      bootstrapController.current?.abort();
      authorityRef.current = null;
      controlRef.current = null;
      snapshotBoundaryRef.current = null;
      pendingOperations.clear();
    };
  }, [connect]);

  useEffect(() => {
    if (peer === null) return;
    const generation = closingGeneration.current;
    const openV2Events = requireService(services.openV2Events, "v2 control events");
    const controller = new AbortController();
    let disposed = false;
    let stream: ReturnType<typeof openV2Events> | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let stabilityTimer: ReturnType<typeof setTimeout> | undefined;
    let reconnectAttempts = 0;
    let resume: ResumeCursor | undefined;

    const isCurrent = () =>
      !disposed &&
      !controller.signal.aborted &&
      !stopping.current &&
      generation === closingGeneration.current &&
      authorityRef.current?.peer === peer;

    const markStreamUnavailable = () => {
      if (!isCurrent()) return;
      if (stabilityTimer !== undefined) {
        clearTimeout(stabilityTimer);
        stabilityTimer = undefined;
      }
      controlRef.current = null;
      setState((current) => ({
        ...current,
        phase: "reconciling",
        status: null,
        error: null,
        control: null,
      }));
    };

    const scheduleReconnect = () => {
      if (!isCurrent() || reconnectTimer !== undefined) return;
      if (reconnectAttempts >= STREAM_RECONNECT_LIMIT) {
        setState((current) => ({
          ...current,
          phase: "disconnected",
          status: null,
          error: "Durable node updates disconnected.",
          control: null,
        }));
        return;
      }
      const delay = Math.min(3_000, STREAM_RECONNECT_BASE_DELAY_MS * 2 ** Math.min(reconnectAttempts, 4));
      reconnectAttempts += 1;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = undefined;
        open();
      }, delay);
    };

    const open = () => {
      if (!isCurrent()) return;
      const previous = stream;
      try {
        stream = openV2Events(
          peer,
          resume,
          {
            onSnapshot: (snapshot) => {
              if (!isCurrent()) return;
              resume = { epoch: snapshot.epoch, cursor: snapshot.stream.cursor };
              const previousEpoch = controlRef.current?.epoch;
              const control = applyV2Snapshot(controlRef.current ?? undefined, snapshot);
              snapshotVersionRef.current += 1;
              snapshotBoundaryRef.current = {
                epoch: control.epoch,
                revision: control.revision,
                version: snapshotVersionRef.current,
              };
              reconcileReplacementSnapshot(
                control,
                snapshot.stream.cursor_gap || (previousEpoch !== undefined && previousEpoch !== control.epoch),
              );
              publishControl(control);
              if (stabilityTimer !== undefined) clearTimeout(stabilityTimer);
              stabilityTimer = setTimeout(() => {
                if (isCurrent() && controlRef.current !== null) reconnectAttempts = 0;
              }, STREAM_RECONNECT_STABLE_MS);
            },
            onRetainedEvent: () => {
              // Retained observations never mutate the replacement snapshot.
            },
            onEvent: (event) => {
              if (!isCurrent() || controlRef.current === null) return;
              const next = applyV2Event(controlRef.current, event);
              resume = { epoch: next.epoch, cursor: next.cursor };
              publishControl(next);
            },
            onTerminal: (terminal) => {
              if (!isCurrent() || terminal.kind === "cancelled") return;
              const current = controlRef.current;
              if (current !== null) resume = { epoch: current.epoch, cursor: terminal.cursor };
              markStreamUnavailable();
              scheduleReconnect();
            },
          },
          controller.signal,
        );
        previous?.dispose();
      } catch {
        markStreamUnavailable();
        scheduleReconnect();
      }
    };

    open();
    return () => {
      disposed = true;
      controller.abort();
      if (reconnectTimer !== undefined) clearTimeout(reconnectTimer);
      if (stabilityTimer !== undefined) clearTimeout(stabilityTimer);
      stream?.dispose();
    };
  }, [peer, publishControl, reconcileReplacementSnapshot, services]);

  const invalidateModelTruth = useCallback((operationId?: string) => {
    if (stopping.current) return;
    if (operationId !== undefined && !pendingOperationsRef.current.has(operationId)) {
      pendingOperationsRef.current.set(operationId, {});
    }
    setState((current) => ({
      ...current,
      phase: "reconciling",
      status: null,
      error: null,
      control: null,
    }));
  }, []);

  const refreshStatus = useCallback(async () => controlRef.current !== null && authorityRef.current !== null, []);

  const settleModelMutation = useCallback(
    async (operationId: string) => {
      const operation = controlRef.current?.operations.find((candidate) => candidate.operation_id === operationId);
      if (operation !== undefined && isTerminal(operation)) publishControl(controlRef.current!);
    },
    [publishControl],
  );

  const trackAcceptedOperation = useCallback(
    (
      authority: Authority,
      requestSnapshotVersion: number,
      accepted: V2OperationAccepted,
      pending: PendingOperation,
    ) => {
      if (
        stopping.current ||
        authorityRef.current !== authority ||
        (controlRef.current !== null && controlRef.current.epoch !== accepted.epoch)
      ) {
        throw new Error("The accepted operation no longer belongs to the current control authority.");
      }
      const operationId = accepted.operation_id;
      if (pending.kind === "load" || pending.kind === "unload") {
        for (const [pendingId, existing] of pendingOperationsRef.current) {
          if (pendingId !== operationId && (existing.kind === "load" || existing.kind === "unload")) {
            pendingOperationsRef.current.delete(pendingId);
          }
        }
      }
      pendingOperationsRef.current.delete(operationId);
      pendingOperationsRef.current.set(operationId, {
        ...pending,
        epoch: accepted.epoch,
        revision: accepted.revision,
      });
      while (pendingOperationsRef.current.size > MAX_PENDING_OPERATIONS) {
        const oldest = pendingOperationsRef.current.keys().next().value as string | undefined;
        if (oldest === undefined) break;
        pendingOperationsRef.current.delete(oldest);
      }
      const current = controlRef.current;
      if (current !== null) {
        const boundary = snapshotBoundaryRef.current;
        if (
          boundary?.epoch === accepted.epoch &&
          boundary.version > requestSnapshotVersion &&
          BigInt(boundary.revision) >= BigInt(accepted.revision) &&
          !current.operations.some((operation) => operation.operation_id === operationId)
        ) {
          pendingOperationsRef.current.delete(operationId);
        }
        publishControl(current);
        return;
      }
      setState((state) => ({
        ...state,
        phase: "reconciling",
        status: null,
        error: null,
        control: null,
      }));
    },
    [publishControl],
  );

  const getInventory = useCallback(
    async (signal?: AbortSignal) => {
      const authority = requireAuthority(authorityRef.current);
      return requireService(services.getInventory, "v1 artifact inventory")(
        authority.endpoint,
        authority.token,
        signal === undefined ? {} : { signal },
      );
    },
    [services],
  );

  const downloadModel = useCallback(
    async (modelId: string) => {
      const authority = requireAuthority(authorityRef.current);
      requireControl(controlRef.current);
      const requestSnapshotVersion = snapshotVersionRef.current;
      const accepted = await requireService(services.downloadV2Model, "v2 model download")(authority.peer, modelId);
      trackAcceptedOperation(authority, requestSnapshotVersion, accepted, { kind: "download", modelId });
      return accepted;
    },
    [services, trackAcceptedOperation],
  );

  const loadModel = useCallback(
    async (modelId: string) => {
      const authority = requireAuthority(authorityRef.current);
      const control = requireControl(controlRef.current);
      const requestSnapshotVersion = snapshotVersionRef.current;
      const accepted = await requireService(services.loadV2Slot, "v2 slot load")(
        authority.peer,
        control.nodes[0]!.node_id,
        control.slots[0]!.slot_id,
        modelId,
      );
      trackAcceptedOperation(authority, requestSnapshotVersion, accepted, { kind: "load", modelId });
      return accepted;
    },
    [services, trackAcceptedOperation],
  );

  const unloadModel = useCallback(async () => {
    const authority = requireAuthority(authorityRef.current);
    const control = requireControl(controlRef.current);
    const requestSnapshotVersion = snapshotVersionRef.current;
    const accepted = await requireService(services.unloadV2Slot, "v2 slot unload")(
      authority.peer,
      control.nodes[0]!.node_id,
      control.slots[0]!.slot_id,
    );
    trackAcceptedOperation(authority, requestSnapshotVersion, accepted, { kind: "unload", modelId: null });
    return accepted;
  }, [services, trackAcceptedOperation]);

  const cancelOperation = useCallback(
    async (operationId: string) => {
      const authority = requireAuthority(authorityRef.current);
      const control = requireControl(controlRef.current);
      const requestSnapshotVersion = snapshotVersionRef.current;
      const target = control.operations.find((operation) => operation.operation_id === operationId);
      const accepted = await requireService(services.cancelV2Operation, "v2 operation cancellation")(
        authority.peer,
        operationId,
      );
      trackAcceptedOperation(
        authority,
        requestSnapshotVersion,
        accepted,
        target === undefined ? {} : { kind: target.kind, modelId: target.model_id },
      );
      return accepted;
    },
    [services, trackAcceptedOperation],
  );

  const stop = useCallback(async () => {
    if (stateRef.current.ownership !== "owned") return;
    stopping.current = true;
    closingGeneration.current += 1;
    const run = ++bootstrapRun.current;
    bootstrapController.current?.abort();
    authorityRef.current = null;
    controlRef.current = null;
    snapshotBoundaryRef.current = null;
    pendingOperationsRef.current.clear();
    setPeer(null);
    setState((current) => ({ ...current, phase: "stopping", status: null, error: null, proven: false, control: null }));
    try {
      const snapshot = await services.bootstrap.stop();
      if (run !== bootstrapRun.current) return;
      if (snapshot.error) throw new Error(snapshot.error);
      setState({
        phase: "stopped",
        ownership: snapshot.ownership,
        endpoint: snapshot.endpoint,
        status: null,
        error: null,
        proven: false,
        control: null,
      });
    } catch (error) {
      if (run !== bootstrapRun.current) return;
      setState((current) => ({ ...current, phase: "error", error: message(error) }));
    }
  }, [services.bootstrap]);

  const value = useMemo<NodeSessionValue>(
    () => ({
      ...state,
      pendingOperationIds: new Set(pendingOperationsRef.current.keys()),
      getInventory,
      downloadModel,
      loadModel,
      unloadModel,
      cancelOperation,
      invalidateModelTruth,
      settleModelMutation,
      refreshStatus,
      retry: connect,
      stop,
    }),
    [
      cancelOperation,
      connect,
      downloadModel,
      getInventory,
      invalidateModelTruth,
      loadModel,
      refreshStatus,
      settleModelMutation,
      state,
      stop,
      unloadModel,
    ],
  );

  return <NodeSessionContext.Provider value={value}>{children}</NodeSessionContext.Provider>;
}

// The provider and its required hook intentionally share one private context.
// eslint-disable-next-line react-refresh/only-export-components
export function useNodeSession() {
  const session = useContext(NodeSessionContext);
  if (!session) throw new Error("useNodeSession must be used within NodeSessionProvider");
  return session;
}

function requireAuthority(authority: Authority | null): Authority {
  if (authority === null) throw new Error("The proven Loxa control session is unavailable.");
  return authority;
}

function requireService<T>(service: T | undefined, name: string): T {
  if (service === undefined) throw new Error(`The injected ${name} service is unavailable.`);
  return service;
}

function requireControl(control: V2ControlState | null): V2ControlState {
  if (control?.nodes.length !== 1 || control.slots.length !== 1) {
    throw new Error("The authoritative default slot is unavailable.");
  }
  return control;
}

function projectControl(control: V2ControlState): Pick<SessionState, "phase" | "status" | "error"> {
  const node = control.nodes[0];
  const slot = control.slots[0];
  if (node === undefined || slot === undefined) {
    return { phase: "error", status: null, error: "The authoritative default slot is unavailable." };
  }
  if (node.status === "stopping") {
    return { phase: "stopping", status: null, error: null };
  }
  const phase: NodeSessionPhase =
    slot.status === "ready"
      ? "ready"
      : slot.status === "unloaded"
        ? "unloaded"
        : slot.status === "recovery"
          ? "recovery-required"
          : "reconciling";
  const status: NodeStatus = {
    node_id: node.node_id,
    health: slot.status === "ready" ? "ready" : "unavailable",
    model: "loxa",
    engine: null,
    runtime_model: slot.status === "ready" ? slot.model_id : null,
    profile: null,
  };
  return { phase, status, error: slot.error?.message ?? null };
}

function isTerminal(operation: V2Operation): boolean {
  return operation.status === "succeeded" || operation.status === "failed" || operation.status === "cancelled";
}

function message(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
