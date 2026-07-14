import type { NodeStatus } from "./contracts";

export type NodePhase =
  "disconnected" | "connecting" | "starting" | "attached" | "ready" | "stopping" | "recovery-required" | "error";

export type NodeOwnership = "none" | "attached" | "owned";

export type NodeState = {
  phase: NodePhase;
  ownership: NodeOwnership;
  status: NodeStatus | null;
  error: string | null;
};

export type NodeEvent =
  | { type: "connect" }
  | { type: "start" }
  | { type: "ownership"; ownership: NodeOwnership }
  | { type: "status"; status: NodeStatus }
  | { type: "probeFailed"; message: string }
  | { type: "stop" }
  | { type: "stopFailed"; message: string }
  | { type: "stopped" }
  | { type: "recoveryRequired"; message: string };

export type ActionGuards = {
  canStart: boolean;
  canAttachRetry: boolean;
  canStop: boolean;
};

const legalEvents: Record<NodePhase, readonly NodeEvent["type"][]> = {
  disconnected: ["connect", "start", "ownership", "status", "probeFailed", "recoveryRequired"],
  connecting: ["ownership", "status", "probeFailed", "recoveryRequired"],
  starting: ["ownership", "status", "probeFailed", "recoveryRequired"],
  attached: ["ownership", "status", "probeFailed", "stop", "recoveryRequired"],
  ready: ["ownership", "status", "probeFailed", "stop", "recoveryRequired"],
  stopping: ["ownership", "stopFailed", "stopped", "recoveryRequired"],
  "recovery-required": ["ownership", "recoveryRequired"],
  error: ["connect", "start", "ownership", "stop", "recoveryRequired"],
};

export function initialNodeState(): NodeState {
  return {
    phase: "disconnected",
    ownership: "none",
    status: null,
    error: null,
  };
}

export function actionGuards(state: NodeState): ActionGuards {
  const retryable = state.phase === "disconnected" || state.phase === "error";
  return {
    canStart: retryable && state.ownership === "none",
    canAttachRetry: retryable && state.ownership !== "owned",
    canStop:
      state.ownership === "owned" && (state.phase === "attached" || state.phase === "ready" || state.phase === "error"),
  };
}

export function nodeReducer(state: NodeState, event: NodeEvent): NodeState {
  if (!legalEvents[state.phase].includes(event.type)) return state;
  switch (event.type) {
    case "connect":
      return actionGuards(state).canAttachRetry ? { ...state, phase: "connecting", status: null, error: null } : state;
    case "start":
      return actionGuards(state).canStart ? { ...state, phase: "starting", status: null, error: null } : state;
    case "ownership":
      return { ...state, ownership: event.ownership };
    case "status":
      return {
        ...state,
        phase: event.status.health === "ready" ? "ready" : "attached",
        status: event.status,
        error: null,
      };
    case "probeFailed":
    case "stopFailed":
      return { ...state, phase: "error", status: null, error: event.message };
    case "stop":
      return actionGuards(state).canStop ? { ...state, phase: "stopping", error: null } : state;
    case "stopped":
      return state.phase === "stopping" && state.ownership === "owned" ? initialNodeState() : state;
    case "recoveryRequired":
      return {
        ...state,
        phase: "recovery-required",
        status: null,
        error: event.message,
      };
  }
}
