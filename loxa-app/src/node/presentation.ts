import type { StatusBadgeProps } from "../components/loxa/status-badge";
import type { NodeStatus } from "./contracts";
import type { NodeSessionPhase } from "./NodeSession";
import type { NodeOwnership } from "./machine";

export type NodePresentation = {
  rowId: "local-node";
  name: "Local node";
  kind: "Local";
  nodeId: string;
  statusLabel: string;
  statusTone: StatusBadgeProps["tone"];
  activeModel: string;
  engineName: string;
  engineVersion: string;
  profile: string;
  endpoint: string;
  ownership: string;
};

export function presentNode({
  phase,
  endpoint,
  ownership,
  status,
}: {
  phase: NodeSessionPhase;
  endpoint: string;
  ownership: NodeOwnership;
  status: NodeStatus | null;
}): NodePresentation {
  return {
    rowId: "local-node",
    name: "Local node",
    kind: "Local",
    nodeId: status?.node_id ?? "—",
    statusLabel: phaseLabel(phase),
    statusTone: phaseTone(phase),
    activeModel: status ? (status.runtime_model ?? "No model loaded") : "—",
    engineName: status?.engine?.name ?? "—",
    engineVersion: status?.engine?.version ?? "—",
    profile: status?.profile ?? "—",
    endpoint,
    ownership: ownershipLabel(ownership),
  };
}

function phaseLabel(phase: NodeSessionPhase) {
  const labels: Record<NodeSessionPhase, string> = {
    checking: "Checking",
    disconnected: "Disconnected",
    starting: "Starting",
    unloaded: "Node ready — no model loaded",
    ready: "Ready",
    reconciling: "Updating model status",
    stopping: "Stopping",
    stopped: "Stopped",
    "recovery-required": "Recovery required",
    error: "Error",
  };
  return labels[phase];
}

function ownershipLabel(ownership: NodeOwnership) {
  if (ownership === "owned") return "App-owned node";
  if (ownership === "attached") return "Externally attached";
  return "No node ownership";
}

function phaseTone(phase: NodeSessionPhase): StatusBadgeProps["tone"] {
  if (phase === "error" || phase === "recovery-required") return "danger";
  if (phase === "ready" || phase === "unloaded") return "success";
  if (phase === "checking" || phase === "starting" || phase === "reconciling" || phase === "stopping") return "info";
  return "neutral";
}
