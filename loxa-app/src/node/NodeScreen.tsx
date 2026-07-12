import { useCallback, useEffect, useReducer, useRef, useState } from "react";

import { getStatus as defaultGetStatus } from "./client";
import { actionGuards, initialNodeState, nodeReducer, type NodePhase, type NodeOwnership } from "./machine";

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

export type NodeScreenServices = {
  bootstrap: BootstrapApi;
  getStatus: typeof defaultGetStatus;
  copyText(text: string): Promise<void>;
};

const phaseLabels: Record<NodePhase, string> = {
  disconnected: "Disconnected",
  connecting: "Connecting",
  starting: "Starting",
  attached: "Attached — runtime unavailable",
  ready: "Ready",
  stopping: "Stopping",
  "recovery-required": "Recovery required",
  error: "Error",
};

export function NodeScreen({ services, initialPhase, onEndpointChange }: { services: NodeScreenServices; initialPhase?: NodePhase; onEndpointChange?: (endpoint: string) => void }) {
  const [state, dispatch] = useReducer(nodeReducer, undefined, () => ({ ...initialNodeState(), ...(initialPhase ? { phase: initialPhase } : {}) }));
  const [endpoint, setEndpoint] = useState("http://127.0.0.1:8080");
  const [announcement, setAnnouncement] = useState("");
  const mounted = useRef(true);

  const applySnapshot = useCallback(async (snapshot: BootstrapSnapshot, probe: boolean) => {
    if (!mounted.current) return;
    setEndpoint(snapshot.endpoint);
    onEndpointChange?.(snapshot.endpoint);
    dispatch({ type: "ownership", ownership: snapshot.ownership });
    if (snapshot.error) {
      dispatch({ type: snapshot.error.toLowerCase().includes("recovery required") ? "recoveryRequired" : "probeFailed", message: snapshot.error });
      return;
    }
    if (probe && snapshot.ownership !== "none") {
      try {
        const status = await services.getStatus(snapshot.endpoint);
        if (mounted.current) dispatch({ type: "status", status });
      } catch (error) {
        if (mounted.current) dispatch({ type: "probeFailed", message: message(error) });
      }
    }
  }, [onEndpointChange, services]);

  useEffect(() => {
    mounted.current = true;
    void services.bootstrap.snapshot().then((snapshot) => applySnapshot(snapshot, true)).catch((error) => {
      if (mounted.current) dispatch({ type: "probeFailed", message: message(error) });
    });
    return () => { mounted.current = false; };
  }, [applySnapshot, services.bootstrap]);

  const operate = async (kind: "start" | "attach" | "stop") => {
    dispatch({ type: kind === "attach" ? "connect" : kind });
    try {
      const snapshot = kind === "start"
        ? await services.bootstrap.start({ endpoint })
        : kind === "attach"
          ? await services.bootstrap.attach(endpoint)
          : await services.bootstrap.stop();
      if (kind === "stop") {
        if (mounted.current) dispatch({ type: "stopped" });
      } else {
        await applySnapshot(snapshot, true);
      }
    } catch (error) {
      if (mounted.current) dispatch({ type: kind === "stop" ? "stopFailed" : "probeFailed", message: message(error) });
    }
  };

  const copyEndpoint = async () => {
    try {
      await services.copyText(endpoint);
      if (mounted.current) setAnnouncement("Endpoint copied");
    } catch {
      if (mounted.current) setAnnouncement("Could not copy endpoint");
    }
  };

  const guards = actionGuards(state);
  return (
    <section aria-labelledby="node-heading">
      <header className="screen-header">
        <div><p className="eyebrow">Local runtime</p><h1 id="node-heading">Node</h1></div>
        <p className={`status-badge status-${state.phase}`} role="status" aria-live="polite">{phaseLabels[state.phase]}{state.error ? `. ${state.error}` : ""}</p>
      </header>
      <p className="ownership-text">{ownershipLabel(state.ownership)}</p>
      <dl className="status-grid">
        <Field label="Endpoint" value={endpoint} action={<button className="quiet-button interactive-target" type="button" aria-label="Copy endpoint" onClick={() => void copyEndpoint()}>Copy</button>} />
        <Field label="Health" value={state.status?.health ?? "Not connected"} />
        <Field label="Node ID" value={state.status?.node_id ?? "—"} />
        <Field label="Engine" value={state.status?.engine?.name ?? "—"} />
        <Field label="Engine version" value={state.status?.engine?.version ?? "—"} />
        <Field label="Runtime model" value={state.status?.runtime_model ?? "—"} />
        <Field label="Profile" value={state.status?.profile ?? "—"} />
      </dl>
      <div className="action-row">
        {guards.canStart && <button className="primary-button interactive-target" type="button" onClick={() => void operate("start")}>Start node</button>}
        {guards.canAttachRetry && <button className="secondary-button interactive-target" type="button" onClick={() => void operate("attach")}>Attach or retry</button>}
        {guards.canStop && <button className="secondary-button interactive-target" type="button" onClick={() => void operate("stop")}>Stop node</button>}
      </div>
      <p className="visually-hidden" aria-live="polite">{announcement}</p>
    </section>
  );
}

function Field({ label, value, action }: { label: string; value: string; action?: React.ReactNode }) {
  return <div className="status-field"><dt>{label}</dt><dd><span className="technical-value">{value}</span>{action}</dd></div>;
}

function ownershipLabel(ownership: NodeOwnership) {
  if (ownership === "owned") return "App-owned node";
  if (ownership === "attached") return "Externally attached";
  return "No node ownership";
}

function message(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
