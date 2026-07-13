import { useState } from "react";

import { useNodeSession } from "./NodeSession";
import type { NodeOwnership } from "./machine";

export type { BootstrapApi, BootstrapSnapshot, StartNodeRequest } from "./NodeSession";

export type NodeScreenServices = {
  copyText(text: string): Promise<void>;
};

const phaseLabels = {
  checking: "Checking",
  disconnected: "Disconnected",
  starting: "Starting",
  unloaded: "Node ready — no model loaded",
  ready: "Ready",
  stopping: "Stopping",
  "recovery-required": "Recovery required",
  error: "Error",
} as const;

export function NodeScreen({ services }: { services: NodeScreenServices }) {
  const session = useNodeSession();
  const [announcement, setAnnouncement] = useState("");

  const copyEndpoint = async () => {
    try {
      await services.copyText(session.endpoint);
      setAnnouncement("Endpoint copied");
    } catch {
      setAnnouncement("Could not copy endpoint");
    }
  };

  return (
    <section aria-labelledby="node-heading">
      <header className="screen-header">
        <div><p className="eyebrow">Local runtime</p><h1 id="node-heading">Node</h1></div>
        <p className={`status-badge status-${session.phase}`} role="status" aria-live="polite">{phaseLabels[session.phase]}{session.error ? `. ${session.error}` : ""}</p>
      </header>
      <p className="ownership-text">{ownershipLabel(session.ownership)}</p>
      <dl className="status-grid">
        <Field label="Endpoint" value={session.endpoint} action={<button className="quiet-button interactive-target" type="button" aria-label="Copy endpoint" onClick={() => void copyEndpoint()}>Copy</button>} />
        <Field label="Health" value={session.status?.health ?? "Not connected"} />
        <Field label="Node ID" value={session.status?.node_id ?? "—"} />
        <Field label="Engine" value={session.status?.engine?.name ?? "—"} />
        <Field label="Engine version" value={session.status?.engine?.version ?? "—"} />
        <Field label="Runtime model" value={session.status?.runtime_model ?? "—"} />
        <Field label="Profile" value={session.status?.profile ?? "—"} />
      </dl>
      <div className="action-row">
        {(session.phase === "error" || session.phase === "disconnected") && <button className="primary-button interactive-target" type="button" onClick={() => void session.retry()}>Retry node startup</button>}
        {session.ownership === "owned" && !["checking", "starting", "stopping"].includes(session.phase) && <button className="secondary-button interactive-target" type="button" onClick={() => void session.stop()}>Stop node</button>}
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
