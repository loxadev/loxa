import { useState } from "react";

import { useNodeSession } from "./NodeSession";
import type { NodeOwnership } from "./machine";
import styles from "./NodeScreen.module.css";

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
  reconciling: "Updating model status",
  stopping: "Stopping",
  "recovery-required": "Recovery required",
  error: "Error",
} as const;

export function NodeScreen({
  services,
  onNavigateModels,
}: {
  services: NodeScreenServices;
  onNavigateModels?: () => void;
}) {
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
    <section className={styles.screen} aria-labelledby="node-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Local runtime</p>
          <h1 id="node-heading">Node</h1>
        </div>
        <p className={`status-badge status-${session.phase}`} role="status" aria-live="polite">
          {phaseLabels[session.phase]}
          {session.error ? `. ${session.error}` : ""}
        </p>
      </header>
      <div className={styles.summary}>
        <p className={styles.ownership}>{ownershipLabel(session.ownership)}</p>
        <p className={styles.summaryText}>{phaseSummary(session.phase)}</p>
      </div>
      <dl className={styles.facts}>
        <Field
          label="Endpoint"
          value={session.endpoint}
          action={
            <button
              className="quiet-button interactive-target"
              type="button"
              aria-label="Copy endpoint"
              onClick={() => void copyEndpoint()}
            >
              Copy
            </button>
          }
        />
        <Field label="Health" value={session.status?.health ?? "Not connected"} />
        <Field label="Node ID" value={session.status?.node_id ?? "—"} />
        <Field label="Engine" value={session.status?.engine?.name ?? "—"} />
        <Field label="Engine version" value={session.status?.engine?.version ?? "—"} />
        <Field label="Runtime model" value={session.status?.runtime_model ?? "—"} />
        <Field label="Profile" value={session.status?.profile ?? "—"} />
      </dl>
      {session.phase === "unloaded" && (
        <div className={styles.nextAction}>
          <div>
            <h2>Node is running without a model</h2>
            <p>Choose a verified recipe to download or load before starting a chat.</p>
          </div>
          <button className="primary-button interactive-target" type="button" onClick={onNavigateModels}>
            Browse verified models
          </button>
        </div>
      )}
      <div className={styles.actions}>
        {(session.phase === "error" || session.phase === "disconnected") && (
          <button className="primary-button interactive-target" type="button" onClick={() => void session.retry()}>
            Retry node startup
          </button>
        )}
        {session.ownership === "owned" && !["checking", "starting", "stopping"].includes(session.phase) && (
          <button className="secondary-button interactive-target" type="button" onClick={() => void session.stop()}>
            Stop node
          </button>
        )}
      </div>
      <p className="visually-hidden" aria-live="polite">
        {announcement}
      </p>
    </section>
  );
}

function Field({ label, value, action }: { label: string; value: string; action?: React.ReactNode }) {
  return (
    <div className={styles.field}>
      <dt>{label}</dt>
      <dd>
        <span className="technical-value">{value}</span>
        {action}
      </dd>
    </div>
  );
}

function phaseSummary(phase: keyof typeof phaseLabels) {
  if (phase === "unloaded") return "The private node is authenticated and ready for a verified model.";
  if (phase === "ready") return "The private node is authenticated and serving the active model.";
  if (phase === "reconciling") return "Refreshing authoritative node and model status.";
  if (phase === "checking" || phase === "starting") return "Proving local node identity.";
  if (phase === "stopping") return "Stopping the app-owned node safely.";
  if (phase === "recovery-required") return "Runtime recovery is required before model controls can continue.";
  return "The local node is not currently ready.";
}

function ownershipLabel(ownership: NodeOwnership) {
  if (ownership === "owned") return "App-owned node";
  if (ownership === "attached") return "Externally attached";
  return "No node ownership";
}
