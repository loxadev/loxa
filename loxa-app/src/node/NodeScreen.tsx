import { useState } from "react";
import { TriangleAlert } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import { useNodeSession } from "./NodeSession";
import type { NodeOwnership } from "./machine";
import styles from "./NodeScreen.module.css";
import { NodeTable } from "./NodeTable";

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
          <h1 id="node-heading">Nodes</h1>
        </div>
      </header>
      <p className={styles.intro}>{phaseSummary(session.phase)}</p>
      <NodeTable
        nodeId={session.status?.node_id ?? "—"}
        statusLabel={phaseLabels[session.phase]}
        statusTone={statusTone(session.phase)}
        health={session.status?.health ?? "Not connected"}
        activeModel={session.status ? (session.status.runtime_model ?? "No model loaded") : "—"}
        engineName={session.status?.engine?.name ?? "—"}
        engineVersion={session.status?.engine?.version ?? "—"}
        profile={session.status?.profile ?? "—"}
        endpoint={session.endpoint}
        ownership={ownershipLabel(session.ownership)}
        actions={{
          copyEndpoint: (
            <button
              className="quiet-button interactive-target"
              type="button"
              aria-label="Copy endpoint"
              onClick={() => void copyEndpoint()}
            >
              Copy
            </button>
          ),
          model:
            session.phase === "unloaded" ? (
              <button className="primary-button interactive-target" type="button" onClick={onNavigateModels}>
                Browse verified models
              </button>
            ) : undefined,
          retry:
            session.phase === "error" || session.phase === "disconnected" ? (
              <button className="primary-button interactive-target" type="button" onClick={() => void session.retry()}>
                Retry node startup
              </button>
            ) : undefined,
          lifecycle:
            session.ownership === "owned" && !["checking", "starting", "stopping"].includes(session.phase) ? (
              <button className="secondary-button interactive-target" type="button" onClick={() => void session.stop()}>
                Stop node
              </button>
            ) : undefined,
        }}
      />
      {session.error && (
        <Alert className={styles.errorAlert} variant="danger">
          <TriangleAlert aria-hidden="true" className={`${styles.errorIcon} text-danger`} />
          <div>
            <AlertTitle>{session.phase === "recovery-required" ? "Recovery required" : "Node unavailable"}</AlertTitle>
            <AlertDescription>{session.error}</AlertDescription>
          </div>
        </Alert>
      )}
      <p className="visually-hidden" aria-live="polite">
        {announcement}
      </p>
    </section>
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

function statusTone(phase: keyof typeof phaseLabels) {
  if (phase === "error" || phase === "recovery-required") return "danger";
  if (phase === "ready" || phase === "unloaded") return "success";
  if (phase === "checking" || phase === "starting" || phase === "reconciling" || phase === "stopping") return "info";
  return "neutral";
}
