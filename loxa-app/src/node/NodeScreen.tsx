import { useState } from "react";
import { Boxes, Copy, Play, RotateCcw, Square, TriangleAlert } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import { Button } from "../components/ui/button";
import { useNodeSession } from "./NodeSession";
import styles from "./NodeScreen.module.css";
import { DeveloperLogPanel, NodeRuntimeSummary } from "./NodeRuntimePanels";
import { NodeTable } from "./NodeTable";
import { presentNode } from "./presentation";

export type { BootstrapApi, BootstrapSnapshot, StartNodeRequest } from "./NodeSession";

export type NodeScreenServices = {
  copyText(text: string): Promise<void>;
};

export function NodeScreen({
  services,
  onNavigateModels,
}: {
  services: NodeScreenServices;
  onNavigateModels?: () => void;
}) {
  const session = useNodeSession();
  const [announcement, setAnnouncement] = useState("");
  const [selectedRowId, setSelectedRowId] = useState("local-node");
  const node = presentNode(session);
  const rows = [node];
  const selectedNode = rows.find((row) => row.rowId === selectedRowId) ?? rows[0];

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
        rows={[
          {
            ...node,
            actions: {
              copyEndpoint: (
                <Button
                  className="interactive-target"
                  variant="quiet"
                  aria-label="Copy endpoint"
                  onClick={() => void copyEndpoint()}
                >
                  <Copy aria-hidden="true" />
                  Copy
                </Button>
              ),
              model:
                session.phase === "unloaded" ? (
                  <Button className="interactive-target" onClick={onNavigateModels}>
                    <Boxes aria-hidden="true" />
                    Browse verified models
                  </Button>
                ) : undefined,
              retry:
                session.phase === "error" || session.phase === "disconnected" ? (
                  <Button className="interactive-target" onClick={() => void session.retry()}>
                    <RotateCcw aria-hidden="true" />
                    Retry node startup
                  </Button>
                ) : undefined,
              start:
                session.phase === "stopped" ? (
                  <Button className="interactive-target" onClick={() => void session.retry()}>
                    <Play aria-hidden="true" />
                    Start node
                  </Button>
                ) : undefined,
              lifecycle:
                session.ownership === "owned" && !["checking", "starting", "stopping"].includes(session.phase) ? (
                  <Button className="interactive-target" variant="secondary" onClick={() => void session.stop()}>
                    <Square aria-hidden="true" />
                    Stop node
                  </Button>
                ) : undefined,
            },
          },
        ]}
        selectedRowId={selectedNode.rowId}
        onSelectRow={setSelectedRowId}
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
      <div className={styles.runtimePanels}>
        <NodeRuntimeSummary node={selectedNode} />
        <DeveloperLogPanel />
      </div>
      <p className="visually-hidden" aria-live="polite">
        {announcement}
      </p>
    </section>
  );
}

function phaseSummary(phase: Parameters<typeof presentNode>[0]["phase"]) {
  if (phase === "unloaded") return "The private node is authenticated and ready for a verified model.";
  if (phase === "ready") return "The private node is authenticated and serving the active model.";
  if (phase === "reconciling") return "Refreshing authoritative node and model status.";
  if (phase === "checking" || phase === "starting") return "Proving local node identity.";
  if (phase === "stopping") return "Stopping the app-owned node safely.";
  if (phase === "stopped") return "The local node is stopped.";
  if (phase === "recovery-required") return "Runtime recovery is required before model controls can continue.";
  return "The local node is not currently ready.";
}
