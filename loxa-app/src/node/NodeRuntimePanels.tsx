import { Activity, Terminal } from "lucide-react";

import type { NodePresentation } from "./presentation";
import styles from "./NodeRuntimePanels.module.css";

export function NodeRuntimeSummary({ node }: { node: NodePresentation }) {
  const details = [
    ["Node ID", node.nodeId],
    ["Active model", node.activeModel],
    ["Engine", available(node.engineName)],
    ["Version", available(node.engineVersion)],
    ["Profile", available(node.profile)],
    ["Endpoint", node.endpoint],
  ] as const;

  return (
    <section className={styles.panel} aria-label="Selected node runtime">
      <header className={styles.panelHeader}>
        <Activity aria-hidden="true" />
        <div>
          <p className={styles.kicker}>Selected node</p>
          <h2 id="selected-runtime-heading">{node.name} runtime</h2>
        </div>
      </header>
      <dl className={styles.details}>
        {details.map(([label, value]) => (
          <div className={styles.detail} key={label}>
            <dt>{label}</dt>
            <dd className={technicalLabels.has(label) ? "technical-value" : undefined}>{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

export function DeveloperLogPanel() {
  return (
    <section className={styles.panel} aria-labelledby="developer-logs-heading">
      <header className={styles.panelHeader}>
        <Terminal aria-hidden="true" />
        <div>
          <p className={styles.kicker}>Diagnostics</p>
          <h2 id="developer-logs-heading">Developer logs unavailable</h2>
        </div>
      </header>
      <p className={styles.empty}>Developer logs are unavailable because this backend does not expose a log source.</p>
    </section>
  );
}

const technicalLabels = new Set(["Node ID", "Active model", "Engine", "Version", "Profile", "Endpoint"]);

function available(value: string) {
  return value === "—" ? "Unavailable" : value;
}
