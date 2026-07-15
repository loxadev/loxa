import { Boxes, Cpu, Database, Gauge, HardDrive, Scale } from "lucide-react";

import type { ModelInventoryEntry } from "../control/contracts";
import { Badge } from "../components/ui/badge";
import { artifactLabel, formatBytes } from "./modelRowLabels";
import styles from "./ModelsScreen.module.css";

export function ModelDetail({ entry }: { entry: ModelInventoryEntry }) {
  const capabilities = [
    { label: "Parameters", value: entry.params, icon: Database },
    { label: "Quantization", value: entry.quant, icon: Gauge },
    { label: "Engine", value: entry.engine.engine, icon: Cpu },
    { label: "License", value: entry.license, icon: Scale },
    { label: "Size", value: formatBytes(entry.sizeBytes), icon: HardDrive },
  ];

  return (
    <aside className={styles.detailPanel} aria-label="Model details">
      <div className={styles.detailHeading}>
        <span className={styles.detailIcon}>
          <Boxes aria-hidden="true" size={18} />
        </span>
        <div>
          <p className={styles.detailEyebrow}>Selected model</p>
          <p className={styles.detailTitle}>{entry.id}</p>
        </div>
      </div>
      <div className={styles.capabilities} aria-label="Model capabilities">
        {capabilities.map(({ label, value, icon: Icon }) => (
          <Badge key={label} aria-label={`${label}: ${value}`} title={`${label}: ${value}`}>
            <Icon aria-hidden="true" size={12} strokeWidth={1.8} /> {value}
          </Badge>
        ))}
      </div>
      <dl className={styles.detailList}>
        <div>
          <dt>Artifact</dt>
          <dd>Status: {artifactLabel(entry.artifact, entry.sizeBytes)}</dd>
        </div>
        <div>
          <dt>Repository</dt>
          <dd className="technical-value">{entry.repo}</dd>
        </div>
        <div>
          <dt>Revision</dt>
          <dd className="technical-value">{entry.revision}</dd>
        </div>
        <div>
          <dt>File</dt>
          <dd className="technical-value">{entry.filename}</dd>
        </div>
      </dl>
      <div
        className={
          entry.compatibility.compatible && entry.engine.eligible
            ? styles.compatibility
            : `${styles.compatibility} ${styles.compatibilityBlocked}`
        }
      >
        <strong>
          {entry.compatibility.compatible && entry.engine.eligible ? "Ready for this Mac" : "Compatibility blocked"}
        </strong>
        <span>
          {entry.compatibility.reason} {entry.engine.reason}
        </span>
      </div>
    </aside>
  );
}
