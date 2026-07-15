import type { ModelInventoryEntry, NodeSnapshot, OperationView } from "../control/contracts";
import { Button } from "../components/ui/button";
import { TableCell, TableRow } from "../components/ui/table";
import { artifactLabel, formatBytes, operationLabel } from "./modelRowLabels";
import styles from "./ModelsScreen.module.css";

export function ModelRow({
  entry,
  operation,
  unloadOperation,
  pending,
  active,
  selected,
  node,
  mutationBusy,
  onSelect,
  onDownload,
  onLoad,
  onUnload,
  onCancel,
}: {
  entry: ModelInventoryEntry;
  operation?: OperationView;
  unloadOperation?: OperationView;
  pending: boolean;
  active: boolean;
  selected: boolean;
  node: NodeSnapshot | null;
  mutationBusy: boolean;
  onSelect(): void;
  onDownload(): void;
  onLoad(): void;
  onUnload(): void;
  onCancel(operation: OperationView): void;
}) {
  const headingId = `model-${entry.id}`;
  const reasonId = `model-reason-${entry.id}`;
  const actionable = entry.compatibility.compatible && entry.engine.eligible;
  const displayedOperation = unloadOperation ?? operation;
  const inProgress = displayedOperation?.status === "queued" || displayedOperation?.status === "running";
  const status =
    inProgress && displayedOperation
      ? operationLabel(displayedOperation)
      : artifactLabel(entry.artifact, entry.sizeBytes);
  const showDownload =
    !inProgress &&
    entry.artifact.kind !== "downloaded" &&
    !(entry.artifact.kind === "invalid" && entry.artifact.reason === "verification_required");
  const actionLabel =
    entry.artifact.kind === "partial"
      ? `Resume ${entry.id}`
      : entry.artifact.kind === "invalid"
        ? `Repair ${entry.id}`
        : `Download ${entry.id}`;

  return (
    <TableRow className={selected ? `${styles.modelRow} ${styles.selectedRow}` : styles.modelRow}>
      <TableCell>
        <button type="button" className={styles.modelIdentity} onClick={onSelect} aria-pressed={selected}>
          <span className={styles.modelMonogram} aria-hidden="true">
            {entry.id.slice(0, 2).toUpperCase()}
          </span>
          <span className={styles.modelCopy}>
            <span id={headingId} role="heading" aria-level={2} className={styles.modelName}>
              {entry.id}
            </span>
            <span className="technical-value">{entry.repo}</span>
          </span>
        </button>
        {displayedOperation?.progress && (
          <div className={styles.progress}>
            {displayedOperation.progress.totalBytes === null ? (
              <progress aria-label={`Download progress for ${entry.id}`} />
            ) : (
              <progress
                aria-label={`Download progress for ${entry.id}`}
                value={displayedOperation.progress.completedBytes}
                max={displayedOperation.progress.totalBytes}
              />
            )}
            <span className="technical-value">
              {formatBytes(displayedOperation.progress.completedBytes)}
              {displayedOperation.progress.totalBytes === null
                ? " downloaded"
                : ` of ${formatBytes(displayedOperation.progress.totalBytes)}`}
            </span>
          </div>
        )}
        {displayedOperation?.error && <p className={styles.operationError}>{displayedOperation.error}</p>}
        {displayedOperation && !inProgress && (
          <p className={styles.operationHistory}>Last operation: {operationLabel(displayedOperation)}</p>
        )}
      </TableCell>
      <TableCell className="technical-value">
        {entry.params} · {entry.quant}
      </TableCell>
      <TableCell className="technical-value">{formatBytes(entry.sizeBytes)}</TableCell>
      <TableCell>
        <div className={styles.stateStack}>
          {active && <span className={`${styles.stateBadge} ${styles.activeChip}`}>Active</span>}
          <span className={styles.stateBadge}>{status}</span>
        </div>
        <span id={reasonId} className="visually-hidden">
          {entry.compatibility.reason} {entry.engine.reason}
        </span>
      </TableCell>
      <TableCell className={styles.rowActions}>
        {inProgress && displayedOperation?.kind === "download" ? (
          <Button
            variant="secondary"
            disabled={pending}
            onClick={() => onCancel(displayedOperation)}
            aria-label={`Cancel ${displayedOperation.kind} ${entry.id}`}
          >
            Cancel
          </Button>
        ) : inProgress && displayedOperation ? (
          <span className={styles.actionLabel}>{operationLabel(displayedOperation)}</span>
        ) : showDownload ? (
          <Button
            disabled={!actionable || pending || mutationBusy}
            aria-describedby={reasonId}
            aria-label={actionLabel}
            onClick={onDownload}
          >
            {entry.artifact.kind === "partial" ? "Resume" : entry.artifact.kind === "invalid" ? "Repair" : "Download"}
          </Button>
        ) : entry.artifact.kind === "downloaded" && actionable && node?.status !== "recovery_required" ? (
          <Button
            variant={active ? "secondary" : "primary"}
            disabled={pending || mutationBusy}
            onClick={active ? onUnload : onLoad}
            aria-label={
              active ? `Unload ${entry.id}` : node?.activeModelId ? `Switch to ${entry.id}` : `Load ${entry.id}`
            }
          >
            {active ? "Unload" : node?.activeModelId ? "Switch" : "Load"}
          </Button>
        ) : (
          <span className={styles.actionLabel}>
            {entry.artifact.kind === "downloaded" ? "Unavailable to load" : "Awaiting verification"}
          </span>
        )}
      </TableCell>
    </TableRow>
  );
}
