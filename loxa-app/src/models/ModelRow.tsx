import type { ModelInventoryEntry, NodeSnapshot, OperationView } from "../control/contracts";
import { artifactLabel, formatBytes, operationLabel } from "./modelRowLabels";
import styles from "./ModelsScreen.module.css";

export function ModelRow({
  entry,
  operation,
  unloadOperation,
  pending,
  active,
  node,
  mutationBusy,
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
  node: NodeSnapshot | null;
  mutationBusy: boolean;
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
    <article className={styles.row} aria-labelledby={headingId}>
      <div className={styles.main}>
        <div className={styles.headingLine}>
          <h2 id={headingId}>{entry.id}</h2>
          {active && <span className={`${styles.chip} ${styles.activeChip}`}>Active</span>}
          <span className={styles.chip}>{status}</span>
        </div>
        <p className={styles.metadata}>
          <span>{entry.params}</span>
          <span>{entry.quant}</span>
          <span>{formatBytes(entry.sizeBytes)}</span>
          <span>{entry.license}</span>
          <span>{entry.engine.engine}</span>
        </p>
        <p className={`technical-value ${styles.repository}`}>
          {entry.repo}@{entry.revision}
        </p>
        <p id={reasonId} className={actionable ? styles.reason : `${styles.reason} ${styles.reasonBlocking}`}>
          {entry.compatibility.reason} {entry.engine.reason}
        </p>
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
      </div>
      <div className={styles.actions}>
        {inProgress && displayedOperation?.kind === "download" ? (
          <button
            className="secondary-button interactive-target"
            type="button"
            disabled={pending}
            onClick={() => onCancel(displayedOperation)}
            aria-label={`Cancel ${displayedOperation.kind} ${entry.id}`}
          >
            Cancel
          </button>
        ) : inProgress && displayedOperation ? (
          <span className={styles.actionLabel}>{operationLabel(displayedOperation)}</span>
        ) : showDownload ? (
          <button
            className="primary-button interactive-target"
            type="button"
            disabled={!actionable || pending || mutationBusy}
            aria-describedby={reasonId}
            aria-label={actionLabel}
            onClick={onDownload}
          >
            {entry.artifact.kind === "partial" ? "Resume" : entry.artifact.kind === "invalid" ? "Repair" : "Download"}
          </button>
        ) : entry.artifact.kind === "downloaded" && actionable && node?.status !== "recovery_required" ? (
          <button
            className={active ? "secondary-button interactive-target" : "primary-button interactive-target"}
            type="button"
            disabled={pending || mutationBusy}
            onClick={active ? onUnload : onLoad}
            aria-label={
              active ? `Unload ${entry.id}` : node?.activeModelId ? `Switch to ${entry.id}` : `Load ${entry.id}`
            }
          >
            {active ? "Unload" : node?.activeModelId ? "Switch" : "Load"}
          </button>
        ) : (
          <span className={styles.actionLabel}>
            {entry.artifact.kind === "downloaded" ? "Unavailable to load" : "Awaiting verification"}
          </span>
        )}
      </div>
    </article>
  );
}
