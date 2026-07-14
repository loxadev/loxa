import type { ArtifactInvalidReason, ArtifactState, OperationView } from "../control/contracts";

export function artifactLabel(artifact: ArtifactState, sizeBytes: number): string {
  if (artifact.kind === "not_downloaded") return "Not downloaded";
  if (artifact.kind === "partial") return `Partial — ${formatBytes(artifact.bytes)} of ${formatBytes(sizeBytes)}`;
  if (artifact.kind === "downloaded") return "Downloaded and verified";
  const labels: Record<ArtifactInvalidReason, string> = {
    size_mismatch: "Size mismatch",
    checksum_mismatch: "Checksum invalid",
    unreadable: "Unreadable artifact",
    verification_required: "Verification required",
  };
  return labels[artifact.reason];
}

export function operationLabel(operation: OperationView): string {
  const action = operation.kind === "download" ? "Download" : operation.kind === "load" ? "Load" : "Unload";
  if (operation.status === "running") return `${action} in progress`;
  if (operation.status === "succeeded") return `${action} completed`;
  if (operation.status === "failed") return `${action} failed`;
  return `${action} ${operation.status}`;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  const formatted = value >= 10 || Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1);
  return `${formatted} ${units[index]}`;
}
