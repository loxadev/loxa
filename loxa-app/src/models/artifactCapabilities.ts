import type { ArtifactState, ModelInventoryEntry } from "../control/contracts";

export function canEnterLoadVerification(artifact: ArtifactState): boolean {
  return artifact.kind === "downloaded" || (artifact.kind === "invalid" && artifact.reason === "verification_required");
}

export function isLoadVerificationCandidate(entry: ModelInventoryEntry): boolean {
  return canEnterLoadVerification(entry.artifact) && entry.compatibility.compatible && entry.engine.eligible;
}
