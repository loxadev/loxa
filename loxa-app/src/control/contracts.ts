export type ArtifactInvalidReason = "size_mismatch" | "checksum_mismatch" | "unreadable" | "verification_required";

export type ArtifactState =
  | { kind: "not_downloaded" }
  | { kind: "partial"; bytes: number }
  | { kind: "downloaded" }
  | { kind: "invalid"; reason: ArtifactInvalidReason };

export type ModelInventoryEntry = {
  id: string;
  repo: string;
  revision: string;
  filename: string;
  sha256: string;
  sizeBytes: number;
  license: string;
  params: string;
  quant: string;
  minFreeMemoryGiB: number;
  artifact: ArtifactState;
  compatibility: { compatible: boolean; reason: string };
  engine: { engine: string; eligible: boolean; reason: string };
};

export type NodeControlStatus = "unloaded" | "loading" | "ready" | "unloading" | "recovery_required" | "error";

export type NodeSnapshot = {
  status: NodeControlStatus;
  activeModelId: string | null;
  operationId: string | null;
  error: string | null;
};

export type Capabilities = {
  documentInput: boolean;
  documentInputReason: string;
  textChat: boolean;
};

export type OperationKind = "download" | "load" | "unload";
export type OperationStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export type OperationView = {
  id: string;
  kind: OperationKind;
  status: OperationStatus;
  modelId: string | null;
  progress: { completedBytes: number; totalBytes: number | null } | null;
  error: string | null;
  createdAtUnixMs: number;
  updatedAtUnixMs: number;
};

export type ControlEvent = { sequence: number; operation: OperationView };
export type ReconnectSnapshot = {
  cursor: number;
  cursorGap: boolean;
  operations: OperationView[];
  events: ControlEvent[];
};

export type OperationAccepted = { operationId: string };
export type ControlError = { code: string; message: string };

export type NodeIdentityProof = {
  protocolVersion: 1;
  nodeId: string;
  runtimeIdentity: string;
  status: NodeControlStatus;
  challengeProof: string;
};

export class ControlContractError extends Error {
  constructor(contract: string) {
    super(`invalid ${contract}`);
    this.name = "ControlContractError";
  }
}

function invalid(contract: string): never {
  throw new ControlContractError(contract);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function isSafeNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isFiniteNonNegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function nullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function nonEmpty(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function decodeArtifact(value: unknown): ArtifactState {
  if (value === "not_downloaded" || value === "downloaded") {
    return { kind: value };
  }
  if (!isRecord(value) || Object.keys(value).length !== 1) return invalid("artifact state");
  if (isRecord(value.partial) && hasKeys(value.partial, ["bytes"]) && isSafeNonNegativeInteger(value.partial.bytes)) {
    return { kind: "partial", bytes: value.partial.bytes };
  }
  if (isRecord(value.invalid) && hasKeys(value.invalid, ["reason"])) {
    const reason = value.invalid.reason;
    if (
      reason === "size_mismatch" ||
      reason === "checksum_mismatch" ||
      reason === "unreadable" ||
      reason === "verification_required"
    ) {
      return { kind: "invalid", reason };
    }
  }
  return invalid("artifact state");
}

export function decodeInventory(value: unknown): ModelInventoryEntry[] {
  if (!Array.isArray(value)) return invalid("model inventory");
  const seen = new Set<string>();
  return value.map((item) => {
    if (
      !isRecord(item) ||
      !hasKeys(item, [
        "id",
        "repo",
        "revision",
        "filename",
        "sha256",
        "size_bytes",
        "license",
        "params",
        "quant",
        "min_free_mem_gb",
        "artifact",
        "compatibility",
        "engine",
      ])
    )
      return invalid("model inventory");
    if (
      !nonEmpty(item.id) ||
      seen.has(item.id) ||
      !nonEmpty(item.repo) ||
      !nonEmpty(item.revision) ||
      !nonEmpty(item.filename) ||
      !nonEmpty(item.sha256) ||
      !/^[0-9a-f]{64}$/.test(item.sha256) ||
      !isSafeNonNegativeInteger(item.size_bytes) ||
      !nonEmpty(item.license) ||
      !nonEmpty(item.params) ||
      !nonEmpty(item.quant) ||
      !isFiniteNonNegative(item.min_free_mem_gb) ||
      !isRecord(item.compatibility) ||
      !hasKeys(item.compatibility, ["compatible", "reason"]) ||
      typeof item.compatibility.compatible !== "boolean" ||
      typeof item.compatibility.reason !== "string" ||
      !isRecord(item.engine) ||
      !hasKeys(item.engine, ["engine", "eligible", "reason"]) ||
      !nonEmpty(item.engine.engine) ||
      typeof item.engine.eligible !== "boolean" ||
      typeof item.engine.reason !== "string"
    )
      return invalid("model inventory");
    seen.add(item.id);
    return {
      id: item.id,
      repo: item.repo,
      revision: item.revision,
      filename: item.filename,
      sha256: item.sha256,
      sizeBytes: item.size_bytes,
      license: item.license,
      params: item.params,
      quant: item.quant,
      minFreeMemoryGiB: item.min_free_mem_gb,
      artifact: decodeArtifact(item.artifact),
      compatibility: { compatible: item.compatibility.compatible, reason: item.compatibility.reason },
      engine: { engine: item.engine.engine, eligible: item.engine.eligible, reason: item.engine.reason },
    };
  });
}

function decodeNodeStatus(value: unknown): NodeControlStatus {
  if (
    value === "unloaded" ||
    value === "loading" ||
    value === "ready" ||
    value === "unloading" ||
    value === "recovery_required" ||
    value === "error"
  )
    return value;
  return invalid("node status");
}

export function decodeNodeSnapshot(value: unknown): NodeSnapshot {
  if (
    !isRecord(value) ||
    !hasKeys(value, ["status", "active_model_id", "operation_id", "error"]) ||
    !nullableString(value.active_model_id) ||
    !nullableString(value.operation_id) ||
    !nullableString(value.error)
  ) {
    return invalid("node snapshot");
  }
  return {
    status: decodeNodeStatus(value.status),
    activeModelId: value.active_model_id,
    operationId: value.operation_id,
    error: value.error,
  };
}

export function decodeCapabilities(value: unknown): Capabilities {
  if (
    !isRecord(value) ||
    !hasKeys(value, ["document_input", "document_input_reason", "text_chat"]) ||
    typeof value.document_input !== "boolean" ||
    typeof value.document_input_reason !== "string" ||
    typeof value.text_chat !== "boolean" ||
    (!value.document_input && value.document_input_reason.trim().length === 0)
  )
    return invalid("capabilities");
  return {
    documentInput: value.document_input,
    documentInputReason: value.document_input_reason,
    textChat: value.text_chat,
  };
}

export function decodeOperation(value: unknown): OperationView {
  if (
    !isRecord(value) ||
    !hasKeys(value, [
      "id",
      "kind",
      "status",
      "model_id",
      "progress",
      "error",
      "created_at_unix_ms",
      "updated_at_unix_ms",
    ]) ||
    !nonEmpty(value.id) ||
    !nullableString(value.model_id) ||
    !nullableString(value.error) ||
    !isSafeNonNegativeInteger(value.created_at_unix_ms) ||
    !isSafeNonNegativeInteger(value.updated_at_unix_ms) ||
    value.updated_at_unix_ms < value.created_at_unix_ms
  )
    return invalid("operation");
  if (value.kind !== "download" && value.kind !== "load" && value.kind !== "unload") return invalid("operation");
  if (
    value.status !== "queued" &&
    value.status !== "running" &&
    value.status !== "succeeded" &&
    value.status !== "failed" &&
    value.status !== "cancelled"
  )
    return invalid("operation");
  if ((value.status === "failed") !== nonEmpty(value.error)) return invalid("operation");
  let progress: OperationView["progress"] = null;
  if (value.progress !== null) {
    if (
      !isRecord(value.progress) ||
      !hasKeys(value.progress, ["completed_bytes", "total_bytes"]) ||
      !isSafeNonNegativeInteger(value.progress.completed_bytes) ||
      !(value.progress.total_bytes === null || isSafeNonNegativeInteger(value.progress.total_bytes)) ||
      (typeof value.progress.total_bytes === "number" && value.progress.completed_bytes > value.progress.total_bytes)
    ) {
      return invalid("operation progress");
    }
    progress = { completedBytes: value.progress.completed_bytes, totalBytes: value.progress.total_bytes };
  }
  return {
    id: value.id,
    kind: value.kind,
    status: value.status,
    modelId: value.model_id,
    progress,
    error: value.error,
    createdAtUnixMs: value.created_at_unix_ms,
    updatedAtUnixMs: value.updated_at_unix_ms,
  };
}

export function decodeControlEvent(value: unknown): ControlEvent {
  if (!isRecord(value) || !hasKeys(value, ["sequence", "operation"]) || !isSafeNonNegativeInteger(value.sequence))
    return invalid("control event");
  return { sequence: value.sequence, operation: decodeOperation(value.operation) };
}

export function decodeReconnectSnapshot(value: unknown): ReconnectSnapshot {
  if (
    !isRecord(value) ||
    !hasKeys(value, ["cursor", "cursor_gap", "operations", "events"]) ||
    !isSafeNonNegativeInteger(value.cursor) ||
    typeof value.cursor_gap !== "boolean" ||
    !Array.isArray(value.operations) ||
    !Array.isArray(value.events)
  )
    return invalid("reconnect snapshot");
  const operations = value.operations.map(decodeOperation);
  if (new Set(operations.map((item) => item.id)).size !== operations.length) return invalid("reconnect snapshot");
  const events = value.events.map(decodeControlEvent);
  let previous = -1;
  for (const event of events) {
    if (event.sequence <= previous || event.sequence > value.cursor) return invalid("reconnect snapshot");
    previous = event.sequence;
  }
  return { cursor: value.cursor, cursorGap: value.cursor_gap, operations, events };
}

export function decodeOperationAccepted(value: unknown): OperationAccepted {
  if (!isRecord(value) || !hasKeys(value, ["operation_id"]) || !nonEmpty(value.operation_id))
    return invalid("operation acceptance");
  return { operationId: value.operation_id };
}

export function decodeControlError(value: unknown): ControlError {
  if (!isRecord(value) || !hasKeys(value, ["code", "message"]) || !nonEmpty(value.code) || !nonEmpty(value.message))
    return invalid("control error");
  return { code: value.code, message: value.message };
}

export function decodeNodeIdentityProof(value: unknown): NodeIdentityProof {
  if (
    !isRecord(value) ||
    !hasKeys(value, ["protocol_version", "node_id", "runtime_identity", "status", "challenge_proof"]) ||
    value.protocol_version !== 1 ||
    !nonEmpty(value.node_id) ||
    !nonEmpty(value.runtime_identity) ||
    typeof value.challenge_proof !== "string" ||
    !/^[0-9a-f]{64}$/.test(value.challenge_proof)
  ) {
    return invalid("node identity proof");
  }
  return {
    protocolVersion: 1,
    nodeId: value.node_id,
    runtimeIdentity: value.runtime_identity,
    status: decodeNodeStatus(value.status),
    challengeProof: value.challenge_proof,
  };
}
