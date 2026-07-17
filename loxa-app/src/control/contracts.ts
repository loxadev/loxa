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

function boundedOpaqueIdentity(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && new TextEncoder().encode(value).byteLength <= 1024;
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
    !boundedOpaqueIdentity(value.node_id) ||
    !boundedOpaqueIdentity(value.runtime_identity) ||
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

const MAX_V2_SNAPSHOT_BYTES = 2 * 1024 * 1024;
const MAX_V2_EVENT_BYTES = 16 * 1024;
const MAX_V2_OPERATIONS = 256;
const MAX_V2_EVENTS = 1024;
const MAX_U64 = 18_446_744_073_709_551_615n;

export type DecimalString = string & { readonly __decimalString: unique symbol };
export type V2Uuid = string & { readonly __v2Uuid: unique symbol };

export type V2NodeCapabilities = {
  model_download: boolean;
  slot_load: boolean;
  slot_unload: boolean;
  operation_cancel: boolean;
  operation_stream: boolean;
};

export type V2Node = {
  node_id: V2Uuid;
  node_instance_id: V2Uuid;
  control_endpoint: string;
  status: "running" | "stopping";
  slot_capacity: 1;
  capabilities: V2NodeCapabilities;
};

export type V2PublicError = { code: string; message: string };
export type V2Slot = {
  slot_id: V2Uuid;
  node_id: V2Uuid;
  name: "default";
  status: "unloaded" | "loading" | "ready" | "unloading" | "recovery";
  model_id: string | null;
  operation_id: V2Uuid | null;
  error: V2PublicError | null;
};

export type V2OperationProgress = { completed_bytes: DecimalString; total_bytes: DecimalString | null };
export type V2Operation = {
  operation_id: V2Uuid;
  node_id: V2Uuid;
  kind: "download" | "load" | "unload";
  status: "queued" | "running" | "cancelling" | "succeeded" | "failed" | "cancelled";
  slot_id: V2Uuid | null;
  model_id: string | null;
  progress: V2OperationProgress | null;
  error: V2PublicError | null;
  created_revision: DecimalString;
  updated_revision: DecimalString;
  created_at_unix_ms: DecimalString;
  updated_at_unix_ms: DecimalString;
};

export type V2NodeCollection = {
  schema_version: 2;
  epoch: V2Uuid;
  revision: DecimalString;
  generated_at_unix_ms: DecimalString;
  nodes: V2Node[];
};

export type V2SlotCollection = {
  schema_version: 2;
  epoch: V2Uuid;
  revision: DecimalString;
  generated_at_unix_ms: DecimalString;
  node_id: V2Uuid;
  slots: V2Slot[];
};

export type V2OperationCollection = {
  schema_version: 2;
  epoch: V2Uuid;
  revision: DecimalString;
  generated_at_unix_ms: DecimalString;
  operations: V2Operation[];
};

export type V2ControlEvent = {
  schema_version: 2;
  event_id: V2Uuid;
  epoch: V2Uuid;
  sequence: DecimalString;
  revision: DecimalString;
  committed_at_unix_ms: DecimalString;
  entity: "node" | "slot" | "operation";
  entity_id: V2Uuid;
  node_id: V2Uuid;
  node_instance_id: V2Uuid | null;
  slot_id: V2Uuid | null;
  operation_id: V2Uuid | null;
  node: V2Node | null;
  slot: V2Slot | null;
  operation: V2Operation | null;
};

export type V2ReconnectSnapshot = {
  schema_version: 2;
  epoch: V2Uuid;
  revision: DecimalString;
  generated_at_unix_ms: DecimalString;
  stream: { epoch: V2Uuid; cursor: DecimalString; cursor_gap: boolean };
  nodes: V2Node[];
  slots: V2Slot[];
  operations: V2Operation[];
  events: V2ControlEvent[];
};

export type V2OperationAccepted = {
  epoch: V2Uuid;
  operation_id: V2Uuid;
  revision: DecimalString;
};

export type V2OperationEnvelope = {
  schema_version: 2;
  epoch: V2Uuid;
  revision: DecimalString;
  generated_at_unix_ms: DecimalString;
  operation: V2Operation;
};

export type V2ControlErrorBody = { code: string; message: string };

type RawJson = string | Uint8Array;

function decodeRawJson(value: RawJson, maxBytes: number, contract: string): unknown {
  const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
  if (bytes.byteLength > maxBytes) return invalid(contract);
  let text: string;
  try {
    text = typeof value === "string" ? value : new TextDecoder("utf-8", { fatal: true }).decode(value);
    assertJsonHasUniqueKeys(text, contract);
    return JSON.parse(text) as unknown;
  } catch (error) {
    if (error instanceof ControlContractError) throw error;
    return invalid(contract);
  }
}

function assertJsonHasUniqueKeys(text: string, contract: string): void {
  let offset = 0;
  const fail = (): never => invalid(contract);
  const whitespace = (): void => {
    while (offset < text.length && /[\t\n\r ]/.test(text[offset] ?? "")) offset += 1;
  };
  const consume = (expected: string): void => {
    if (text[offset] !== expected) fail();
    offset += 1;
  };
  const parseString = (): string => {
    const start = offset;
    consume('"');
    while (offset < text.length) {
      const character = text[offset];
      if (character === '"') {
        offset += 1;
        try {
          return JSON.parse(text.slice(start, offset)) as string;
        } catch {
          return fail();
        }
      }
      if (character === "\\") {
        offset += 1;
        const escape = text[offset];
        if (escape === "u") {
          if (!/^[0-9a-fA-F]{4}$/.test(text.slice(offset + 1, offset + 5))) fail();
          const codeUnit = Number.parseInt(text.slice(offset + 1, offset + 5), 16);
          if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
            if (text[offset + 5] !== "\\" || text[offset + 6] !== "u") fail();
            if (!/^[0-9a-fA-F]{4}$/.test(text.slice(offset + 7, offset + 11))) fail();
            const lowSurrogate = Number.parseInt(text.slice(offset + 7, offset + 11), 16);
            if (lowSurrogate < 0xdc00 || lowSurrogate > 0xdfff) fail();
            offset += 11;
            continue;
          }
          if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) fail();
          offset += 5;
          continue;
        }
        if (!escape || !'"\\/bfnrt'.includes(escape)) fail();
        offset += 1;
        continue;
      }
      if (!character || character.charCodeAt(0) < 0x20) fail();
      const codeUnit = character.charCodeAt(0);
      if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
        const lowSurrogate = text.charCodeAt(offset + 1);
        if (lowSurrogate < 0xdc00 || lowSurrogate > 0xdfff) fail();
        offset += 2;
        continue;
      }
      if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) fail();
      offset += 1;
    }
    return fail();
  };
  const parseValue = (): void => {
    whitespace();
    const character = text[offset];
    if (character === "{") {
      parseObject();
      return;
    }
    if (character === "[") {
      parseArray();
      return;
    }
    if (character === '"') {
      parseString();
      return;
    }
    for (const literal of ["true", "false", "null"]) {
      if (text.startsWith(literal, offset)) {
        offset += literal.length;
        return;
      }
    }
    const number = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(text.slice(offset));
    if (!number) return fail();
    offset += number[0].length;
  };
  const parseObject = (): void => {
    consume("{");
    whitespace();
    const keys = new Set<string>();
    if (text[offset] === "}") {
      offset += 1;
      return;
    }
    for (;;) {
      whitespace();
      const key = parseString();
      if (keys.has(key)) fail();
      keys.add(key);
      whitespace();
      consume(":");
      parseValue();
      whitespace();
      if (text[offset] === "}") {
        offset += 1;
        return;
      }
      consume(",");
    }
  };
  const parseArray = (): void => {
    consume("[");
    whitespace();
    if (text[offset] === "]") {
      offset += 1;
      return;
    }
    for (;;) {
      parseValue();
      whitespace();
      if (text[offset] === "]") {
        offset += 1;
        return;
      }
      consume(",");
    }
  };
  whitespace();
  parseValue();
  whitespace();
  if (offset !== text.length) fail();
}

function exactRecord(value: unknown, keys: readonly string[], contract: string): Record<string, unknown> {
  if (!isRecord(value) || !hasKeys(value, keys)) return invalid(contract);
  return value;
}

function oneOf<T extends string>(value: unknown, choices: readonly T[], contract: string): T {
  if (typeof value !== "string" || !choices.includes(value as T)) return invalid(contract);
  return value as T;
}

function boundedV2Text(value: unknown, contract: string): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.trim() !== value ||
    /[\u0000-\u001f\u007f-\u009f]/u.test(value) ||
    new TextEncoder().encode(value).byteLength > 256
  )
    return invalid(contract);
  return value;
}

function nullableModelId(value: unknown): string | null {
  return value === null ? null : boundedV2Text(value, "v2 model ID");
}

function decodeV2Uuid(value: unknown): V2Uuid {
  if (typeof value !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value))
    return invalid("v2 UUID");
  return value as V2Uuid;
}

export function decodeDecimalString(value: unknown): DecimalString {
  if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)) return invalid("decimal string");
  let parsed: bigint;
  try {
    parsed = BigInt(value);
  } catch {
    return invalid("decimal string");
  }
  if (parsed > MAX_U64) return invalid("decimal string");
  return value as DecimalString;
}

function compareDecimal(left: DecimalString, right: DecimalString): number {
  const leftValue = BigInt(left);
  const rightValue = BigInt(right);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

function decodeV2Capabilities(value: unknown): V2NodeCapabilities {
  const record = exactRecord(
    value,
    ["model_download", "slot_load", "slot_unload", "operation_cancel", "operation_stream"],
    "v2 node capabilities",
  );
  for (const key of Object.keys(record)) if (typeof record[key] !== "boolean") return invalid("v2 node capabilities");
  return record as V2NodeCapabilities;
}

function decodeV2Endpoint(value: unknown): string {
  if (typeof value !== "string" || value.length > 256 || /[@?#]/.test(value)) return invalid("v2 control endpoint");
  const match = /^http:\/\/(?:127\.0\.0\.1|localhost|\[::1\]):([0-9]+)$/.exec(value);
  if (!match) return invalid("v2 control endpoint");
  const port = Number(match[1]);
  if (!Number.isInteger(port) || port < 1 || port > 65535) return invalid("v2 control endpoint");
  return value;
}

function decodeV2Node(value: unknown): V2Node {
  const record = exactRecord(
    value,
    ["node_id", "node_instance_id", "control_endpoint", "status", "slot_capacity", "capabilities"],
    "v2 node",
  );
  if (record.slot_capacity !== 1) return invalid("v2 node");
  decodeV2Uuid(record.node_id);
  decodeV2Uuid(record.node_instance_id);
  decodeV2Endpoint(record.control_endpoint);
  oneOf(record.status, ["running", "stopping"] as const, "v2 node status");
  decodeV2Capabilities(record.capabilities);
  return record as V2Node;
}

function decodeV2PublicError(value: unknown, codes: readonly string[], contract: string): V2PublicError {
  const record = exactRecord(value, ["code", "message"], contract);
  oneOf(record.code, codes, contract);
  boundedV2Text(record.message, contract);
  return record as V2PublicError;
}

function decodeV2Slot(value: unknown): V2Slot {
  const record = exactRecord(
    value,
    ["slot_id", "node_id", "name", "status", "model_id", "operation_id", "error"],
    "v2 slot",
  );
  decodeV2Uuid(record.slot_id);
  decodeV2Uuid(record.node_id);
  if (record.name !== "default") return invalid("v2 slot");
  const status = oneOf(
    record.status,
    ["unloaded", "loading", "ready", "unloading", "recovery"] as const,
    "v2 slot status",
  );
  const modelId = nullableModelId(record.model_id);
  const operationId = record.operation_id === null ? null : decodeV2Uuid(record.operation_id);
  const error =
    record.error === null ? null : decodeV2PublicError(record.error, ["lifecycle_recovery_required"], "v2 slot error");
  const legal =
    (status === "unloaded" && modelId === null && operationId === null && error === null) ||
    (status === "loading" && operationId !== null && error === null) ||
    (status === "ready" && modelId !== null && operationId === null && error === null) ||
    (status === "unloading" && modelId !== null && operationId !== null && error === null) ||
    (status === "recovery" && operationId === null && error !== null);
  if (!legal) return invalid("v2 slot correlation");
  return record as V2Slot;
}

function decodeV2Progress(value: unknown): V2OperationProgress {
  const record = exactRecord(value, ["completed_bytes", "total_bytes"], "v2 operation progress");
  const completed = decodeDecimalString(record.completed_bytes);
  const total = record.total_bytes === null ? null : decodeDecimalString(record.total_bytes);
  if (total !== null && compareDecimal(completed, total) > 0) return invalid("v2 operation progress");
  return record as V2OperationProgress;
}

const operationErrorCodes = [
  "download_failed",
  "load_failed",
  "unload_failed",
  "node_restarted_before_start",
  "node_restarted",
  "cancellation_outcome_unknown",
] as const;

function decodeV2Operation(value: unknown): V2Operation {
  const record = exactRecord(
    value,
    [
      "operation_id",
      "node_id",
      "kind",
      "status",
      "slot_id",
      "model_id",
      "progress",
      "error",
      "created_revision",
      "updated_revision",
      "created_at_unix_ms",
      "updated_at_unix_ms",
    ],
    "v2 operation",
  );
  decodeV2Uuid(record.operation_id);
  decodeV2Uuid(record.node_id);
  const kind = oneOf(record.kind, ["download", "load", "unload"] as const, "v2 operation kind");
  const status = oneOf(
    record.status,
    ["queued", "running", "cancelling", "succeeded", "failed", "cancelled"] as const,
    "v2 operation status",
  );
  const slotId = record.slot_id === null ? null : decodeV2Uuid(record.slot_id);
  const modelId = nullableModelId(record.model_id);
  const progress = record.progress === null ? null : decodeV2Progress(record.progress);
  const error =
    record.error === null ? null : decodeV2PublicError(record.error, operationErrorCodes, "v2 operation error");
  const createdRevision = decodeDecimalString(record.created_revision);
  const updatedRevision = decodeDecimalString(record.updated_revision);
  const createdAt = decodeDecimalString(record.created_at_unix_ms);
  const updatedAt = decodeDecimalString(record.updated_at_unix_ms);
  if (compareDecimal(createdRevision, updatedRevision) > 0 || compareDecimal(createdAt, updatedAt) > 0)
    return invalid("v2 operation ordering");
  const kindValid =
    (kind === "download" && slotId === null && modelId !== null) ||
    (kind === "load" && slotId !== null && modelId !== null && progress === null) ||
    (kind === "unload" && slotId !== null && modelId === null && progress === null);
  if (!kindValid || (status === "failed") !== (error !== null)) return invalid("v2 operation correlation");
  if (error) {
    if (error.code === "download_failed" && kind !== "download") return invalid("v2 operation error correlation");
    if (error.code === "load_failed" && kind !== "load") return invalid("v2 operation error correlation");
    if (error.code === "unload_failed" && kind !== "unload") return invalid("v2 operation error correlation");
  }
  return record as V2Operation;
}

function decodeV2CollectionHeader(record: Record<string, unknown>): {
  revision: DecimalString;
  generatedAt: DecimalString;
} {
  if (record.schema_version !== 2) return invalid("v2 schema version");
  decodeV2Uuid(record.epoch);
  const revision = decodeDecimalString(record.revision);
  const generatedAt = decodeDecimalString(record.generated_at_unix_ms);
  if (revision === "0") return invalid("v2 collection revision");
  return { revision, generatedAt };
}

function validateOperationOrdering(operations: readonly V2Operation[], contract: string): void {
  for (let index = 1; index < operations.length; index += 1) {
    const previous = operations[index - 1];
    const current = operations[index];
    if (!previous || !current) return invalid(contract);
    const revisionOrder = compareDecimal(previous.created_revision, current.created_revision);
    if (revisionOrder > 0 || (revisionOrder === 0 && previous.operation_id >= current.operation_id))
      return invalid(contract);
  }
}

function validateOperationSnapshotBounds(
  operation: V2Operation,
  revision: DecimalString,
  generatedAt: DecimalString,
  contract: string,
): void {
  if (
    compareDecimal(operation.updated_revision, revision) > 0 ||
    compareDecimal(operation.updated_at_unix_ms, generatedAt) > 0
  )
    invalid(contract);
}

export function decodeV2NodeCollection(value: unknown): V2NodeCollection {
  const record = exactRecord(
    value,
    ["schema_version", "epoch", "revision", "generated_at_unix_ms", "nodes"],
    "v2 node collection",
  );
  decodeV2CollectionHeader(record);
  if (!Array.isArray(record.nodes) || record.nodes.length !== 1) return invalid("v2 node collection");
  decodeV2Node(record.nodes[0]);
  return record as V2NodeCollection;
}

export function decodeV2SlotCollection(value: unknown): V2SlotCollection {
  const record = exactRecord(
    value,
    ["schema_version", "epoch", "revision", "generated_at_unix_ms", "node_id", "slots"],
    "v2 slot collection",
  );
  decodeV2CollectionHeader(record);
  const nodeId = decodeV2Uuid(record.node_id);
  if (!Array.isArray(record.slots) || record.slots.length !== 1) return invalid("v2 slot collection");
  const slot = decodeV2Slot(record.slots[0]);
  if (slot.node_id !== nodeId) return invalid("v2 slot collection correlation");
  return record as V2SlotCollection;
}

export function decodeV2OperationCollection(value: unknown): V2OperationCollection {
  const record = exactRecord(
    value,
    ["schema_version", "epoch", "revision", "generated_at_unix_ms", "operations"],
    "v2 operation collection",
  );
  const { revision, generatedAt } = decodeV2CollectionHeader(record);
  if (!Array.isArray(record.operations) || record.operations.length > MAX_V2_OPERATIONS)
    return invalid("v2 operation collection");
  const operations = record.operations.map(decodeV2Operation);
  const ids = new Set<string>();
  const nodeId = operations[0]?.node_id;
  for (const operation of operations) {
    if (ids.has(operation.operation_id) || (nodeId !== undefined && operation.node_id !== nodeId))
      return invalid("v2 operation collection correlation");
    ids.add(operation.operation_id);
    validateOperationSnapshotBounds(operation, revision, generatedAt, "v2 operation collection bounds");
  }
  validateOperationOrdering(operations, "v2 operation collection ordering");
  return record as V2OperationCollection;
}

export function decodeV2OperationAccepted(value: unknown): V2OperationAccepted {
  const record = exactRecord(value, ["epoch", "operation_id", "revision"], "v2 operation acceptance");
  decodeV2Uuid(record.epoch);
  decodeV2Uuid(record.operation_id);
  const revision = decodeDecimalString(record.revision);
  if (revision === "0") return invalid("v2 operation acceptance");
  return record as V2OperationAccepted;
}

export function decodeV2OperationEnvelope(value: unknown): V2OperationEnvelope {
  const record = exactRecord(
    value,
    ["schema_version", "epoch", "revision", "generated_at_unix_ms", "operation"],
    "v2 operation envelope",
  );
  const { revision, generatedAt } = decodeV2CollectionHeader(record);
  const operation = decodeV2Operation(record.operation);
  validateOperationSnapshotBounds(operation, revision, generatedAt, "v2 operation envelope correlation");
  return record as V2OperationEnvelope;
}

const controlErrorCodes = [
  "invalid_request",
  "node_not_found",
  "slot_not_found",
  "operation_not_found",
  "unknown_model",
  "operation_conflict",
  "operation_terminal",
  "cancellation_not_safe",
  "model_unavailable",
  "unsupported_media_type",
  "node_stopping",
  "state_writer_overloaded",
  "durable_state_unavailable",
] as const;

export function decodeV2ControlError(value: unknown): V2ControlErrorBody {
  return decodeV2PublicError(value, controlErrorCodes, "v2 control error") as V2ControlErrorBody;
}

export function decodeV2ControlEvent(value: unknown): V2ControlEvent {
  const record = exactRecord(
    value,
    [
      "schema_version",
      "event_id",
      "epoch",
      "sequence",
      "revision",
      "committed_at_unix_ms",
      "entity",
      "entity_id",
      "node_id",
      "node_instance_id",
      "slot_id",
      "operation_id",
      "node",
      "slot",
      "operation",
    ],
    "v2 control event",
  );
  if (record.schema_version !== 2) return invalid("v2 control event");
  decodeV2Uuid(record.event_id);
  decodeV2Uuid(record.epoch);
  const sequence = decodeDecimalString(record.sequence);
  const revision = decodeDecimalString(record.revision);
  const committedAt = decodeDecimalString(record.committed_at_unix_ms);
  if (sequence === "0" || revision === "0" || compareDecimal(sequence, revision) > 0)
    return invalid("v2 control event position");
  const entity = oneOf(record.entity, ["node", "slot", "operation"] as const, "v2 event entity");
  const entityId = decodeV2Uuid(record.entity_id);
  const nodeId = decodeV2Uuid(record.node_id);
  const nodeInstanceId = record.node_instance_id === null ? null : decodeV2Uuid(record.node_instance_id);
  const slotId = record.slot_id === null ? null : decodeV2Uuid(record.slot_id);
  const operationId = record.operation_id === null ? null : decodeV2Uuid(record.operation_id);
  const node = record.node === null ? null : decodeV2Node(record.node);
  const slot = record.slot === null ? null : decodeV2Slot(record.slot);
  const operation = record.operation === null ? null : decodeV2Operation(record.operation);
  if (node === null && slot === null && operation === null) return invalid("v2 control event record");

  const expectedSlotId = slot?.slot_id ?? operation?.slot_id ?? null;
  const expectedOperationId = operation?.operation_id ?? slot?.operation_id ?? null;
  if (
    (node !== null && (node.node_id !== nodeId || node.node_instance_id !== nodeInstanceId)) ||
    (slot !== null && (slot.node_id !== nodeId || slot.slot_id !== slotId)) ||
    (operation !== null &&
      (operation.node_id !== nodeId ||
        operation.operation_id !== operationId ||
        operation.updated_revision !== revision ||
        compareDecimal(operation.updated_at_unix_ms, committedAt) > 0 ||
        operation.slot_id !== slotId)) ||
    slotId !== expectedSlotId ||
    operationId !== expectedOperationId ||
    (operation !== null && nodeInstanceId === null) ||
    (slot?.operation_id !== null && slot?.operation_id !== undefined && slot.operation_id !== operationId) ||
    (operation !== null && operation.slot_id !== null && slot !== null && operation.slot_id !== slot.slot_id)
  )
    return invalid("v2 control event correlation");

  const entityMatches =
    (entity === "node" && entityId === nodeId && node !== null && slot === null && operation === null) ||
    (entity === "slot" && entityId === slotId && slot !== null && operation === null) ||
    (entity === "operation" && entityId === operationId && operation !== null);
  if (!entityMatches) return invalid("v2 control event entity correlation");
  return record as V2ControlEvent;
}

function isActiveLifecycle(operation: V2Operation): boolean {
  return (
    (operation.kind === "load" || operation.kind === "unload") &&
    (operation.status === "queued" || operation.status === "running" || operation.status === "cancelling")
  );
}

export function decodeV2ReconnectSnapshot(value: unknown): V2ReconnectSnapshot {
  const record = exactRecord(
    value,
    ["schema_version", "epoch", "revision", "generated_at_unix_ms", "stream", "nodes", "slots", "operations", "events"],
    "v2 reconnect snapshot",
  );
  const { revision: headerRevision, generatedAt } = decodeV2CollectionHeader(record);
  const epoch = decodeV2Uuid(record.epoch);
  const revision = decodeDecimalString(record.revision);
  const stream = exactRecord(record.stream, ["epoch", "cursor", "cursor_gap"], "v2 stream position");
  const streamEpoch = decodeV2Uuid(stream.epoch);
  const cursor = decodeDecimalString(stream.cursor);
  if (typeof stream.cursor_gap !== "boolean") return invalid("v2 stream position");
  if (
    streamEpoch !== epoch ||
    revision === "0" ||
    cursor === "0" ||
    compareDecimal(cursor, revision) > 0 ||
    !Array.isArray(record.nodes) ||
    record.nodes.length !== 1 ||
    !Array.isArray(record.slots) ||
    record.slots.length !== 1 ||
    !Array.isArray(record.operations) ||
    record.operations.length > MAX_V2_OPERATIONS ||
    !Array.isArray(record.events) ||
    record.events.length > MAX_V2_EVENTS
  )
    return invalid("v2 reconnect snapshot");
  const node = decodeV2Node(record.nodes[0]);
  const slot = decodeV2Slot(record.slots[0]);
  if (slot.node_id !== node.node_id) return invalid("v2 reconnect slot correlation");
  const operations = record.operations.map(decodeV2Operation);
  validateOperationOrdering(operations, "v2 reconnect operation ordering");
  const operationIds = new Set<string>();
  let activeLifecycle: V2Operation | undefined;
  let observedLifecycle: V2Operation | undefined;
  for (const operation of operations) {
    if (operation.node_id !== node.node_id || operationIds.has(operation.operation_id))
      return invalid("v2 reconnect operation correlation");
    operationIds.add(operation.operation_id);
    validateOperationSnapshotBounds(operation, headerRevision, generatedAt, "v2 reconnect operation bounds");
    if (isActiveLifecycle(operation)) {
      if (activeLifecycle) return invalid("v2 reconnect lifecycle correlation");
      activeLifecycle = operation;
      if (operation.status === "running" || operation.status === "cancelling") observedLifecycle = operation;
    }
  }
  const slotMatches =
    (slot.status === "loading" &&
      observedLifecycle?.kind === "load" &&
      observedLifecycle.slot_id === slot.slot_id &&
      slot.operation_id === observedLifecycle.operation_id) ||
    (slot.status === "unloading" &&
      observedLifecycle?.kind === "unload" &&
      observedLifecycle.slot_id === slot.slot_id &&
      slot.operation_id === observedLifecycle.operation_id) ||
    ((slot.status === "unloaded" || slot.status === "ready" || slot.status === "recovery") &&
      observedLifecycle === undefined);
  if (!slotMatches) return invalid("v2 reconnect slot operation correlation");

  const events = record.events.map(decodeV2ControlEvent);
  if (stream.cursor_gap && events.length > 0) return invalid("v2 reconnect gap");
  const eventIds = new Set<string>();
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index];
    if (!event) return invalid("v2 reconnect event correlation");
    if (
      event.epoch !== epoch ||
      event.node_id !== node.node_id ||
      compareDecimal(event.sequence, cursor) > 0 ||
      compareDecimal(event.revision, revision) > 0 ||
      compareDecimal(event.committed_at_unix_ms, generatedAt) > 0 ||
      eventIds.has(event.event_id)
    )
      return invalid("v2 reconnect event correlation");
    eventIds.add(event.event_id);
    const previous = events[index - 1];
    if (
      previous &&
      (BigInt(previous.sequence) + 1n !== BigInt(event.sequence) ||
        BigInt(previous.revision) + 1n !== BigInt(event.revision) ||
        compareDecimal(previous.committed_at_unix_ms, event.committed_at_unix_ms) > 0)
    )
      return invalid("v2 reconnect event ordering");
  }
  const tail = events[events.length - 1];
  if (!stream.cursor_gap && tail && (tail.sequence !== cursor || tail.revision !== revision))
    return invalid("v2 reconnect event tail");
  return record as V2ReconnectSnapshot;
}

export function decodeV2NodeCollectionJson(value: RawJson): V2NodeCollection {
  return decodeV2NodeCollection(decodeRawJson(value, MAX_V2_SNAPSHOT_BYTES, "v2 node collection JSON"));
}

export function decodeV2SlotCollectionJson(value: RawJson): V2SlotCollection {
  return decodeV2SlotCollection(decodeRawJson(value, MAX_V2_SNAPSHOT_BYTES, "v2 slot collection JSON"));
}

export function decodeV2OperationCollectionJson(value: RawJson): V2OperationCollection {
  return decodeV2OperationCollection(decodeRawJson(value, MAX_V2_SNAPSHOT_BYTES, "v2 operation collection JSON"));
}

export function decodeV2OperationEnvelopeJson(value: RawJson): V2OperationEnvelope {
  return decodeV2OperationEnvelope(decodeRawJson(value, MAX_V2_SNAPSHOT_BYTES, "v2 operation envelope JSON"));
}

export function decodeV2ReconnectSnapshotJson(value: RawJson): V2ReconnectSnapshot {
  return decodeV2ReconnectSnapshot(decodeRawJson(value, MAX_V2_SNAPSHOT_BYTES, "v2 reconnect snapshot JSON"));
}

export function decodeV2ControlEventJson(value: RawJson): V2ControlEvent {
  return decodeV2ControlEvent(decodeRawJson(value, MAX_V2_EVENT_BYTES, "v2 control event JSON"));
}

export function decodeV2OperationAcceptedJson(value: RawJson): V2OperationAccepted {
  return decodeV2OperationAccepted(decodeRawJson(value, MAX_V2_EVENT_BYTES, "v2 operation acceptance JSON"));
}

export function decodeV2ControlErrorJson(value: RawJson): V2ControlErrorBody {
  return decodeV2ControlError(decodeRawJson(value, MAX_V2_EVENT_BYTES, "v2 control error JSON"));
}
