export const v2Ids = {
  node: "123e4567-e89b-42d3-a456-426614174000",
  otherNode: "123e4567-e89b-42d3-a456-426614174010",
  instance: "123e4567-e89b-42d3-b456-426614174001",
  slot: "123e4567-e89b-42d3-8456-426614174002",
  operation: "123e4567-e89b-42d3-9456-426614174003",
  event: "123e4567-e89b-42d3-a456-426614174004",
  nextEvent: "123e4567-e89b-42d3-a456-426614174006",
  epoch: "123e4567-e89b-42d3-b456-426614174005",
  oldEpoch: "123e4567-e89b-42d3-b456-426614174007",
} as const;

export const validV2Node = {
  node_id: v2Ids.node,
  node_instance_id: v2Ids.instance,
  control_endpoint: "http://127.0.0.1:19431",
  status: "running",
  slot_capacity: 1,
  capabilities: {
    model_download: true,
    slot_load: true,
    slot_unload: true,
    operation_cancel: true,
    operation_stream: true,
  },
} as const;

export const validV2Slot = {
  slot_id: v2Ids.slot,
  node_id: v2Ids.node,
  name: "default",
  status: "loading",
  model_id: null,
  operation_id: v2Ids.operation,
  error: null,
} as const;

export const validV2Operation = {
  operation_id: v2Ids.operation,
  node_id: v2Ids.node,
  kind: "load",
  status: "running",
  slot_id: v2Ids.slot,
  model_id: "gemma-3-4b-it-q4",
  progress: null,
  error: null,
  created_revision: "10",
  updated_revision: "11",
  created_at_unix_ms: "1784246400000",
  updated_at_unix_ms: "1784246400500",
} as const;

export const validV2Event = {
  schema_version: 2,
  event_id: v2Ids.event,
  epoch: v2Ids.epoch,
  sequence: "11",
  revision: "11",
  committed_at_unix_ms: "1784246400500",
  entity: "operation",
  entity_id: v2Ids.operation,
  node_id: v2Ids.node,
  node_instance_id: v2Ids.instance,
  slot_id: v2Ids.slot,
  operation_id: v2Ids.operation,
  node: null,
  slot: validV2Slot,
  operation: validV2Operation,
} as const;

export const validV2NodeCollection = {
  schema_version: 2,
  epoch: v2Ids.epoch,
  revision: "11",
  generated_at_unix_ms: "1784246400600",
  nodes: [validV2Node],
} as const;

export const validV2SlotCollection = {
  schema_version: 2,
  epoch: v2Ids.epoch,
  revision: "11",
  generated_at_unix_ms: "1784246400600",
  node_id: v2Ids.node,
  slots: [validV2Slot],
} as const;

export const validV2OperationCollection = {
  schema_version: 2,
  epoch: v2Ids.epoch,
  revision: "11",
  generated_at_unix_ms: "1784246400600",
  operations: [validV2Operation],
} as const;

export const validV2ReconnectSnapshot = {
  schema_version: 2,
  epoch: v2Ids.epoch,
  revision: "11",
  generated_at_unix_ms: "1784246400600",
  stream: { epoch: v2Ids.epoch, cursor: "11", cursor_gap: false },
  nodes: [validV2Node],
  slots: [validV2Slot],
  operations: [validV2Operation],
  events: [],
} as const;

export const validV2OperationAccepted = {
  epoch: v2Ids.epoch,
  operation_id: v2Ids.operation,
  revision: "11",
} as const;

export const validV2OperationEnvelope = {
  schema_version: 2,
  epoch: v2Ids.epoch,
  revision: "11",
  generated_at_unix_ms: "1784246400600",
  operation: validV2Operation,
} as const;

export const validV2ControlError = {
  code: "operation_conflict",
  message: "A conflicting operation is active.",
} as const;

function indexedUuid(index: number): string {
  return `123e4567-e89b-42d3-9456-${(index + 1_000).toString(16).padStart(12, "0")}`;
}

export function v2DownloadOperation(index: number) {
  const revision = String(index + 1);
  return {
    ...validV2Operation,
    operation_id: indexedUuid(index),
    kind: "download",
    status: "succeeded",
    slot_id: null,
    model_id: `model-${index}`,
    created_revision: revision,
    updated_revision: revision,
    created_at_unix_ms: "1",
    updated_at_unix_ms: "1",
  } as const;
}

export function v2Event(index: number) {
  const position = String(index + 1);
  return {
    ...validV2Event,
    event_id: indexedUuid(index),
    sequence: position,
    revision: position,
    committed_at_unix_ms: position,
    operation: {
      ...validV2Operation,
      created_revision: "1",
      updated_revision: position,
      created_at_unix_ms: "1",
      updated_at_unix_ms: position,
    },
  } as const;
}

export const nextV2Event = {
  ...validV2Event,
  event_id: v2Ids.nextEvent,
  sequence: "12",
  revision: "12",
  committed_at_unix_ms: "1784246400601",
  operation: {
    ...validV2Operation,
    updated_revision: "12",
    updated_at_unix_ms: "1784246400601",
  },
} as const;

function hexBytes(value: string): Uint8Array {
  return Uint8Array.from(value.match(/../g) ?? [], (byte) => Number.parseInt(byte, 16));
}

function lengthPrefix(value: string): Uint8Array {
  const bytes = new TextEncoder().encode(value);
  const prefixed = new Uint8Array(4 + bytes.byteLength);
  new DataView(prefixed.buffer).setUint32(0, bytes.byteLength, false);
  prefixed.set(bytes, 4);
  return prefixed;
}

export async function v1IdentityProof(
  token: string,
  nonce: string,
  nodeId: string = v2Ids.node,
  instanceId: string = v2Ids.instance,
): Promise<string> {
  const domain = new TextEncoder().encode("loxa-control-node-identity-v1\0");
  const protocol = new Uint8Array([0, 0, 0, 1]);
  const node = lengthPrefix(nodeId);
  const instance = lengthPrefix(instanceId);
  const message = new Uint8Array(domain.length + protocol.length + 32 + node.length + instance.length + 1);
  let offset = 0;
  for (const part of [domain, protocol, hexBytes(nonce), node, instance, Uint8Array.of(0)]) {
    message.set(part, offset);
    offset += part.length;
  }
  const key = await crypto.subtle.importKey(
    "raw",
    Uint8Array.from(hexBytes(token)).buffer,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, Uint8Array.from(message).buffer));
  return [...signature].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
