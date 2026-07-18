import { describe, expect, it } from "vitest";

import {
  ControlContractError,
  decodeCapabilities,
  decodeControlEvent,
  decodeInventory,
  decodeNodeIdentityProof,
  decodeNodeSnapshot,
  decodeOperation,
  decodeReconnectSnapshot,
  decodeDecimalString,
  decodeV2ControlEvent,
  decodeV2ControlEventJson,
  decodeV2ControlError,
  decodeV2ControlErrorJson,
  decodeV2NodeCollection,
  decodeV2NodeCollectionJson,
  decodeV2OperationAccepted,
  decodeV2OperationAcceptedJson,
  decodeV2OperationCollection,
  decodeV2OperationEnvelope,
  decodeV2OperationEnvelopeJson,
  decodeV2ReconnectSnapshot,
  decodeV2ReconnectSnapshotJson,
  decodeV2SlotCollection,
} from "./contracts";
import {
  v2Ids,
  v2DownloadOperation,
  v2Event,
  validV2Event,
  validV2ControlError,
  validV2NodeCollection,
  validV2Operation,
  validV2OperationAccepted,
  validV2OperationCollection,
  validV2OperationEnvelope,
  validV2ReconnectSnapshot,
  validV2Slot,
  validV2SlotCollection,
} from "./testSupport";

const operation = {
  id: "op-7",
  kind: "download",
  status: "running",
  model_id: "gemma-3-4b-it-q4",
  progress: { completed_bytes: 512, total_bytes: 1024 },
  error: null,
  created_at_unix_ms: 1_700_000_000_000,
  updated_at_unix_ms: 1_700_000_000_100,
};

function padJsonToBytes(json: string, byteLength: number): string {
  const current = new TextEncoder().encode(json).byteLength;
  if (current > byteLength) throw new Error("fixture already exceeds requested byte length");
  return `${json}${" ".repeat(byteLength - current)}`;
}

const inventory = [
  {
    id: "gemma-3-4b-it-q4",
    repo: "publisher/model",
    revision: "0123456789abcdef",
    filename: "model.gguf",
    sha256: "ab".repeat(32),
    size_bytes: 1024,
    license: "Apache-2.0",
    params: "4B",
    quant: "Q4_K_M",
    min_free_mem_gb: 6,
    artifact: { partial: { bytes: 512 } },
    compatibility: { compatible: true, reason: "memory fits" },
    engine: { engine: "llama-cpp", eligible: true, reason: "verified recipe" },
  },
];

describe("control contracts", () => {
  it("keeps the closed v1 decoder surface additive beside strict v2 collections", () => {
    const v1 = decodeNodeSnapshot({
      status: "unloaded",
      active_model_id: null,
      operation_id: null,
      error: null,
    });
    const v2 = decodeV2NodeCollection(validV2NodeCollection);

    expect(v1).toEqual({ status: "unloaded", activeModelId: null, operationId: null, error: null });
    expect(v2.nodes[0]).toMatchObject({
      node_id: v2Ids.node,
      node_instance_id: v2Ids.instance,
      slot_capacity: 1,
    });
  });

  it("accepts canonical UUID-shaped and older opaque v1 proof identities", () => {
    const proof = (nodeId: string, runtimeIdentity: string) => ({
      protocol_version: 1,
      node_id: nodeId,
      runtime_identity: runtimeIdentity,
      status: "unloaded",
      challenge_proof: "ab".repeat(32),
    });

    expect(
      decodeNodeIdentityProof(proof("550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001")),
    ).toMatchObject({
      nodeId: "550e8400-e29b-41d4-a716-446655440000",
      runtimeIdentity: "550e8400-e29b-41d4-a716-446655440001",
    });
    expect(decodeNodeIdentityProof(proof("older-node", "pid-shaped-runtime"))).toMatchObject({
      nodeId: "older-node",
      runtimeIdentity: "pid-shaped-runtime",
    });

    expect(() => decodeNodeIdentityProof(proof("n".repeat(1025), "runtime"))).toThrow(ControlContractError);
    expect(() => decodeNodeIdentityProof(proof("node", "r".repeat(1025)))).toThrow(ControlContractError);
    expect(() => decodeNodeIdentityProof({ ...proof("node", "runtime"), extra: true })).toThrow(ControlContractError);
    const missing = proof("node", "runtime") as Record<string, unknown>;
    delete missing.runtime_identity;
    expect(() => decodeNodeIdentityProof(missing)).toThrow(ControlContractError);
  });

  it("decodes the closed node and capability snapshots", () => {
    expect(
      decodeNodeSnapshot({
        status: "unloaded",
        active_model_id: null,
        operation_id: null,
        error: null,
      }),
    ).toEqual({
      status: "unloaded",
      activeModelId: null,
      operationId: null,
      error: null,
    });
    expect(
      decodeCapabilities({
        document_input: false,
        document_input_reason: "Document input is not supported.",
        text_chat: true,
      }),
    ).toEqual({
      documentInput: false,
      documentInputReason: "Document input is not supported.",
      textChat: true,
    });
  });

  it("decodes all authoritative artifact states without promotion", () => {
    const states = [
      ["not_downloaded", { kind: "not_downloaded" }],
      [{ partial: { bytes: 512 } }, { kind: "partial", bytes: 512 }],
      ["downloaded", { kind: "downloaded" }],
      [{ invalid: { reason: "verification_required" } }, { kind: "invalid", reason: "verification_required" }],
    ] as const;

    for (const [wire, expected] of states) {
      expect(decodeInventory([{ ...inventory[0], artifact: wire }])[0].artifact).toEqual(expected);
    }
  });

  it("decodes operation snapshots and monotonic events", () => {
    expect(decodeOperation(operation)).toMatchObject({
      id: "op-7",
      kind: "download",
      status: "running",
      modelId: "gemma-3-4b-it-q4",
      progress: { completedBytes: 512, totalBytes: 1024 },
    });
    expect(decodeControlEvent({ sequence: 9, operation })).toMatchObject({
      sequence: 9,
      operation: { id: "op-7" },
    });
    expect(
      decodeReconnectSnapshot({
        cursor: 9,
        cursor_gap: false,
        operations: [operation],
        events: [{ sequence: 9, operation }],
      }),
    ).toMatchObject({ cursor: 9, cursorGap: false });
  });

  it.each([
    [
      "extra node field",
      () =>
        decodeNodeSnapshot({ status: "unloaded", active_model_id: null, operation_id: null, error: null, extra: true }),
    ],
    ["contradictory operation", () => decodeOperation({ ...operation, status: "succeeded", error: "failed" })],
    ["unsafe byte count", () => decodeInventory([{ ...inventory[0], size_bytes: Number.MAX_SAFE_INTEGER + 1 }])],
    ["unknown artifact state", () => decodeInventory([{ ...inventory[0], artifact: "verifying" }])],
    [
      "event beyond cursor",
      () =>
        decodeReconnectSnapshot({
          cursor: 8,
          cursor_gap: false,
          operations: [operation],
          events: [{ sequence: 9, operation }],
        }),
    ],
    [
      "missing unsupported document reason",
      () => decodeCapabilities({ document_input: false, document_input_reason: "", text_chat: true }),
    ],
  ])("rejects %s", (_name, decode) => {
    expect(decode).toThrow(ControlContractError);
  });
});

describe("strict v2 control contracts", () => {
  it("accepts canonical decimal strings and rejects numeric, noncanonical, and overflowing values", () => {
    expect(decodeDecimalString("0")).toBe("0");
    expect(decodeDecimalString("18446744073709551615")).toBe("18446744073709551615");
    for (const value of [10, "", "00", "01", "+1", "-1", "18446744073709551616"]) {
      expect(() => decodeDecimalString(value)).toThrow(ControlContractError);
    }
    expect(decodeV2OperationAccepted(validV2OperationAccepted)).toEqual(validV2OperationAccepted);
    expect(() => decodeV2OperationAccepted({ ...validV2OperationAccepted, revision: 10 })).toThrow(
      ControlContractError,
    );
    expect(() => decodeV2OperationAccepted({ ...validV2OperationAccepted, extra: true })).toThrow(ControlContractError);
  });

  it("decodes exact operation envelopes and closed stable control errors", () => {
    expect(decodeV2OperationEnvelope(validV2OperationEnvelope)).toEqual(validV2OperationEnvelope);
    expect(decodeV2ControlError(validV2ControlError)).toEqual(validV2ControlError);
    expect(() => decodeV2OperationEnvelope({ ...validV2OperationEnvelope, revision: 11 })).toThrow(
      ControlContractError,
    );
    expect(() => decodeV2ControlError({ ...validV2ControlError, code: "surprise" })).toThrow(ControlContractError);
    expect(() => decodeV2ControlError({ ...validV2ControlError, extra: true })).toThrow(ControlContractError);
  });

  it("requires canonical lowercase UUIDv4 identifiers", () => {
    expect(() =>
      decodeV2OperationAccepted({ ...validV2OperationAccepted, operation_id: v2Ids.operation.toUpperCase() }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationAccepted({
        ...validV2OperationAccepted,
        operation_id: "123e4567-e89b-12d3-9456-426614174003",
      }),
    ).toThrow(ControlContractError);
  });

  it("decodes exact capacity-one node, slot, and operation collections", () => {
    expect(decodeV2NodeCollection(validV2NodeCollection)).toEqual(validV2NodeCollection);
    expect(decodeV2SlotCollection(validV2SlotCollection)).toEqual(validV2SlotCollection);
    expect(decodeV2OperationCollection(validV2OperationCollection)).toEqual(validV2OperationCollection);
    expect(() => decodeV2NodeCollection({ ...validV2NodeCollection, nodes: [] })).toThrow(ControlContractError);
    expect(() =>
      decodeV2SlotCollection({
        ...validV2SlotCollection,
        slots: [{ ...validV2Slot, node_id: v2Ids.otherNode }],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationCollection({
        ...validV2OperationCollection,
        operations: [validV2Operation, validV2Operation],
      }),
    ).toThrow(ControlContractError);
  });

  it("rejects closed-enum, nested exact-key, model, and nullable-correlation violations", () => {
    expect(() =>
      decodeV2NodeCollection({
        ...validV2NodeCollection,
        nodes: [{ ...validV2NodeCollection.nodes[0], status: "starting" }],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationCollection({
        ...validV2OperationCollection,
        operations: [{ ...validV2Operation, model_id: "x".repeat(257) }],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2SlotCollection({
        ...validV2SlotCollection,
        slots: [{ ...validV2Slot, operation_id: null }],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationCollection({
        ...validV2OperationCollection,
        operations: [{ ...validV2Operation, extra: true }],
      }),
    ).toThrow(ControlContractError);
  });

  it("decodes full events and rejects malformed authoritative correlation", () => {
    expect(decodeV2ControlEvent(validV2Event)).toEqual(validV2Event);
    expect(() => decodeV2ControlEvent({ ...validV2Event, node_id: v2Ids.otherNode })).toThrow(ControlContractError);
    expect(() =>
      decodeV2ControlEvent({
        ...validV2Event,
        operation: { ...validV2Operation, model_id: "x".repeat(257) },
      }),
    ).toThrow(ControlContractError);
    expect(() => decodeV2ControlEvent({ ...validV2Event, operation_id: null })).toThrow(ControlContractError);
  });

  it("decodes reconnect snapshots and rejects gaps, epochs, ordering, and exact size overflow", () => {
    expect(decodeV2ReconnectSnapshot(validV2ReconnectSnapshot)).toEqual(validV2ReconnectSnapshot);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        stream: { ...validV2ReconnectSnapshot.stream, epoch: v2Ids.event },
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        stream: { ...validV2ReconnectSnapshot.stream, cursor_gap: true },
        events: [validV2Event],
      }),
    ).toThrow(ControlContractError);
  });

  it("enforces raw UTF-8 byte limits and duplicate keys before JSON object construction", () => {
    const snapshotJson = JSON.stringify(validV2ReconnectSnapshot);
    const eventJson = JSON.stringify(validV2Event);
    expect(decodeV2ReconnectSnapshotJson(snapshotJson)).toEqual(validV2ReconnectSnapshot);
    expect(decodeV2ControlEventJson(new TextEncoder().encode(eventJson))).toEqual(validV2Event);
    expect(() => decodeV2ReconnectSnapshotJson(`${snapshotJson}${" ".repeat(2 * 1024 * 1024)}`)).toThrow(
      ControlContractError,
    );
    expect(() => decodeV2ControlEventJson(`${eventJson}${" ".repeat(16 * 1024)}`)).toThrow(ControlContractError);
    expect(() =>
      decodeV2NodeCollectionJson(
        JSON.stringify(validV2NodeCollection).replace('"schema_version":2,', '"schema_version":2,"schema_version":2,'),
      ),
    ).toThrow(ControlContractError);
    expect(() => decodeV2ControlEventJson(eventJson.replace('"epoch":', '"ep\\u006fch":"duplicate","epoch":'))).toThrow(
      ControlContractError,
    );
  });

  it("rejects malformed Unicode scalar values and invalid UTF-8 before JSON decoding", () => {
    const errorJson = JSON.stringify(validV2ControlError);
    for (const malformed of [String.raw`\ud800`, String.raw`\udfff`, String.raw`\ud800\u0041`]) {
      expect(() =>
        decodeV2ControlErrorJson(errorJson.replace("A conflicting operation is active.", malformed)),
      ).toThrow(ControlContractError);
    }
    for (const malformed of ["\ud800", "\udfff"]) {
      expect(() =>
        decodeV2ControlErrorJson(errorJson.replace("A conflicting operation is active.", malformed)),
      ).toThrow(ControlContractError);
    }
    expect(() => decodeV2ControlErrorJson(new Uint8Array([0x7b, 0x22, 0xc3, 0x28, 0x22, 0x3a, 0x31, 0x7d]))).toThrow(
      ControlContractError,
    );
    expect(
      decodeV2ControlErrorJson(errorJson.replace("A conflicting operation is active.", String.raw`\ud83d\ude80`)),
    ).toMatchObject({ message: "🚀" });
  });

  it("rejects duplicate keys in raw operation-acceptance and control-error responses", () => {
    const acceptedJson = JSON.stringify(validV2OperationAccepted);
    const errorJson = JSON.stringify(validV2ControlError);
    expect(decodeV2OperationAcceptedJson(acceptedJson)).toEqual(validV2OperationAccepted);
    expect(decodeV2ControlErrorJson(errorJson)).toEqual(validV2ControlError);
    expect(() =>
      decodeV2OperationAcceptedJson(acceptedJson.replace('"revision":', '"revision":"11","revision":')),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ControlErrorJson(errorJson.replace('"message":', '"m\\u0065ssage":"duplicate","message":')),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationEnvelopeJson(
        JSON.stringify(validV2OperationEnvelope).replace(
          '"operation_id":',
          '"operation_id":"duplicate","operation_id":',
        ),
      ),
    ).toThrow(ControlContractError);
  });

  it("accepts exact raw byte limits and rejects one byte beyond them", () => {
    const exactSnapshot = padJsonToBytes(JSON.stringify(validV2ReconnectSnapshot), 2 * 1024 * 1024);
    const exactEvent = padJsonToBytes(JSON.stringify(validV2Event), 16 * 1024);
    expect(decodeV2ReconnectSnapshotJson(exactSnapshot)).toEqual(validV2ReconnectSnapshot);
    expect(decodeV2ControlEventJson(exactEvent)).toEqual(validV2Event);
    expect(() => decodeV2ReconnectSnapshotJson(`${exactSnapshot} `)).toThrow(ControlContractError);
    expect(() => decodeV2ControlEventJson(`${exactEvent} `)).toThrow(ControlContractError);
  });

  it("requires nonzero acceptance revisions and nondecreasing retained-event commit times", () => {
    expect(() => decodeV2OperationAccepted({ ...validV2OperationAccepted, revision: "0" })).toThrow(
      ControlContractError,
    );
    const unloadedSlot = { ...validV2Slot, status: "unloaded", model_id: null, operation_id: null } as const;
    const first = { ...v2Event(0), committed_at_unix_ms: "2" } as const;
    const second = { ...v2Event(1), committed_at_unix_ms: "2" } as const;
    const snapshot = {
      ...validV2ReconnectSnapshot,
      revision: "2",
      stream: { ...validV2ReconnectSnapshot.stream, cursor: "2" },
      slots: [unloadedSlot],
      operations: [],
      events: [first, second],
    } as const;
    expect(decodeV2ReconnectSnapshot(snapshot)).toEqual(snapshot);
    const earlierSecond = {
      ...second,
      committed_at_unix_ms: "1",
      operation: { ...second.operation, updated_at_unix_ms: "1" },
    } as const;
    expect(decodeV2ControlEvent(earlierSecond)).toEqual(earlierSecond);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...snapshot,
        events: [first, earlierSecond],
      }),
    ).toThrow(ControlContractError);
  });

  it("enforces collection and operation-envelope commit-time bounds", () => {
    expect(() => decodeV2NodeCollection({ ...validV2NodeCollection, revision: "0" })).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationEnvelope({
        ...validV2OperationEnvelope,
        operation: { ...validV2OperationEnvelope.operation, updated_revision: "12" },
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationCollection({
        ...validV2OperationCollection,
        revision: "10",
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationCollection({
        ...validV2OperationCollection,
        operations: [{ ...validV2Operation, updated_at_unix_ms: "1784246400601" }],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2OperationEnvelope({
        ...validV2OperationEnvelope,
        operation: { ...validV2OperationEnvelope.operation, updated_at_unix_ms: "1784246400601" },
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ControlEvent({
        ...validV2Event,
        operation: { ...validV2Operation, updated_at_unix_ms: "1784246400501" },
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        revision: "11",
        stream: { ...validV2ReconnectSnapshot.stream, cursor: "11" },
        events: [{ ...validV2Event, committed_at_unix_ms: "1784246400601" }],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        operations: [{ ...validV2Operation, updated_revision: "12" }],
      }),
    ).toThrow(ControlContractError);
  });

  it("separates queued desired lifecycle intent from observed transitional slot state", () => {
    const queuedLoad = { ...validV2Operation, status: "queued" } as const;
    const unloaded = {
      ...validV2Slot,
      status: "unloaded",
      operation_id: null,
    } as const;
    expect(
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        slots: [unloaded],
        operations: [queuedLoad],
      }),
    ).toBeDefined();

    const queuedUnload = {
      ...validV2Operation,
      kind: "unload",
      status: "queued",
      model_id: null,
    } as const;
    const ready = {
      ...validV2Slot,
      status: "ready",
      model_id: "gemma-3-4b-it-q4",
      operation_id: null,
    } as const;
    expect(
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        slots: [ready],
        operations: [queuedUnload],
      }),
    ).toBeDefined();
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        slots: [unloaded],
        operations: [validV2Operation],
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        slots: [validV2Slot],
        operations: [queuedLoad],
      }),
    ).toThrow(ControlContractError);
  });

  it("rejects operation and retained-event collections beyond their durable public bounds", () => {
    expect(() =>
      decodeV2OperationCollection({
        ...validV2OperationCollection,
        revision: "257",
        operations: Array.from({ length: 257 }, (_, index) => v2DownloadOperation(index)),
      }),
    ).toThrow(ControlContractError);
    expect(() =>
      decodeV2ReconnectSnapshot({
        ...validV2ReconnectSnapshot,
        revision: "1025",
        stream: { ...validV2ReconnectSnapshot.stream, cursor: "1025" },
        events: Array.from({ length: 1025 }, (_, index) => v2Event(index)),
      }),
    ).toThrow(ControlContractError);
  });
});
