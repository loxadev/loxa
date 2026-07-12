import { describe, expect, it } from "vitest";

import {
  ControlContractError,
  decodeCapabilities,
  decodeControlEvent,
  decodeInventory,
  decodeNodeSnapshot,
  decodeOperation,
  decodeReconnectSnapshot,
} from "./contracts";

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

const inventory = [{
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
}];

describe("control contracts", () => {
  it("decodes the closed node and capability snapshots", () => {
    expect(decodeNodeSnapshot({
      status: "unloaded",
      active_model_id: null,
      operation_id: null,
      error: null,
    })).toEqual({
      status: "unloaded",
      activeModelId: null,
      operationId: null,
      error: null,
    });
    expect(decodeCapabilities({
      document_input: false,
      document_input_reason: "Document input is not supported.",
      text_chat: true,
    })).toEqual({
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
    expect(decodeReconnectSnapshot({
      cursor: 9,
      cursor_gap: false,
      operations: [operation],
      events: [{ sequence: 9, operation }],
    })).toMatchObject({ cursor: 9, cursorGap: false });
  });

  it.each([
    ["extra node field", () => decodeNodeSnapshot({ status: "unloaded", active_model_id: null, operation_id: null, error: null, extra: true })],
    ["contradictory operation", () => decodeOperation({ ...operation, status: "succeeded", error: "failed" })],
    ["unsafe byte count", () => decodeInventory([{ ...inventory[0], size_bytes: Number.MAX_SAFE_INTEGER + 1 }])],
    ["unknown artifact state", () => decodeInventory([{ ...inventory[0], artifact: "verifying" }])],
    ["event beyond cursor", () => decodeReconnectSnapshot({ cursor: 8, cursor_gap: false, operations: [operation], events: [{ sequence: 9, operation }] })],
    ["missing unsupported document reason", () => decodeCapabilities({ document_input: false, document_input_reason: "", text_chat: true })],
  ])("rejects %s", (_name, decode) => {
    expect(decode).toThrow(ControlContractError);
  });
});
