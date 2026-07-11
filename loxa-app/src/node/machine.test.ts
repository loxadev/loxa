import { describe, expect, it } from "vitest";

import { actionGuards, initialNodeState, nodeReducer, type NodePhase } from "./machine";

const readyStatus = {
  node_id: "node-test",
  health: "ready",
  model: "loxa" as const,
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
};

const unavailableStatus = {
  ...readyStatus,
  health: "unavailable",
  engine: null,
  runtime_model: null,
  profile: null,
};

describe("nodeReducer", () => {
  it("covers every required presentation phase", () => {
    const disconnected = initialNodeState();
    const connecting = nodeReducer(disconnected, { type: "connect" });
    const starting = nodeReducer(disconnected, { type: "start" });
    const attached = nodeReducer(
      { ...connecting, ownership: "attached" },
      { type: "status", status: unavailableStatus },
    );
    const ready = nodeReducer(attached, { type: "status", status: readyStatus });
    const stopping = nodeReducer(
      { ...ready, ownership: "owned" },
      { type: "stop" },
    );
    const recovery = nodeReducer(stopping, {
      type: "recoveryRequired",
      message: "ownership could not be proven",
    });
    const error = nodeReducer(connecting, { type: "failure", message: "refused" });

    expect([
      disconnected.phase,
      connecting.phase,
      starting.phase,
      attached.phase,
      ready.phase,
      stopping.phase,
      recovery.phase,
      error.phase,
    ] satisfies NodePhase[]).toEqual([
      "disconnected",
      "connecting",
      "starting",
      "attached",
      "ready",
      "stopping",
      "recovery-required",
      "error",
    ]);
  });

  it("renders ready only from authoritative ready health", () => {
    const state = { ...initialNodeState(), phase: "connecting" as const, ownership: "attached" as const };
    expect(nodeReducer(state, { type: "status", status: unavailableStatus }).phase).toBe("attached");
    expect(nodeReducer(state, { type: "status", status: readyStatus }).phase).toBe("ready");
  });

  it("keeps ownership separate from health", () => {
    const ownedUnavailable = nodeReducer(
      { ...initialNodeState(), ownership: "owned" },
      { type: "status", status: unavailableStatus },
    );
    const attachedReady = nodeReducer(
      { ...initialNodeState(), ownership: "attached" },
      { type: "status", status: readyStatus },
    );

    expect(ownedUnavailable).toMatchObject({ phase: "attached", ownership: "owned" });
    expect(attachedReady).toMatchObject({ phase: "ready", ownership: "attached" });
    expect(actionGuards(ownedUnavailable).canStop).toBe(true);
    expect(actionGuards(attachedReady).canStop).toBe(false);
  });

  it("accepts native ownership snapshots without inventing health", () => {
    const state = nodeReducer(initialNodeState(), {
      type: "ownership",
      ownership: "owned",
    });
    expect(state).toMatchObject({ phase: "disconnected", ownership: "owned", status: null });
  });

  it("clears status, error, and ownership after an exact owned stop", () => {
    const state = {
      phase: "stopping" as const,
      ownership: "owned" as const,
      status: readyStatus,
      error: "old",
    };
    expect(nodeReducer(state, { type: "stopped" })).toEqual(initialNodeState());
  });

  it("ignores unsafe start and stop events", () => {
    const attached = {
      phase: "ready" as const,
      ownership: "attached" as const,
      status: readyStatus,
      error: null,
    };
    expect(nodeReducer(attached, { type: "start" })).toBe(attached);
    expect(nodeReducer(attached, { type: "stop" })).toBe(attached);
  });
});

describe("actionGuards", () => {
  it.each([
    ["disconnected", "none", true, true, false],
    ["connecting", "none", false, false, false],
    ["starting", "none", false, false, false],
    ["attached", "attached", false, false, false],
    ["ready", "attached", false, false, false],
    ["ready", "owned", false, false, true],
    ["stopping", "owned", false, false, false],
    ["recovery-required", "owned", false, false, false],
    ["error", "none", true, true, false],
    ["error", "attached", false, true, false],
    ["error", "owned", false, false, true],
  ] as const)(
    "%s with %s ownership enables only safe actions",
    (phase, ownership, canStart, canAttachRetry, canStop) => {
      expect(actionGuards({ phase, ownership, status: null, error: null })).toEqual({
        canStart,
        canAttachRetry,
        canStop,
      });
    },
  );
});
