import { describe, expect, it } from "vitest";

import type { NodeStatus } from "./contracts";
import { actionGuards, initialNodeState, nodeReducer, type NodePhase } from "./machine";

const readyStatus = {
  node_id: "node-test",
  health: "ready",
  model: "loxa" as const,
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
} satisfies NodeStatus;

const unavailableStatus = {
  ...readyStatus,
  health: "unavailable",
  engine: null,
  runtime_model: null,
  profile: null,
} satisfies NodeStatus;

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
    const stopping = nodeReducer({ ...ready, ownership: "owned" }, { type: "stop" });
    const recovery = nodeReducer(stopping, {
      type: "recoveryRequired",
      message: "ownership could not be proven",
    });
    const error = nodeReducer(connecting, { type: "probeFailed", message: "refused" });

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

  it.each(["stopping", "recovery-required", "error"] as const)("ignores a late ready status while %s", (phase) => {
    const state = {
      phase,
      ownership: "owned" as const,
      status: null,
      error: phase === "stopping" ? null : "preserve me",
    };
    expect(nodeReducer(state, { type: "status", status: readyStatus })).toBe(state);
  });

  it("accepts stopped only after an owned stop is in progress", () => {
    const ready = {
      phase: "ready" as const,
      ownership: "owned" as const,
      status: readyStatus,
      error: null,
    };
    const attached = { ...ready, phase: "attached" as const };
    const externallyStopping = { ...ready, phase: "stopping" as const, ownership: "attached" as const };

    expect(nodeReducer(ready, { type: "stopped" })).toBe(ready);
    expect(nodeReducer(attached, { type: "stopped" })).toBe(attached);
    expect(nodeReducer(externallyStopping, { type: "stopped" })).toBe(externallyStopping);
    expect(nodeReducer({ ...ready, phase: "stopping" }, { type: "stopped" })).toEqual(initialNodeState());
  });

  it("ignores a stale probe failure while stopping", () => {
    const stopping = {
      phase: "stopping" as const,
      ownership: "owned" as const,
      status: readyStatus,
      error: null,
    };

    expect(nodeReducer(stopping, { type: "probeFailed", message: "late refusal" })).toBe(stopping);
  });

  it("accepts a genuine failure from the active stop operation", () => {
    const stopping = {
      phase: "stopping" as const,
      ownership: "owned" as const,
      status: readyStatus,
      error: null,
    };

    expect(nodeReducer(stopping, { type: "stopFailed", message: "child did not exit" })).toMatchObject({
      phase: "error",
      ownership: "owned",
      status: null,
      error: "child did not exit",
    });
  });

  it("can complete a valid stop after ignoring a stale probe failure", () => {
    const stopping = {
      phase: "stopping" as const,
      ownership: "owned" as const,
      status: readyStatus,
      error: null,
    };
    const afterStaleProbe = nodeReducer(stopping, {
      type: "probeFailed",
      message: "late refusal",
    });

    expect(nodeReducer(afterStaleProbe, { type: "stopped" })).toEqual(initialNodeState());
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
