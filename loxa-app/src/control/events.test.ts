import { describe, expect, it, vi } from "vitest";

import { decodeV2ControlEvent, decodeV2ReconnectSnapshot } from "./contracts";
import { proveV2ControlPeer } from "./client";
import {
  applyV2Event,
  applyV2Snapshot,
  openV2Events,
  streamControlEvents,
  type ControlStreamCallbacks,
  type V2StreamCallbacks,
} from "./events";
import {
  nextV2Event,
  validV2Event,
  validV2Node,
  validV2NodeCollection,
  validV2ReconnectSnapshot,
  validV2Slot,
  v1IdentityProof,
  v2Event,
  v2Ids,
} from "./testSupport";

const token = "ab".repeat(32);
const operation = {
  id: "op-1",
  kind: "download",
  status: "running",
  model_id: "gemma-3-4b-it-q4",
  progress: { completed_bytes: 5, total_bytes: 10 },
  error: null,
  created_at_unix_ms: 1,
  updated_at_unix_ms: 2,
};
const encode = (text: string) => new TextEncoder().encode(text);

function responseFrom(chunks: Uint8Array[], cancel = vi.fn(async () => undefined)): Response {
  let index = 0;
  const reader = {
    read: vi.fn(async () =>
      index < chunks.length
        ? { done: false as const, value: chunks[index++] }
        : { done: true as const, value: undefined },
    ),
    cancel,
    releaseLock: vi.fn(),
  };
  return {
    ok: true,
    status: 200,
    headers: new Headers({ "content-type": "text/event-stream" }),
    body: { getReader: () => reader },
  } as unknown as Response;
}

async function createV2Peer(streamResponse: Response) {
  let provedNodes = false;
  const fetch = vi.fn(async (input: string, init?: RequestInit) => {
    if (input.endsWith("/loxa/v1/node")) {
      const nonce = new Headers(init?.headers).get("x-loxa-challenge") ?? "";
      return Response.json({
        protocol_version: 1,
        node_id: v2Ids.node,
        runtime_identity: v2Ids.instance,
        status: "unloaded",
        challenge_proof: await v1IdentityProof(token, nonce),
      });
    }
    if (!provedNodes && input.endsWith("/loxa/v2/nodes")) {
      provedNodes = true;
      return Response.json(validV2NodeCollection);
    }
    return streamResponse;
  });
  return { peer: await proveV2ControlPeer("http://127.0.0.1:8080", token, { fetch }), fetch };
}

function callbacks() {
  const snapshots: unknown[] = [];
  const events: unknown[] = [];
  const terminals: unknown[] = [];
  const value: ControlStreamCallbacks = {
    onSnapshot: (snapshot) => snapshots.push(snapshot),
    onEvent: (event) => events.push(event),
    onTerminal: (terminal) => terminals.push(terminal),
  };
  return { value, snapshots, events, terminals };
}

function v2Callbacks() {
  const snapshots: unknown[] = [];
  const retainedEvents: unknown[] = [];
  const events: unknown[] = [];
  const terminals: unknown[] = [];
  const value: V2StreamCallbacks = {
    onSnapshot: (snapshot) => snapshots.push(snapshot),
    onRetainedEvent: (event) => retainedEvents.push(event),
    onEvent: (event) => events.push(event),
    onTerminal: (terminal) => terminals.push(terminal),
  };
  return { value, snapshots, retainedEvents, events, terminals };
}

function liveSlotEvent(nodeInstanceId: string | null) {
  return decodeV2ControlEvent({
    ...nextV2Event,
    entity: "slot",
    entity_id: v2Ids.slot,
    node_instance_id: nodeInstanceId,
    operation_id: null,
    slot: {
      ...validV2Slot,
      status: "ready",
      model_id: "gemma-3-4b-it-q4",
      operation_id: null,
    },
    operation: null,
  });
}

describe("streamControlEvents", () => {
  it("authenticates, resumes by cursor, and decodes snapshot plus operation frames across byte splits", async () => {
    const snapshot = { cursor: 4, cursor_gap: false, operations: [operation], events: [] };
    const event = { sequence: 5, operation };
    const bytes = encode(
      `event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n` +
        `id: 5\nevent: operation\ndata: ${JSON.stringify(event)}\n\n`,
    );
    const fetch = vi.fn(async () => responseFrom([...bytes].map((byte) => Uint8Array.of(byte))));
    const observed = callbacks();
    const handle = streamControlEvents("http://127.0.0.1:8080/", token, 3, observed.value, undefined, fetch);

    await expect(handle.finished).resolves.toEqual({
      kind: "error",
      cursor: 5,
      message: "Live model updates disconnected.",
    });
    expect(observed.snapshots).toHaveLength(1);
    expect(observed.events).toEqual([expect.objectContaining({ sequence: 5 })]);
    expect(observed.terminals).toEqual([{ kind: "error", cursor: 5, message: "Live model updates disconnected." }]);
    expect(fetch).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/loxa/v1/events?cursor=3",
      expect.objectContaining({ headers: expect.objectContaining({ authorization: `Bearer ${token}` }) }),
    );
  });

  it("disposal aborts the body and suppresses late callbacks", async () => {
    let releaseRead!: () => void;
    const cancel = vi.fn(async () => undefined);
    const reader = {
      read: vi.fn(
        () =>
          new Promise<ReadableStreamReadResult<Uint8Array>>((resolve) => {
            releaseRead = () =>
              resolve({
                done: false,
                value: encode(`event: operation\ndata: ${JSON.stringify({ sequence: 2, operation })}\n\n`),
              });
          }),
      ),
      cancel,
      releaseLock: vi.fn(),
    };
    const fetch = vi.fn(
      async () => ({ ok: true, status: 200, body: { getReader: () => reader } }) as unknown as Response,
    );
    const observed = callbacks();
    const handle = streamControlEvents("http://127.0.0.1:8080", token, 0, observed.value, undefined, fetch);
    await vi.waitFor(() => expect(reader.read).toHaveBeenCalled());

    handle.dispose();
    releaseRead();

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled", cursor: 0 });
    expect(cancel).toHaveBeenCalledOnce();
    expect(observed.events).toEqual([]);
    expect(observed.terminals).toEqual([]);
  });

  it("fails closed on malformed or regressing event streams", async () => {
    const observed = callbacks();
    const snapshot = { cursor: 0, cursor_gap: false, operations: [], events: [] };
    const fetch = vi.fn(async () =>
      responseFrom([
        encode(
          `event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\nid: 1\nevent: operation\ndata: ${JSON.stringify({ sequence: 1, operation })}\n\nid: 1\nevent: operation\ndata: ${JSON.stringify({ sequence: 1, operation })}\n\n`,
        ),
      ]),
    );
    await expect(
      streamControlEvents("http://127.0.0.1:8080", token, 0, observed.value, undefined, fetch).finished,
    ).resolves.toMatchObject({ kind: "error", message: expect.stringMatching(/invalid/i) });
    expect(observed.events).toHaveLength(1);
    expect(observed.terminals).toHaveLength(1);
  });

  it.each([
    ["one oversized chunk", [encode("x".repeat(2 * 1024 * 1024 + 1))]],
    ["cumulative unterminated chunks", [encode("x".repeat(1024 * 1024)), encode("x".repeat(1024 * 1024 + 1))]],
  ])("bounds %s, cancels the reader, and emits only a sanitized terminal error", async (_name, chunks) => {
    const cancel = vi.fn(async () => undefined);
    const observed = callbacks();
    const fetch = vi.fn(async () => responseFrom(chunks, cancel));

    const result = await streamControlEvents("http://127.0.0.1:8080", token, 0, observed.value, undefined, fetch)
      .finished;

    expect(result).toEqual({
      kind: "error",
      cursor: 0,
      message: "The Loxa node returned an invalid model update stream.",
    });
    expect(observed.snapshots).toEqual([]);
    expect(observed.events).toEqual([]);
    expect(observed.terminals).toEqual([result]);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("resets the buffered-frame budget after each valid frame", async () => {
    const snapshot = { cursor: 0, cursor_gap: false, operations: [], events: [] };
    const padding = `: ${"x".repeat(1024 * 1024)}\n`;
    const fetch = vi.fn(async () =>
      responseFrom([
        encode(`${padding}event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`),
        encode(`${padding}id: 1\nevent: operation\ndata: ${JSON.stringify({ sequence: 1, operation })}\n\n`),
      ]),
    );
    const observed = callbacks();

    const result = await streamControlEvents("http://127.0.0.1:8080", token, 0, observed.value, undefined, fetch)
      .finished;

    expect(result).toMatchObject({ kind: "error", cursor: 1, message: "Live model updates disconnected." });
    expect(observed.snapshots).toHaveLength(1);
    expect(observed.events).toHaveLength(1);
  });

  it("times out a stalled connection and reports a sanitized error", async () => {
    vi.useFakeTimers();
    try {
      const observed = callbacks();
      const fetch = vi.fn(
        (_url: string, init?: RequestInit) =>
          new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
          }),
      );
      const handle = streamControlEvents("http://127.0.0.1:8080", token, 0, observed.value, undefined, fetch);

      await vi.advanceTimersByTimeAsync(5_000);

      await expect(handle.finished).resolves.toEqual({
        kind: "error",
        cursor: 0,
        message: "Connecting to live model updates timed out.",
      });
      expect(observed.terminals).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects an unterminated final control frame instead of silently dropping it", async () => {
    const observed = callbacks();
    const snapshot = { cursor: 0, cursor_gap: false, operations: [], events: [] };
    const fetch = vi.fn(async () => responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(snapshot)}`)]));

    const result = await streamControlEvents("http://127.0.0.1:8080", token, 0, observed.value, undefined, fetch)
      .finished;

    expect(result).toEqual({
      kind: "error",
      cursor: 0,
      message: "The Loxa node returned an invalid model update stream.",
    });
    expect(observed.snapshots).toEqual([]);
  });
});

describe("durable v2 control events", () => {
  it("keeps v1 numeric cursors independent from v2 epoch-scoped decimal cursors", async () => {
    const v1 = callbacks();
    const v1Handle = streamControlEvents(
      "http://127.0.0.1:8080",
      token,
      3,
      v1.value,
      undefined,
      vi.fn(async () =>
        responseFrom([
          encode('event: snapshot\ndata: {"cursor":4,"cursor_gap":false,"operations":[],"events":[]}\n\n'),
        ]),
      ),
    );
    await expect(v1Handle.finished).resolves.toEqual({
      kind: "error",
      cursor: 4,
      message: "Live model updates disconnected.",
    });

    const v2 = applyV2Snapshot(undefined, decodeV2ReconnectSnapshot(validV2ReconnectSnapshot));
    expect(v1.snapshots).toHaveLength(1);
    expect(v2.epoch).toBe(v2Ids.epoch);
    expect(v2.cursor).toBe("11");
  });

  it("fully replaces collections on every snapshot without replaying retained event records", () => {
    const initial = applyV2Snapshot(undefined, decodeV2ReconnectSnapshot(validV2ReconnectSnapshot));
    const replacement = decodeV2ReconnectSnapshot({
      ...validV2ReconnectSnapshot,
      generated_at_unix_ms: "1784246400700",
      slots: [{ ...validV2Slot, status: "unloaded", model_id: null, operation_id: null }],
      operations: [],
      events: [validV2Event],
    });

    const replaced = applyV2Snapshot(initial, replacement);

    expect(replaced).not.toBe(initial);
    expect(replaced.epoch).toBe(initial.epoch);
    expect(replaced.cursor).toBe("11");
    expect(replaced.operations).toEqual([]);
    expect(replaced.slots).toEqual(replacement.slots);
    expect(replacement.events[0].operation).toEqual(validV2Event.operation);
    expect(applyV2Event(replaced, decodeV2ControlEvent(validV2Event))).toBe(replaced);
    expect(replaced.operations).toEqual([]);
  });

  it("replaces old epoch and gap state, ignores only exact duplicate identity, and rejects skipped cursors", () => {
    const state = applyV2Snapshot(undefined, decodeV2ReconnectSnapshot(validV2ReconnectSnapshot));
    const applied = applyV2Event(state, decodeV2ControlEvent(nextV2Event));
    expect(applied.cursor).toBe("12");
    expect(applyV2Event(applied, decodeV2ControlEvent(nextV2Event))).toBe(applied);
    expect(() => applyV2Event(applied, decodeV2ControlEvent({ ...nextV2Event, event_id: v2Ids.event }))).toThrow(
      /duplicate|regress|cursor/i,
    );
    expect(() =>
      applyV2Event(
        state,
        decodeV2ControlEvent({
          ...nextV2Event,
          sequence: "13",
          revision: "13",
          operation: { ...nextV2Event.operation, updated_revision: "13" },
        }),
      ),
    ).toThrow(/cursor/i);
    expect(
      applyV2Event(
        applied,
        decodeV2ControlEvent({
          ...nextV2Event,
          epoch: v2Ids.oldEpoch,
          sequence: "13",
          revision: "13",
          operation: { ...nextV2Event.operation, updated_revision: "13" },
        }),
      ),
    ).toBe(applied);

    const gap = decodeV2ReconnectSnapshot({
      ...validV2ReconnectSnapshot,
      epoch: v2Ids.oldEpoch,
      revision: "12",
      stream: { epoch: v2Ids.oldEpoch, cursor: "12", cursor_gap: true },
      nodes: [{ ...validV2Node, node_instance_id: v2Ids.nextEvent }],
      slots: [{ ...validV2Slot, status: "unloaded", model_id: null, operation_id: null }],
      operations: [],
      events: [],
    });
    const replaced = applyV2Snapshot(applied, gap);
    expect(replaced.epoch).toBe(v2Ids.oldEpoch);
    expect(replaced.operations).toEqual([]);
  });

  it("tracks bounded exact duplicate fingerprints and rejects identity reuse or conflicting payloads", () => {
    const earlier = decodeV2ControlEvent({
      ...v2Event(9),
      event_id: v2Ids.oldEpoch,
      committed_at_unix_ms: "1784246400499",
      operation: {
        ...v2Event(9).operation,
        created_revision: "10",
        updated_revision: "10",
        created_at_unix_ms: "1784246400000",
        updated_at_unix_ms: "1784246400499",
      },
    });
    const snapshot = decodeV2ReconnectSnapshot({ ...validV2ReconnectSnapshot, events: [earlier, validV2Event] });
    const state = applyV2Snapshot(undefined, snapshot);
    expect(applyV2Event(state, earlier)).toBe(state);

    const conflicting = decodeV2ControlEvent({
      ...earlier,
      committed_at_unix_ms: "1784246400498",
      operation: { ...earlier.operation, updated_at_unix_ms: "1784246400498" },
    });
    expect(() => applyV2Event(state, conflicting)).toThrow(/duplicate|identity/i);
    expect(() => applyV2Event(state, decodeV2ControlEvent({ ...nextV2Event, event_id: earlier.event_id }))).toThrow(
      /identity/i,
    );
  });

  it("rejects live ownership changes and exposes no mutable aliases", () => {
    const snapshot = decodeV2ReconnectSnapshot(validV2ReconnectSnapshot);
    const state = applyV2Snapshot(undefined, snapshot);
    expect(Object.isFrozen(state)).toBe(true);
    expect(Object.isFrozen(state.nodes[0])).toBe(true);
    expect(() => {
      (state.nodes[0] as { status: string }).status = "stopping";
    }).toThrow();
    (snapshot.nodes[0] as { status: string }).status = "stopping";
    expect(state.nodes[0]?.status).toBe("running");

    const foreign = decodeV2ControlEvent({
      ...nextV2Event,
      node_id: v2Ids.otherNode,
      slot: { ...nextV2Event.slot, node_id: v2Ids.otherNode },
      operation: { ...nextV2Event.operation, node_id: v2Ids.otherNode },
    });
    expect(() => applyV2Event(state, foreign)).toThrow(/correlation|ownership/i);
  });

  it("requires every applied live event to carry the exact proved node instance", () => {
    const state = applyV2Snapshot(undefined, decodeV2ReconnectSnapshot(validV2ReconnectSnapshot));
    expect(() => applyV2Event(state, liveSlotEvent(null))).toThrow(/instance|ownership|correlation/i);
    expect(() => applyV2Event(state, liveSlotEvent(v2Ids.nextEvent))).toThrow(/instance|ownership|correlation/i);
    expect(applyV2Event(state, liveSlotEvent(v2Ids.instance)).cursor).toBe("12");
  });

  it("uses decimal immediate successors without lossy number conversion", () => {
    const cursor = "9007199254740992";
    const snapshot = decodeV2ReconnectSnapshot({
      ...validV2ReconnectSnapshot,
      revision: cursor,
      stream: { ...validV2ReconnectSnapshot.stream, cursor },
      generated_at_unix_ms: "9007199254740992",
      slots: [{ ...validV2Slot, status: "unloaded", model_id: null, operation_id: null }],
      operations: [],
      events: [],
    });
    const event = decodeV2ControlEvent({
      ...nextV2Event,
      sequence: "9007199254740993",
      revision: "9007199254740993",
      committed_at_unix_ms: "9007199254740993",
      operation: {
        ...nextV2Event.operation,
        created_revision: cursor,
        updated_revision: "9007199254740993",
        created_at_unix_ms: cursor,
        updated_at_unix_ms: "9007199254740993",
      },
    });
    expect(applyV2Event(applyV2Snapshot(undefined, snapshot), event).cursor).toBe("9007199254740993");
  });

  it("opens an authenticated resume stream snapshot-first and separates retained from live events", async () => {
    const snapshot = { ...validV2ReconnectSnapshot, events: [validV2Event] };
    const bytes = encode(
      `event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n` +
        `id: 12\nevent: state\ndata: ${JSON.stringify(nextV2Event)}\n\n`,
    );
    const { peer, fetch } = await createV2Peer(responseFrom([...bytes].map((byte) => Uint8Array.of(byte))));
    const observed = v2Callbacks();
    const handle = openV2Events(peer, { epoch: v2Ids.epoch, cursor: "10" }, observed.value);

    await expect(handle.finished).resolves.toMatchObject({ kind: "error", cursor: "12" });
    expect(fetch).toHaveBeenCalledWith(
      `http://127.0.0.1:8080/loxa/v2/events?epoch=${v2Ids.epoch}&cursor=10`,
      expect.objectContaining({ method: "GET" }),
    );
    expect(observed.snapshots).toEqual([snapshot]);
    expect(observed.retainedEvents).toEqual([validV2Event]);
    expect(observed.events).toEqual([nextV2Event]);
  });

  it.each([
    {
      name: "unmarked epoch replacement",
      resume: { epoch: v2Ids.epoch, cursor: "11" },
      snapshot: {
        ...validV2ReconnectSnapshot,
        epoch: v2Ids.oldEpoch,
        stream: { epoch: v2Ids.oldEpoch, cursor: "11", cursor_gap: false },
      },
    },
    {
      name: "snapshot behind requested cursor",
      resume: { epoch: v2Ids.epoch, cursor: "12" },
      snapshot: validV2ReconnectSnapshot,
    },
    {
      name: "missing retained catch-up",
      resume: { epoch: v2Ids.epoch, cursor: "10" },
      snapshot: validV2ReconnectSnapshot,
    },
    {
      name: "retained event at the requested cursor",
      resume: { epoch: v2Ids.epoch, cursor: "11" },
      snapshot: { ...validV2ReconnectSnapshot, events: [validV2Event] },
    },
  ] as const)("rejects a reconnect snapshot with $name", async ({ resume, snapshot }) => {
    const peer = await createV2Peer(responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`)]));
    const observed = v2Callbacks();

    await expect(openV2Events(peer.peer, resume, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(observed.snapshots).toEqual([]);
    expect(observed.retainedEvents).toEqual([]);
  });

  it("accepts an explicitly marked replacement gap before delivering its snapshot", async () => {
    const replacement = {
      ...validV2ReconnectSnapshot,
      epoch: v2Ids.oldEpoch,
      stream: { epoch: v2Ids.oldEpoch, cursor: "11", cursor_gap: true },
    };
    const peer = await createV2Peer(
      responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(replacement)}\n\n`)]),
    );
    const observed = v2Callbacks();

    await expect(
      openV2Events(peer.peer, { epoch: v2Ids.epoch, cursor: "12" }, observed.value).finished,
    ).resolves.toMatchObject({ kind: "error", cursor: "11", message: expect.stringMatching(/disconnected/i) });
    expect(observed.snapshots).toEqual([replacement]);
  });

  it("rejects an unsolicited cursor gap on an initial stream before callbacks", async () => {
    const snapshot = {
      ...validV2ReconnectSnapshot,
      stream: { ...validV2ReconnectSnapshot.stream, cursor_gap: true },
    };
    const peer = await createV2Peer(responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`)]));
    const observed = v2Callbacks();

    await expect(openV2Events(peer.peer, undefined, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(observed.snapshots).toEqual([]);
    expect(observed.retainedEvents).toEqual([]);
  });

  it("rejects unsolicited retained history on an initial stream before callbacks", async () => {
    const snapshot = { ...validV2ReconnectSnapshot, events: [validV2Event] };
    const peer = await createV2Peer(responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`)]));
    const observed = v2Callbacks();

    await expect(openV2Events(peer.peer, undefined, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(observed.snapshots).toEqual([]);
    expect(observed.retainedEvents).toEqual([]);
  });

  it("rejects oversized or duplicate-key v2 event data before object construction", async () => {
    const observed = v2Callbacks();
    const duplicateEvent = JSON.stringify(nextV2Event).replace('"sequence":"12",', '"sequence":"12","sequence":"12",');
    const duplicatePeer = await createV2Peer(
      responseFrom([
        encode(`event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n`),
        encode(`id: 12\nevent: state\ndata: ${duplicateEvent}\n\n`),
      ]),
    );
    await expect(openV2Events(duplicatePeer.peer, undefined, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(observed.events).toEqual([]);

    const oversized = `${JSON.stringify(nextV2Event)}${" ".repeat(16 * 1024)}`;
    const cancel = vi.fn(async () => undefined);
    const oversizedPeer = await createV2Peer(
      responseFrom(
        [
          encode(
            `event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n` +
              `id: 12\nevent: state\ndata: ${oversized}\n\n`,
          ),
        ],
        cancel,
      ),
    );
    await expect(openV2Events(oversizedPeer.peer, undefined, v2Callbacks().value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("handles CRLF, bare CR, comments, retry, multiline data, and split UTF-8 exactly", async () => {
    const emojiEvent = {
      ...nextV2Event,
      operation: { ...nextV2Event.operation, model_id: "gemma-🚀" },
    };
    const dataLines = (value: unknown, ending: string) =>
      JSON.stringify(value, null, 2)
        .split("\n")
        .map((line) => `data: ${line}${ending}`)
        .join("");
    const text =
      `: snapshot comment\r\nretry: 1000\r\nevent: snapshot\r\n${dataLines(validV2ReconnectSnapshot, "\r\n")}\r\n` +
      `: live comment\rretry: 1000\rid: 12\revent: state\r${dataLines(emojiEvent, "\r")}\r`;
    const bytes = encode(text);
    const { peer } = await createV2Peer(responseFrom([...bytes].map((byte) => Uint8Array.of(byte))));
    const observed = v2Callbacks();

    await expect(openV2Events(peer, undefined, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      cursor: "12",
    });
    expect(observed.snapshots).toHaveLength(1);
    expect(observed.events).toEqual([emojiEvent]);
  });

  it("fails closed on invalid UTF-8, incomplete EOF, and cumulative undelimited overflow", async () => {
    const snapshotFrame = encode(`event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n`);
    const liveFrame = encode(`id: 12\nevent: state\ndata: ${JSON.stringify(nextV2Event)}\n\n`);
    const invalidLive = Uint8Array.from(liveFrame);
    const modelByte = invalidLive.indexOf("g".charCodeAt(0));
    invalidLive[modelByte] = 0xff;
    const invalidPeer = await createV2Peer(responseFrom([snapshotFrame, invalidLive]));
    await expect(openV2Events(invalidPeer.peer, undefined, v2Callbacks().value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });

    const incompletePeer = await createV2Peer(
      responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}`)]),
    );
    await expect(openV2Events(incompletePeer.peer, undefined, v2Callbacks().value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });

    const cancel = vi.fn(async () => undefined);
    const overflowPeer = await createV2Peer(
      responseFrom([new Uint8Array(1024 * 1024), new Uint8Array(1024 * 1024), new Uint8Array(1025)], cancel),
    );
    await expect(openV2Events(overflowPeer.peer, undefined, v2Callbacks().value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("bounds exact duplicate tracking to 1024 retained positions with explicit eviction semantics", () => {
    const retained = Array.from({ length: 1024 }, (_, index) => v2Event(index));
    const snapshot = decodeV2ReconnectSnapshot({
      ...validV2ReconnectSnapshot,
      revision: "1024",
      stream: { ...validV2ReconnectSnapshot.stream, cursor: "1024" },
      operations: [],
      slots: [{ ...validV2Slot, status: "unloaded", model_id: null, operation_id: null }],
      events: retained,
    });
    const state = applyV2Snapshot(undefined, snapshot);
    expect(applyV2Event(state, decodeV2ControlEvent(retained[1]))).toBe(state);
    expect(() =>
      applyV2Event(
        state,
        decodeV2ControlEvent({
          ...retained[1],
          committed_at_unix_ms: "3",
          operation: { ...retained[1].operation, updated_at_unix_ms: "3" },
        }),
      ),
    ).toThrow(/identity|duplicate/i);

    const next = decodeV2ControlEvent({
      ...v2Event(1024),
      operation: {
        ...v2Event(1024).operation,
        created_revision: "1",
        created_at_unix_ms: "1",
      },
    });
    const advanced = applyV2Event(state, next);
    expect(() => applyV2Event(advanced, decodeV2ControlEvent(retained[0]))).toThrow(/regress|cursor|identity/i);
    const reusedAfterEviction = decodeV2ControlEvent({
      ...v2Event(1025),
      event_id: retained[0].event_id,
      operation: {
        ...v2Event(1025).operation,
        created_revision: "1",
        created_at_unix_ms: "1",
      },
    });
    expect(applyV2Event(advanced, reusedAfterEviction).cursor).toBe("1026");
  });

  it("rejects nullable or replaced instances before delivering live SSE events", async () => {
    for (const instance of [null, v2Ids.nextEvent]) {
      const live = liveSlotEvent(instance);
      const peer = await createV2Peer(
        responseFrom([
          encode(
            `event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n` +
              `id: 12\nevent: state\ndata: ${JSON.stringify(live)}\n\n`,
          ),
        ]),
      );
      const observed = v2Callbacks();
      await expect(openV2Events(peer.peer, undefined, observed.value).finished).resolves.toMatchObject({
        kind: "error",
        message: expect.stringMatching(/invalid/i),
      });
      expect(observed.events).toEqual([]);
    }
  });

  it("rejects a foreign live node even when it reuses the proved instance before callbacks", async () => {
    const foreign = decodeV2ControlEvent({
      ...nextV2Event,
      node_id: v2Ids.otherNode,
      slot: { ...nextV2Event.slot, node_id: v2Ids.otherNode },
      operation: { ...nextV2Event.operation, node_id: v2Ids.otherNode },
    });
    const peer = await createV2Peer(
      responseFrom([
        encode(
          `event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n` +
            `id: 12\nevent: state\ndata: ${JSON.stringify(foreign)}\n\n`,
        ),
      ]),
    );
    const observed = v2Callbacks();

    await expect(openV2Events(peer.peer, undefined, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(observed.events).toEqual([]);
  });

  it("terminates the stream when a live event belongs to a foreign epoch", async () => {
    const foreign = decodeV2ControlEvent({
      ...nextV2Event,
      epoch: v2Ids.oldEpoch,
    });
    const peer = await createV2Peer(
      responseFrom([
        encode(
          `event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n` +
            `id: 12\nevent: state\ndata: ${JSON.stringify(foreign)}\n\n`,
        ),
      ]),
    );
    const observed = v2Callbacks();

    await expect(openV2Events(peer.peer, undefined, observed.value).finished).resolves.toEqual({
      kind: "error",
      cursor: "11",
      message: "The Loxa node returned an invalid durable update stream.",
    });
    expect(observed.events).toEqual([]);
    expect(observed.terminals).toEqual([
      {
        kind: "error",
        cursor: "11",
        message: "The Loxa node returned an invalid durable update stream.",
      },
    ]);
  });

  it("switches to the 16 KiB live budget inside one literal ReadableStream chunk", async () => {
    const oversized = `${JSON.stringify(nextV2Event)}${" ".repeat(16 * 1024)}`;
    const chunk = encode(
      `event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n` +
        `id: 12\nevent: state\ndata: ${oversized}\n\n`,
    );
    let pulls = 0;
    const response = new Response(
      new ReadableStream<Uint8Array>({
        pull(controller) {
          pulls += 1;
          controller.enqueue(chunk);
          controller.close();
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    );
    const peer = await createV2Peer(response);
    const observed = v2Callbacks();

    await expect(openV2Events(peer.peer, undefined, observed.value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid/i),
    });
    expect(pulls).toBe(1);
    expect(observed.snapshots).toHaveLength(1);
    expect(observed.events).toEqual([]);
  });

  it("delivers nullable pre-publication retained history separately without mutating replacement truth", async () => {
    const retained = liveSlotEvent(null);
    const snapshot = decodeV2ReconnectSnapshot({
      ...validV2ReconnectSnapshot,
      revision: "12",
      generated_at_unix_ms: "1784246400700",
      stream: { ...validV2ReconnectSnapshot.stream, cursor: "12" },
      slots: [{ ...validV2Slot, status: "unloaded", model_id: null, operation_id: null }],
      operations: [],
      events: [retained],
    });
    const peer = await createV2Peer(responseFrom([encode(`event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`)]));
    const observed = v2Callbacks();

    await openV2Events(peer.peer, { epoch: v2Ids.epoch, cursor: "11" }, observed.value).finished;
    const replacement = applyV2Snapshot(undefined, snapshot);
    expect(observed.retainedEvents).toEqual([retained]);
    expect(observed.events).toEqual([]);
    expect(replacement.operations).toEqual([]);
    expect(replacement.slots).toEqual(snapshot.slots);
  });

  it("requires SSE media type and strictly decodes bounded non-success errors", async () => {
    const plain = await createV2Peer(
      new Response(`event: snapshot\ndata: ${JSON.stringify(validV2ReconnectSnapshot)}\n\n`, {
        headers: { "content-type": "application/json" },
      }),
    );
    await expect(openV2Events(plain.peer, undefined, v2Callbacks().value).finished).resolves.toMatchObject({
      kind: "error",
      message: expect.stringMatching(/invalid|media/i),
    });

    const conflict = await createV2Peer(
      Response.json({ code: "operation_conflict", message: "A conflicting operation is active." }, { status: 409 }),
    );
    await expect(openV2Events(conflict.peer, undefined, v2Callbacks().value).finished).resolves.toEqual({
      kind: "error",
      cursor: "0",
      message: "A conflicting operation is active.",
    });
  });
});
