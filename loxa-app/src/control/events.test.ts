import { describe, expect, it, vi } from "vitest";

import { streamControlEvents, type ControlStreamCallbacks } from "./events";

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
  return { ok: true, status: 200, body: { getReader: () => reader } } as unknown as Response;
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
