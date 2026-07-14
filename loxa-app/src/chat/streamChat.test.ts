import { describe, expect, it, vi } from "vitest";

import { streamChat, type StreamCallbacks, type StreamTerminal } from "./streamChat";

const encoder = new TextEncoder();
const chunk = (text: string) => encoder.encode(text);
const delta = (text: string) => `data: ${JSON.stringify({ choices: [{ delta: { content: text } }] })}\n\n`;

function callbacks() {
  const terminals: StreamTerminal[] = [];
  const deltas: string[] = [];
  const value: StreamCallbacks = {
    onDelta: (text) => deltas.push(text),
    onTerminal: (terminal) => terminals.push(terminal),
  };
  return { value, terminals, deltas };
}

function responseFrom(chunks: Uint8Array[], onCancel = vi.fn()): Response {
  let index = 0;
  const reader = {
    read: vi.fn(async () =>
      index < chunks.length
        ? { done: false as const, value: chunks[index++] }
        : { done: true as const, value: undefined },
    ),
    cancel: onCancel,
    releaseLock: vi.fn(),
  };
  return {
    ok: true,
    status: 200,
    body: { getReader: () => reader },
  } as unknown as Response;
}

describe("streamChat", () => {
  it("times out only while waiting for response headers and aborts the request", async () => {
    const observed = callbacks();
    let requestSignal: AbortSignal | undefined;
    const fetch = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          requestSignal = init?.signal ?? undefined;
          requestSignal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), {
            once: true,
          });
        }),
    );

    const result = await streamChat("http://node", {}, observed.value, undefined, fetch, 1).finished;

    expect(result).toEqual({ kind: "error", message: "Timed out waiting for the Loxa node to begin responding." });
    expect(requestSignal?.aborted).toBe(true);
    expect(observed.terminals).toEqual([result]);
  });

  it("classifies cancellation between headers and body inspection exactly once", async () => {
    const observed = callbacks();
    let resolveResponse!: (response: Response) => void;
    const fetch = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveResponse = resolve;
        }),
    );
    const handle = streamChat("http://node", {}, observed.value, undefined, fetch);
    resolveResponse({
      ok: true,
      status: 200,
      get body() {
        handle.cancel();
        return null;
      },
    } as Response);

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(observed.terminals).toEqual([{ kind: "cancelled" }]);
  });
  it("posts the stable streaming request and emits deltas then one completion", async () => {
    const seen: { url?: string; init?: RequestInit } = {};
    const fetch = vi.fn(async (url: string, init?: RequestInit) => {
      seen.url = url;
      seen.init = init;
      return responseFrom([chunk(delta("hel") + delta("lo") + "data: [DONE]\n\n")]);
    });
    const observed = callbacks();

    const handle = streamChat(
      "http://127.0.0.1:31000/",
      { model: "loxa", messages: [{ role: "user", content: "hi" }] },
      observed.value,
      undefined,
      fetch,
    );

    await expect(handle.finished).resolves.toEqual({ kind: "completed" });
    expect(observed.deltas).toEqual(["hel", "lo"]);
    expect(observed.terminals).toEqual([{ kind: "completed" }]);
    expect(seen.url).toBe("http://127.0.0.1:31000/v1/chat/completions");
    expect(JSON.parse(String(seen.init?.body))).toMatchObject({ model: "loxa", stream: true });
  });

  it("handles arbitrary SSE byte and UTF-8 splits", async () => {
    const bytes = chunk(delta("🙂 café") + "data: [DONE]\r\n\r\n");
    const observed = callbacks();
    const fetch = vi.fn(async () => responseFrom([...bytes].map((byte) => Uint8Array.of(byte))));

    await streamChat("http://node", {}, observed.value, undefined, fetch).finished;

    expect(observed.deltas).toEqual(["🙂 café"]);
    expect(observed.terminals).toEqual([{ kind: "completed" }]);
  });

  it("ignores comments and empty OpenAI deltas", async () => {
    const observed = callbacks();
    const fetch = vi.fn(async () =>
      responseFrom([
        chunk(
          ": keepalive\n\n" + delta("") + 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n' + "data: [DONE]\n\n",
        ),
      ]),
    );

    await streamChat("http://node", {}, observed.value, undefined, fetch).finished;
    expect(observed.deltas).toEqual([]);
    expect(observed.terminals).toEqual([{ kind: "completed" }]);
  });

  it.each([
    ["malformed JSON", [chunk("data: {nope}\n\n")], /malformed/i],
    ["invalid UTF-8", [Uint8Array.of(0xff, 0x0a, 0x0a)], /malformed/i],
    ["oversized event", [chunk(`data: ${"x".repeat(2 * 1024 * 1024 + 1)}`)], /larger than 2 MiB/i],
    ["EOF before DONE", [chunk(delta("partial"))], /before.*done/i],
    ["no DONE", [], /before.*done/i],
    ["unterminated DONE", [chunk("data: [DONE]")], /before.*done/i],
  ])("reports %s once and cancels the response body", async (_name, chunks, message) => {
    const observed = callbacks();
    const cancel = vi.fn(async () => undefined);
    const fetch = vi.fn(async () => responseFrom(chunks, cancel));

    const result = await streamChat("http://node", {}, observed.value, undefined, fetch).finished;

    expect(result).toMatchObject({ kind: "error", message: expect.stringMatching(message) });
    expect(observed.terminals).toEqual([result]);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("ignores duplicate DONE frames and data after the first terminal", async () => {
    const observed = callbacks();
    const cancel = vi.fn(async () => undefined);
    const fetch = vi.fn(async () =>
      responseFrom([chunk(delta("before") + "data: [DONE]\n\ndata: [DONE]\n\n" + delta("after"))], cancel),
    );

    await streamChat("http://node", {}, observed.value, undefined, fetch).finished;
    expect(observed.deltas).toEqual(["before"]);
    expect(observed.terminals).toEqual([{ kind: "completed" }]);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("cancels a body kept open after the first framed DONE", async () => {
    const cancel = vi.fn(async () => undefined);
    const reader = {
      read: vi
        .fn()
        .mockResolvedValueOnce({ done: false, value: chunk("data: [DONE]\n\n") })
        .mockImplementation(() => new Promise(() => undefined)),
      cancel,
      releaseLock: vi.fn(),
    };
    const fetch = vi.fn(
      async () =>
        ({
          ok: true,
          status: 200,
          body: { getReader: () => reader },
        }) as unknown as Response,
    );
    const observed = callbacks();

    await expect(streamChat("http://node", {}, observed.value, undefined, fetch).finished).resolves.toEqual({
      kind: "completed",
    });
    expect(reader.read).toHaveBeenCalledOnce();
    expect(cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();
    expect(observed.terminals).toEqual([{ kind: "completed" }]);
  });

  it("stops callbacks immediately when onDelta disposes a multi-choice event", async () => {
    const deltas: string[] = [];
    const terminals: StreamTerminal[] = [];
    const cancel = vi.fn(async () => undefined);
    const payload = JSON.stringify({
      choices: [{ delta: { content: "first" } }, { delta: { content: "second" } }],
    });
    const fetch = vi.fn(async () => responseFrom([chunk(`data: ${payload}\n\ndata: [DONE]\n\n`)], cancel));
    const handle = streamChat(
      "http://node",
      {},
      {
        onDelta: (text) => {
          deltas.push(text);
          handle.dispose();
        },
        onTerminal: (terminal) => terminals.push(terminal),
      },
      undefined,
      fetch,
    );

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(deltas).toEqual(["first"]);
    expect(terminals).toEqual([]);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("cancels a never-ending body after a malformed event", async () => {
    const cancel = vi.fn(async () => undefined);
    const reader = {
      read: vi
        .fn()
        .mockResolvedValueOnce({ done: false, value: chunk("data: {bad}\n\n") })
        .mockImplementation(() => new Promise(() => undefined)),
      cancel,
      releaseLock: vi.fn(),
    };
    const fetch = vi.fn(
      async () =>
        ({
          ok: true,
          status: 200,
          body: { getReader: () => reader },
        }) as unknown as Response,
    );
    const observed = callbacks();

    await expect(streamChat("http://node", {}, observed.value, undefined, fetch).finished).resolves.toMatchObject({
      kind: "error",
    });
    expect(reader.read).toHaveBeenCalledOnce();
    expect(cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();
  });

  it("preserves OpenAI error details for HTTP failures", async () => {
    const observed = callbacks();
    const fetch = vi.fn(async () =>
      Response.json(
        { error: { message: "engine unavailable", type: "server_error", param: null, code: "engine_unavailable" } },
        { status: 503 },
      ),
    );

    const result = await streamChat("http://node", {}, observed.value, undefined, fetch).finished;
    expect(result).toEqual({ kind: "error", message: "engine unavailable" });
    expect(observed.terminals).toEqual([result]);
  });

  it("reports malformed HTTP errors safely", async () => {
    const observed = callbacks();
    const fetch = vi.fn(async () => new Response("bad gateway", { status: 502 }));
    await expect(streamChat("http://node", {}, observed.value, undefined, fetch).finished).resolves.toEqual({
      kind: "error",
      message: "The Loxa node returned HTTP 502.",
    });
  });

  it("rejects a streaming response after the cumulative body exceeds 2 MiB", async () => {
    const observed = callbacks();
    const cancel = vi.fn(async () => undefined);
    const payload = chunk(`: ${"x".repeat(700 * 1024)}\n\n`);
    const fetch = vi.fn(async () => responseFrom([payload, payload, payload], cancel));

    const result = await streamChat("http://node", {}, observed.value, undefined, fetch).finished;

    expect(result).toEqual({ kind: "error", message: "The Loxa node returned a chat response larger than 2 MiB." });
    expect(observed.terminals).toEqual([result]);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("bounds an HTTP error body before attempting to decode it", async () => {
    const observed = callbacks();
    const cancel = vi.fn(async () => undefined);
    const payload = chunk("x".repeat(1024 * 1024 + 1));
    const response = responseFrom([payload, payload], cancel) as Response & { status: number };
    Object.defineProperties(response, { ok: { value: false }, status: { value: 500 } });
    const fetch = vi.fn(async () => response);

    const result = await streamChat("http://node", {}, observed.value, undefined, fetch).finished;

    expect(result).toEqual({ kind: "error", message: "The Loxa node returned HTTP 500." });
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("reports reader failure once and releases its lock", async () => {
    const releaseLock = vi.fn();
    const reader = {
      read: vi.fn().mockRejectedValue(new Error("socket broke")),
      cancel: vi.fn(),
      releaseLock,
    };
    const fetch = vi.fn(
      async () =>
        ({
          ok: true,
          status: 200,
          body: { getReader: () => reader },
        }) as unknown as Response,
    );
    const observed = callbacks();

    const result = await streamChat("http://node", {}, observed.value, undefined, fetch).finished;
    expect(result).toEqual({ kind: "error", message: "The chat stream failed while reading." });
    expect(observed.terminals).toEqual([result]);
    expect(releaseLock).toHaveBeenCalledOnce();
  });

  it("caller cancellation aborts fetch, cancels the reader, and terminates once", async () => {
    const caller = new AbortController();
    const cancel = vi.fn(async () => undefined);
    let releaseRead!: () => void;
    const reader = {
      read: vi.fn(
        () =>
          new Promise<ReadableStreamReadResult<Uint8Array>>((resolve) => {
            releaseRead = () => resolve({ done: true, value: undefined });
          }),
      ),
      cancel,
      releaseLock: vi.fn(),
    };
    const fetch = vi.fn(
      async () => ({ ok: true, status: 200, body: { getReader: () => reader } }) as unknown as Response,
    );
    const observed = callbacks();
    const handle = streamChat("http://node", {}, observed.value, caller.signal, fetch);
    await vi.waitFor(() => expect(reader.read).toHaveBeenCalled());

    caller.abort();
    releaseRead();

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(cancel).toHaveBeenCalledOnce();
    expect(observed.terminals).toEqual([{ kind: "cancelled" }]);
  });

  it("dispose aborts downstream work and suppresses all later callbacks", async () => {
    const cancel = vi.fn(async () => undefined);
    let releaseRead!: () => void;
    const reader = {
      read: vi.fn(
        () =>
          new Promise<ReadableStreamReadResult<Uint8Array>>((resolve) => {
            releaseRead = () => resolve({ done: false, value: chunk(delta("late") + "data: [DONE]\n\n") });
          }),
      ),
      cancel,
      releaseLock: vi.fn(),
    };
    const fetch = vi.fn(
      async () => ({ ok: true, status: 200, body: { getReader: () => reader } }) as unknown as Response,
    );
    const observed = callbacks();
    const handle = streamChat("http://node", {}, observed.value, undefined, fetch);
    await vi.waitFor(() => expect(reader.read).toHaveBeenCalled());

    handle.dispose();
    releaseRead();

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();
    expect(observed.deltas).toEqual([]);
    expect(observed.terminals).toEqual([]);
  });

  it("disposal while fetch is settling cancels a late body without reading it", async () => {
    let resolveFetch!: (response: Response) => void;
    const cancel = vi.fn(async () => undefined);
    const reader = { read: vi.fn(), cancel, releaseLock: vi.fn() };
    const fetch = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const observed = callbacks();
    const handle = streamChat("http://node", {}, observed.value, undefined, fetch);

    handle.dispose();
    resolveFetch({ ok: true, status: 200, body: { getReader: () => reader } } as unknown as Response);

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(reader.read).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();
    expect(observed.terminals).toEqual([]);
  });

  it("keeps the first abort cause immutable", async () => {
    const caller = new AbortController();
    const fetch = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            setTimeout(() => reject(new DOMException("aborted", "AbortError")), 10),
          );
        }),
    );
    const observed = callbacks();
    const handle = streamChat("http://node", {}, observed.value, caller.signal, fetch);

    caller.abort();
    handle.dispose();

    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(observed.terminals).toEqual([{ kind: "cancelled" }]);
  });

  it("handles an already-aborted caller without fetching", async () => {
    const caller = new AbortController();
    caller.abort();
    const fetch = vi.fn();
    const observed = callbacks();

    await expect(streamChat("http://node", {}, observed.value, caller.signal, fetch).finished).resolves.toEqual({
      kind: "cancelled",
    });
    expect(fetch).not.toHaveBeenCalled();
    expect(observed.terminals).toEqual([{ kind: "cancelled" }]);
  });
});
