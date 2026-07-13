import { describe, expect, it, vi } from "vitest";
import {
  HistoryClientError,
  clearChats,
  createChat,
  deleteChat,
  getChat,
  getMessageContent,
  listChats,
  listMessageSummaries,
  listTurns,
  renameChat,
  streamPersistentTurn,
} from "./historyClient";

const endpoint = "http://127.0.0.1:8080";
const token = "ab".repeat(32);
const chatId = "0123456789abcdef0123456789abcdef";
const turnId = "1123456789abcdef0123456789abcdef";
const chat = { id: chatId, title: "Node health", created_at_ms: 10, updated_at_ms: 11 };

describe("history client", () => {
  it("lists chats using bounded opaque pagination and keeps content out of URLs", async () => {
    const cursor = "Abc_-0123456789";
    const fetch = vi.fn(async () => Response.json({ chats: [chat], next_before: cursor }));

    await expect(listChats(endpoint, token, { limit: 30, before: cursor }, { fetch })).resolves.toEqual({
      chats: [{ id: chatId, title: "Node health", createdAtMs: 10, updatedAtMs: 11 }],
      nextBefore: cursor,
    });

    expect(fetch).toHaveBeenCalledWith(
      `http://127.0.0.1:8080/loxa/v1/chats?limit=30&before=${encodeURIComponent(cursor)}`,
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({ authorization: `Bearer ${token}` }),
      }),
    );
  });

  it("creates, gets, renames, deletes, and explicitly clears chats", async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(Response.json(chat, { status: 201 }))
      .mockResolvedValueOnce(Response.json(chat))
      .mockResolvedValueOnce(Response.json({ ...chat, title: "Renamed", updated_at_ms: 12 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(Response.json({ deleted: 2 }));

    await expect(createChat(endpoint, token, { fetch })).resolves.toMatchObject({ id: chatId, title: "Node health" });
    await expect(getChat(endpoint, token, chatId, { fetch })).resolves.toMatchObject({ id: chatId });
    await expect(renameChat(endpoint, token, chatId, "Renamed", { fetch })).resolves.toMatchObject({ title: "Renamed" });
    await expect(deleteChat(endpoint, token, chatId, { fetch })).resolves.toBeUndefined();
    await expect(clearChats(endpoint, token, { fetch })).resolves.toEqual({ deleted: 2 });

    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      `${endpoint}/loxa/v1/chats`,
      `${endpoint}/loxa/v1/chats/${chatId}`,
      `${endpoint}/loxa/v1/chats/${chatId}`,
      `${endpoint}/loxa/v1/chats/${chatId}`,
      `${endpoint}/loxa/v1/chats/clear`,
    ]);
    expect(fetch.mock.calls[2][1]).toEqual(expect.objectContaining({ method: "PATCH", body: JSON.stringify({ title: "Renamed" }) }));
    expect(fetch.mock.calls[4][1]).toEqual(expect.objectContaining({ method: "POST", body: JSON.stringify({ confirm: "delete_all_chat_history" }) }));
  });

  it("strictly decodes bounded turn metadata pages", async () => {
    const fetch = vi.fn(async () => Response.json({
      turns: [{
        id: turnId,
        chat_id: chatId,
        ordinal: 0,
        state: "completed",
        provenance: { model_alias: "loxa", recipe_id: "gemma", engine_name: "llama.cpp", engine_version: null },
        error_code: null,
        created_at_ms: 20,
        updated_at_ms: 22,
      }],
      next_after: null,
    }));

    await expect(listTurns(endpoint, token, chatId, { limit: 30 }, { fetch })).resolves.toEqual({
      turns: [expect.objectContaining({
        id: turnId,
        state: "completed",
        recipeId: "gemma",
      })],
      nextAfter: null,
    });
  });

  it("restores message content from bounded UTF-8 segments", async () => {
    const messageId = "2123456789abcdef0123456789abcdef";
    const fetch = vi.fn()
      .mockResolvedValueOnce(Response.json({ messages: [{ id: messageId, turn_id: turnId, role: "user", content_bytes: 8, created_at_ms: 20, updated_at_ms: 20 }] }))
      .mockResolvedValueOnce(Response.json({
        message_id: messageId, turn_id: turnId, role: "user", segment_count: 2,
        segments: [{ message_id: messageId, turn_id: turnId, role: "user", segment_index: 0, segment_count: 2, content: "hi " }],
        next_segment: 1,
      }))
      .mockResolvedValueOnce(Response.json({
        message_id: messageId, turn_id: turnId, role: "user", segment_count: 2,
        segments: [{ message_id: messageId, turn_id: turnId, role: "user", segment_index: 1, segment_count: 2, content: "🙂" }],
        next_segment: null,
      }));

    await expect(listMessageSummaries(endpoint, token, chatId, turnId, { fetch })).resolves.toEqual([
      expect.objectContaining({ id: messageId, role: "user", contentBytes: 8 }),
    ]);
    await expect(getMessageContent(endpoint, token, chatId, turnId, messageId, { fetch })).resolves.toBe("hi 🙂");
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      `${endpoint}/loxa/v1/chats/${chatId}/turns/${turnId}/messages`,
      `${endpoint}/loxa/v1/chats/${chatId}/turns/${turnId}/messages/${messageId}?segment=0`,
      `${endpoint}/loxa/v1/chats/${chatId}/turns/${turnId}/messages/${messageId}?segment=1`,
    ]);
  });

  it("streams one persistent turn and explicitly cancels by server turn id", async () => {
    const encoder = new TextEncoder();
    let streamController!: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        streamController = controller;
        controller.enqueue(encoder.encode(
          `event: turn.started\ndata: ${JSON.stringify({ chat_id: chatId, turn_id: turnId, state: "streaming", omitted_turns: 3 })}\n\n` +
          `event: turn.delta\ndata: ${JSON.stringify({ turn_id: turnId, content: "hello" })}\n\n`,
        ));
      },
    });
    const fetch = vi.fn(async (url: string, init?: RequestInit) => {
      if (url.endsWith(`/turns/${turnId}/cancel`)) {
        expect(init).toEqual(expect.objectContaining({ method: "POST", body: "" }));
        streamController.enqueue(encoder.encode(
          `event: turn.cancelled\ndata: ${JSON.stringify({ turn_id: turnId, state: "cancelled", error_code: null })}\n\n`,
        ));
        streamController.close();
        return Response.json({ turn_id: turnId, cancel_requested: true }, { status: 202 });
      }
      return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
    });
    const onStarted = vi.fn();
    const onDelta = vi.fn();
    const onTerminal = vi.fn();

    const handle = streamPersistentTurn(endpoint, token, chatId, "hello", { onStarted, onDelta, onTerminal }, undefined, fetch);
    await vi.waitFor(() => expect(onStarted).toHaveBeenCalledWith(turnId, 3));
    handle.cancel();
    await expect(handle.finished).resolves.toEqual({ kind: "cancelled" });
    expect(onStarted).toHaveBeenCalledWith(turnId, 3);
    expect(onDelta).toHaveBeenCalledWith("hello");
    expect(onTerminal).toHaveBeenCalledWith({ kind: "cancelled" });
    expect(fetch.mock.calls[0]).toEqual([
      `${endpoint}/loxa/v1/chats/${chatId}/turns`,
      expect.objectContaining({ method: "POST", body: JSON.stringify({ content: "hello", model: "loxa" }) }),
    ]);
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    expect(fetch.mock.calls[1]).toEqual([
      `${endpoint}/loxa/v1/chats/${chatId}/turns/${turnId}/cancel`,
      expect.objectContaining({ method: "POST", body: "" }),
    ]);
  });

  it("decodes the node's nested typed error envelope", async () => {
    const fetch = vi.fn(async () => Response.json({ error: { code: "history_conflict", message: "chat history operation conflicts with current state" } }, { status: 409 }));
    await expect(deleteChat(endpoint, token, chatId, { fetch })).rejects.toMatchObject({
      kind: "http", status: 409, code: "history_conflict", message: "chat history operation conflicts with current state",
    });
  });

  it.each([
    { chats: [chat], next_before: null, extra: true },
    { chats: [{ ...chat, title: "" }], next_before: null },
    { chats: [{ ...chat, updated_at_ms: 9 }], next_before: null },
    { chats: [chat, chat], next_before: null },
  ])("rejects malformed list payloads without exposing data: %j", async (payload) => {
    await expect(listChats(endpoint, token, {}, { fetch: vi.fn(async () => Response.json(payload)) })).rejects.toMatchObject({
      kind: "invalid-response",
      message: "The Loxa node returned an invalid chat-history payload.",
    });
  });

  it("rejects invalid IDs, limits, cursors, and rename content before fetch", async () => {
    const fetch = vi.fn();
    await expect(getChat(endpoint, token, "../secret", { fetch })).rejects.toMatchObject({ kind: "invalid-request" });
    await expect(listChats(endpoint, token, { limit: 101 }, { fetch })).rejects.toMatchObject({ kind: "invalid-request" });
    await expect(listChats(endpoint, token, { before: "x y" }, { fetch })).rejects.toMatchObject({ kind: "invalid-request" });
    await expect(renameChat(endpoint, token, chatId, "\0secret", { fetch })).rejects.toMatchObject({ kind: "invalid-request" });
    await expect(renameChat(endpoint, token, chatId, "x".repeat(161), { fetch })).rejects.toMatchObject({ kind: "invalid-request" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["http://example.com:8080", "https://127.0.0.1:8080", "http://127.0.0.1:8080/path"])(
    "never sends the credential outside exact IPv4 loopback: %s",
    async (unsafeEndpoint) => {
      const fetch = vi.fn();
      await expect(listChats(unsafeEndpoint, token, {}, { fetch })).rejects.toMatchObject({ kind: "endpoint" });
      expect(fetch).not.toHaveBeenCalled();
    },
  );

  it("classifies typed HTTP, transport, timeout, and caller cancellation safely", async () => {
    const httpFetch = vi.fn(async () => Response.json({ code: "chat_busy", message: "Finish the active turn first." }, { status: 409 }));
    await expect(deleteChat(endpoint, token, chatId, { fetch: httpFetch })).rejects.toMatchObject({
      kind: "http", status: 409, code: "chat_busy", message: "Finish the active turn first.",
    });

    await expect(listChats(endpoint, token, {}, { fetch: vi.fn(async () => { throw new Error("private detail"); }) })).rejects.toMatchObject({
      kind: "transport", message: "Could not connect to Loxa chat history.",
    });

    const hanging = vi.fn((_url: string, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
    }));
    await expect(listChats(endpoint, token, {}, { fetch: hanging, timeoutMs: 1 })).rejects.toMatchObject({ kind: "timeout" });

    const caller = new AbortController();
    caller.abort();
    await expect(listChats(endpoint, token, {}, { signal: caller.signal })).rejects.toMatchObject({ kind: "aborted" });
  });

  it("cancels and rejects a response above the 1 MiB boundary", async () => {
    const chunk = new Uint8Array(512 * 1024);
    let calls = 0;
    const reader = {
      read: vi.fn(async () => ++calls <= 3 ? { done: false as const, value: chunk } : { done: true as const, value: undefined }),
      cancel: vi.fn(async () => undefined),
      releaseLock: vi.fn(),
    };
    const response = { ok: true, status: 200, body: { getReader: () => reader } } as unknown as Response;
    await expect(listChats(endpoint, token, {}, { fetch: vi.fn(async () => response) })).rejects.toMatchObject({ kind: "invalid-response" });
    expect(reader.cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();
  });

  it("rejects out-of-order turns and a mismatched chat", async () => {
    const payload = {
      turns: [{
        id: turnId, chat_id: chatId, ordinal: 0, state: "completed",
        provenance: { model_alias: "loxa", recipe_id: "gemma", engine_name: null, engine_version: null },
        error_code: null, created_at_ms: 20, updated_at_ms: 22,
      }, {
        id: "3123456789abcdef0123456789abcdef", chat_id: chatId, ordinal: 0, state: "completed",
        provenance: { model_alias: "loxa", recipe_id: "gemma", engine_name: null, engine_version: null },
        error_code: null, created_at_ms: 23, updated_at_ms: 24,
      }], next_after: null,
    };
    await expect(listTurns(endpoint, token, chatId, {}, { fetch: vi.fn(async () => Response.json(payload)) })).rejects.toBeInstanceOf(HistoryClientError);
  });

  it.each([
    ["missing field", (turn: Record<string, unknown>) => { delete turn.updated_at_ms; }],
    ["extra field", (turn: Record<string, unknown>) => { turn.extra = true; }],
    ["bad provenance", (turn: Record<string, unknown>) => { turn.provenance = { model_alias: "other", recipe_id: "gemma", engine_name: null, engine_version: null }; }],
    ["negative ordinal", (turn: Record<string, unknown>) => { turn.ordinal = -1; }],
    ["unknown state", (turn: Record<string, unknown>) => { turn.state = "done"; }],
  ])("strictly rejects turn DTO %s", async (_case, mutate) => {
    const turn: Record<string, unknown> = {
      id: turnId,
      chat_id: chatId,
      ordinal: 0,
      state: "completed",
      provenance: { model_alias: "loxa", recipe_id: "gemma", engine_name: null, engine_version: null },
      error_code: null,
      created_at_ms: 20,
      updated_at_ms: 22,
    };
    mutate(turn);
    await expect(listTurns(endpoint, token, chatId, {}, {
      fetch: vi.fn(async () => Response.json({ turns: [turn], next_after: null })),
    })).rejects.toMatchObject({ kind: "invalid-response" });
  });
});
