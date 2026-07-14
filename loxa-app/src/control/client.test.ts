import { describe, expect, it, vi } from "vitest";

import {
  ControlClientError,
  cancelOperation,
  downloadModel,
  loadModel,
  unloadModel,
  getCapabilities,
  getControlNode,
  getNodeIdentityProof,
  getInventory,
  getOperation,
} from "./client";

const token = "ab".repeat(32);
const node = { status: "unloaded", active_model_id: null, operation_id: null, error: null };

describe("control client", () => {
  it("sends the bearer only in the authorization header", async () => {
    const fetch = vi.fn(async (input: string, init?: RequestInit) => {
      void input;
      void init;
      return Response.json(node);
    });
    await getControlNode("http://127.0.0.1:8080/", token, { fetch });

    expect(fetch).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/loxa/v1/node",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({ authorization: `Bearer ${token}` }),
      }),
    );
    expect(fetch.mock.calls[0][0]).not.toContain(token);
  });

  it("keeps node proof unauthenticated and challenge-bound for native bootstrap", async () => {
    const nonce = "01".repeat(32);
    const fetch = vi.fn(async (_input: string, init?: RequestInit) =>
      Response.json(
        {
          protocol_version: 1,
          node_id: "node-1",
          runtime_identity: "runtime-1",
          status: "unloaded",
          challenge_proof: "02".repeat(32),
        },
        { headers: { "x-seen-challenge": String(new Headers(init?.headers).get("x-loxa-challenge")) } },
      ),
    );
    await expect(getNodeIdentityProof("http://127.0.0.1:8080", nonce, { fetch })).resolves.toMatchObject({
      protocolVersion: 1,
      nodeId: "node-1",
    });
    const headers = new Headers(fetch.mock.calls[0][1]?.headers);
    expect(headers.get("x-loxa-challenge")).toBe(nonce);
    expect(headers.has("authorization")).toBe(false);
  });

  it.each([
    "http://example.com:8080",
    "https://127.0.0.1:8080",
    "http://127.0.0.1:8080/path",
    "http://user@127.0.0.1:8080",
  ])("refuses to send the credential outside the exact loopback endpoint: %s", async (endpoint) => {
    const fetch = vi.fn();
    await expect(getControlNode(endpoint, token, { fetch })).rejects.toMatchObject({ kind: "endpoint" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("uses only known recipe IDs for download and closes operation routes", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ operation_id: "op-1" }, { status: 202 }))
      .mockResolvedValueOnce(
        Response.json({
          id: "op-1",
          kind: "download",
          status: "queued",
          model_id: "gemma-3-4b-it-q4",
          progress: null,
          error: null,
          created_at_unix_ms: 1,
          updated_at_unix_ms: 1,
        }),
      )
      .mockResolvedValueOnce(
        Response.json({
          id: "op-1",
          kind: "download",
          status: "cancelled",
          model_id: "gemma-3-4b-it-q4",
          progress: null,
          error: null,
          created_at_unix_ms: 1,
          updated_at_unix_ms: 2,
        }),
      );

    await expect(downloadModel("http://127.0.0.1:8080", token, "gemma-3-4b-it-q4", { fetch })).resolves.toEqual({
      operationId: "op-1",
    });
    await expect(getOperation("http://127.0.0.1:8080", token, "op-1", { fetch })).resolves.toMatchObject({
      status: "queued",
    });
    await expect(cancelOperation("http://127.0.0.1:8080", token, "op-1", { fetch })).resolves.toMatchObject({
      status: "cancelled",
    });
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({ model_id: "gemma-3-4b-it-q4" });
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      "http://127.0.0.1:8080/loxa/v1/models/download",
      "http://127.0.0.1:8080/loxa/v1/operations/op-1",
      "http://127.0.0.1:8080/loxa/v1/operations/op-1/cancel",
    ]);
  });

  it("starts authenticated load and unload operations without accepting launch details", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ operation_id: "op-load" }, { status: 202 }))
      .mockResolvedValueOnce(Response.json({ operation_id: "op-unload" }, { status: 202 }));

    await expect(loadModel("http://127.0.0.1:8080", token, "gemma-3-4b-it-q4", { fetch })).resolves.toEqual({
      operationId: "op-load",
    });
    await expect(unloadModel("http://127.0.0.1:8080", token, { fetch })).resolves.toEqual({ operationId: "op-unload" });

    expect(fetch).toHaveBeenNthCalledWith(
      1,
      "http://127.0.0.1:8080/loxa/v1/models/load",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ model_id: "gemma-3-4b-it-q4" }),
        headers: expect.objectContaining({ authorization: `Bearer ${token}` }),
      }),
    );
    expect(fetch).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8080/loxa/v1/models/unload",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ authorization: `Bearer ${token}` }),
      }),
    );
  });

  it("decodes capabilities and inventory", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        Response.json({ document_input: false, document_input_reason: "Text only", text_chat: true }),
      )
      .mockResolvedValueOnce(Response.json([]));
    await expect(getCapabilities("http://127.0.0.1:8080", token, { fetch })).resolves.toMatchObject({
      documentInput: false,
    });
    await expect(getInventory("http://127.0.0.1:8080", token, { fetch })).resolves.toEqual([]);
  });

  it("preserves actionable closed control errors without leaking the token", async () => {
    const fetch = vi.fn(async () =>
      Response.json({ code: "operation_conflict", message: "download active" }, { status: 409 }),
    );
    const error = await downloadModel("http://127.0.0.1:8080", token, "gemma-3-4b-it-q4", { fetch }).catch(
      (reason: unknown) => reason,
    );
    expect(error).toBeInstanceOf(ControlClientError);
    expect(error).toMatchObject({ kind: "http", status: 409, code: "operation_conflict", message: "download active" });
    expect(String(error)).not.toContain(token);
  });

  it.each(["", "wrong", "AB".repeat(32)])("rejects an unsafe token before fetch: %j", async (unsafeToken) => {
    const fetch = vi.fn();
    await expect(getControlNode("http://127.0.0.1:8080", unsafeToken, { fetch })).rejects.toMatchObject({
      kind: "credential",
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("distinguishes timeout, caller cancellation, transport, and malformed payloads", async () => {
    const timeoutFetch = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
        }),
    );
    await expect(
      getControlNode("http://127.0.0.1:8080", token, { fetch: timeoutFetch, timeoutMs: 1 }),
    ).rejects.toMatchObject({ kind: "timeout" });

    const caller = new AbortController();
    caller.abort();
    await expect(getControlNode("http://127.0.0.1:8080", token, { signal: caller.signal })).rejects.toMatchObject({
      kind: "aborted",
    });
    await expect(
      getControlNode("http://127.0.0.1:8080", token, {
        fetch: vi.fn(async () => {
          throw new Error("secret network detail");
        }),
      }),
    ).rejects.toMatchObject({ kind: "transport" });
    await expect(
      getControlNode("http://127.0.0.1:8080", token, { fetch: vi.fn(async () => Response.json({ nope: true })) }),
    ).rejects.toMatchObject({ kind: "invalid-response" });
  });

  it("stops reading and cancels a control response as soon as its byte limit is exceeded", async () => {
    const oversizedChunk = new Uint8Array(1024 * 1024);
    let index = 0;
    const chunks = [oversizedChunk, oversizedChunk, Uint8Array.of(1), Uint8Array.of(2)];
    const reader = {
      read: vi.fn(async () =>
        index < chunks.length
          ? { done: false as const, value: chunks[index++] }
          : { done: true as const, value: undefined },
      ),
      cancel: vi.fn(async () => undefined),
      releaseLock: vi.fn(),
    };
    const response = {
      ok: true,
      status: 200,
      headers: new Headers(),
      body: { getReader: () => reader },
    } as unknown as Response;

    await expect(
      getControlNode("http://127.0.0.1:8080", token, {
        fetch: vi.fn(async () => response),
      }),
    ).rejects.toMatchObject({ kind: "invalid-response" });
    expect(reader.read).toHaveBeenCalledTimes(3);
    expect(reader.cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();
  });
});
