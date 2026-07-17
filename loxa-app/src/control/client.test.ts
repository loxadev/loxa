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
  fetchV2NodeCollection,
  fetchV2OperationCollection,
  fetchV2OperationEnvelope,
  fetchV2SlotCollection,
  proveV2ControlPeer,
  type ProvenControlPeer,
} from "./client";
import {
  validV2NodeCollection,
  validV2OperationCollection,
  validV2OperationEnvelope,
  validV2SlotCollection,
  v1IdentityProof,
  v2Ids,
} from "./testSupport";

const token = "ab".repeat(32);
const node = { status: "unloaded", active_model_id: null, operation_id: null, error: null };

async function createProvenPeer(responses: Response[] = []) {
  let provedNodes = false;
  const fetch = vi.fn(async (input: string, init?: RequestInit) => {
    if (input.endsWith("/loxa/v1/node")) {
      const nonce = new Headers(init?.headers).get("x-loxa-challenge") ?? "";
      return Response.json({
        protocol_version: 1,
        node_id: validV2NodeCollection.nodes[0].node_id,
        runtime_identity: validV2NodeCollection.nodes[0].node_instance_id,
        status: "unloaded",
        challenge_proof: await v1IdentityProof(token, nonce),
      });
    }
    if (!provedNodes && input.endsWith("/loxa/v2/nodes")) {
      provedNodes = true;
      return Response.json(validV2NodeCollection);
    }
    const response = responses.shift();
    if (!response) throw new Error(`unexpected request: ${input}`);
    return response;
  });
  return { peer: await proveV2ControlPeer("http://127.0.0.1:8080", token, { fetch }), fetch };
}

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
          node_id: "550e8400-e29b-41d4-a716-446655440000",
          runtime_identity: "550e8400-e29b-41d4-a716-446655440001",
          status: "unloaded",
          challenge_proof: "02".repeat(32),
        },
        { headers: { "x-seen-challenge": String(new Headers(init?.headers).get("x-loxa-challenge")) } },
      ),
    );
    await expect(getNodeIdentityProof("http://127.0.0.1:8080", nonce, { fetch })).resolves.toMatchObject({
      protocolVersion: 1,
      nodeId: "550e8400-e29b-41d4-a716-446655440000",
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

  it("fetches strict v2 collections only through the proven authenticated peer", async () => {
    const { peer, fetch } = await createProvenPeer([
      Response.json(validV2NodeCollection),
      Response.json(validV2SlotCollection),
      Response.json(validV2OperationCollection),
      Response.json(validV2OperationEnvelope),
    ]);

    await expect(fetchV2NodeCollection(peer)).resolves.toEqual(validV2NodeCollection);
    await expect(fetchV2SlotCollection(peer, validV2NodeCollection.nodes[0].node_id)).resolves.toEqual(
      validV2SlotCollection,
    );
    await expect(fetchV2OperationCollection(peer)).resolves.toEqual(validV2OperationCollection);
    await expect(fetchV2OperationEnvelope(peer, validV2OperationEnvelope.operation.operation_id)).resolves.toEqual(
      validV2OperationEnvelope,
    );
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      "http://127.0.0.1:8080/loxa/v1/node",
      "http://127.0.0.1:8080/loxa/v2/nodes",
      "http://127.0.0.1:8080/loxa/v2/nodes",
      `http://127.0.0.1:8080/loxa/v2/nodes/${validV2NodeCollection.nodes[0].node_id}/slots`,
      "http://127.0.0.1:8080/loxa/v1/node",
      "http://127.0.0.1:8080/loxa/v2/operations",
      "http://127.0.0.1:8080/loxa/v1/node",
      `http://127.0.0.1:8080/loxa/v2/operations/${validV2OperationEnvelope.operation.operation_id}`,
      "http://127.0.0.1:8080/loxa/v1/node",
    ]);
    for (const [path, init] of fetch.mock.calls) {
      const authorization = new Headers(init?.headers).get("authorization");
      expect(authorization).toBe(path.endsWith("/loxa/v1/node") ? null : `Bearer ${token}`);
    }
  });

  it("rejects a structurally forged peer and pins the proved node instance", async () => {
    const forgedFetch = vi.fn();
    const forged = { fetch: forgedFetch } as unknown as ProvenControlPeer;
    await expect(fetchV2NodeCollection(forged)).rejects.toMatchObject({ kind: "credential" });
    expect(forgedFetch).not.toHaveBeenCalled();

    let proofDone = false;
    const fetch = vi.fn(async (input: string, init?: RequestInit) => {
      if (!proofDone) {
        proofDone = true;
        const nonce = new Headers(init?.headers).get("x-loxa-challenge") ?? "";
        return Response.json({
          protocol_version: 1,
          node_id: validV2NodeCollection.nodes[0].node_id,
          runtime_identity: validV2NodeCollection.nodes[0].node_instance_id,
          status: "unloaded",
          challenge_proof: await v1IdentityProof(token, nonce),
        });
      }
      void input;
      return Response.json({
        ...validV2NodeCollection,
        nodes: [{ ...validV2NodeCollection.nodes[0], node_instance_id: "123e4567-e89b-42d3-a456-426614174099" }],
      });
    });
    await expect(proveV2ControlPeer("http://127.0.0.1:8080", token, { fetch })).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("rejects same-endpoint replacement after slot and operation responses without inventing wire keys", async () => {
    const cases = [
      {
        response: validV2SlotCollection,
        request: (peer: ProvenControlPeer) => fetchV2SlotCollection(peer, v2Ids.node),
      },
      { response: validV2OperationCollection, request: fetchV2OperationCollection },
      {
        response: validV2OperationEnvelope,
        request: (peer: ProvenControlPeer) => fetchV2OperationEnvelope(peer, v2Ids.operation),
      },
    ];
    for (const testCase of cases) {
      let proofCount = 0;
      let nodesServed = false;
      let payloadServed = false;
      const fetch = vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith("/loxa/v1/node")) {
          proofCount += 1;
          const nonce = new Headers(init?.headers).get("x-loxa-challenge") ?? "";
          const instance = proofCount === 1 ? v2Ids.instance : v2Ids.nextEvent;
          return Response.json({
            protocol_version: 1,
            node_id: v2Ids.node,
            runtime_identity: instance,
            status: "unloaded",
            challenge_proof: await v1IdentityProof(token, nonce, v2Ids.node, instance),
          });
        }
        if (!nodesServed && input.endsWith("/loxa/v2/nodes")) {
          nodesServed = true;
          return Response.json(validV2NodeCollection);
        }
        if (!payloadServed) {
          payloadServed = true;
          return Response.json(testCase.response);
        }
        throw new Error(`unexpected request: ${input}`);
      });
      const peer = await proveV2ControlPeer("http://127.0.0.1:8080", token, { fetch });
      await expect(testCase.request(peer)).rejects.toMatchObject({ kind: "credential" });
      expect(proofCount).toBe(2);
    }
  });

  it("bounds v2 bodies and rejects duplicate keys before ordinary JSON construction", async () => {
    const duplicate = JSON.stringify(validV2NodeCollection).replace(
      '"schema_version":2,',
      '"schema_version":2,"schema_version":2,',
    );
    const duplicatePeer = await createProvenPeer([
      new Response(duplicate, { headers: { "content-type": "application/json" } }),
    ]);
    await expect(fetchV2NodeCollection(duplicatePeer.peer)).rejects.toMatchObject({
      kind: "invalid-response",
    });

    const oversizedChunk = new Uint8Array(2 * 1024 * 1024 + 1);
    const reader = {
      read: vi
        .fn()
        .mockResolvedValueOnce({ done: false as const, value: oversizedChunk })
        .mockResolvedValueOnce({ done: true as const, value: undefined }),
      cancel: vi.fn(async () => undefined),
      releaseLock: vi.fn(),
    };
    const response = { ok: true, status: 200, body: { getReader: () => reader } } as unknown as Response;
    const oversizedPeer = await createProvenPeer([response]);
    await expect(fetchV2NodeCollection(oversizedPeer.peer)).rejects.toMatchObject({
      kind: "invalid-response",
    });
    expect(reader.read).toHaveBeenCalledOnce();
    expect(reader.cancel).toHaveBeenCalledOnce();
    expect(reader.releaseLock).toHaveBeenCalledOnce();

    const errorReader = {
      read: vi.fn(async () => ({ done: false as const, value: new Uint8Array(16 * 1024 + 1) })),
      cancel: vi.fn(async () => undefined),
      releaseLock: vi.fn(),
    };
    const errorResponse = {
      ok: false,
      status: 503,
      body: { getReader: () => errorReader },
    } as unknown as Response;
    const errorPeer = await createProvenPeer([errorResponse]);
    await expect(fetchV2NodeCollection(errorPeer.peer)).rejects.toMatchObject({
      kind: "invalid-response",
    });
    expect(errorReader.read).toHaveBeenCalledOnce();
    expect(errorReader.cancel).toHaveBeenCalledOnce();
  });

  it("requires JSON media types and strictly decodes bounded v2 error responses", async () => {
    const plain = await createProvenPeer([
      new Response(JSON.stringify(validV2NodeCollection), { headers: { "content-type": "text/plain" } }),
    ]);
    await expect(fetchV2NodeCollection(plain.peer)).rejects.toMatchObject({ kind: "invalid-response" });

    const conflict = await createProvenPeer([
      Response.json({ code: "operation_conflict", message: "A conflicting operation is active." }, { status: 409 }),
    ]);
    await expect(fetchV2NodeCollection(conflict.peer)).rejects.toMatchObject({
      kind: "http",
      status: 409,
      code: "operation_conflict",
    });
  });
});
