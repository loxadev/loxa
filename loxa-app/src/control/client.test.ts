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
  downloadV2Model,
  loadV2Slot,
  unloadV2Slot,
  cancelV2Operation,
  proveV2ControlPeer,
  type ProvenControlPeer,
} from "./client";
import {
  validV2NodeCollection,
  validV2OperationCollection,
  validV2OperationEnvelope,
  validV2OperationAccepted,
  validV2SlotCollection,
  v1IdentityProof,
  v2Ids,
} from "./testSupport";

const token = "ab".repeat(32);
const node = { status: "unloaded", active_model_id: null, operation_id: null, error: null };

async function createProvenPeer(
  responses: Response[] = [],
  options: { timeoutMs?: number; signal?: AbortSignal } = {},
) {
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
  return { peer: await proveV2ControlPeer("http://127.0.0.1:8080", token, { fetch, ...options }), fetch };
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

  it("posts all four strict v2 mutations through the opaque proven peer", async () => {
    const accepted = [
      validV2OperationAccepted,
      { ...validV2OperationAccepted, operation_id: v2Ids.nextEvent, revision: "11" },
      { ...validV2OperationAccepted, operation_id: v2Ids.oldEpoch, revision: "12" },
      { ...validV2OperationAccepted, revision: "13" },
    ];
    const { peer, fetch } = await createProvenPeer(accepted.map((value) => Response.json(value, { status: 202 })));

    await expect(downloadV2Model(peer, "gemma-3-4b-it-q4")).resolves.toEqual(accepted[0]);
    await expect(loadV2Slot(peer, v2Ids.node, v2Ids.slot, "gemma-3-4b-it-q4")).resolves.toEqual(accepted[1]);
    await expect(unloadV2Slot(peer, v2Ids.node, v2Ids.slot)).resolves.toEqual(accepted[2]);
    await expect(cancelV2Operation(peer, v2Ids.operation)).resolves.toEqual(accepted[3]);

    const mutationCalls = fetch.mock.calls.filter(([, init]) => init?.method === "POST");
    expect(mutationCalls.map(([url]) => url)).toEqual([
      "http://127.0.0.1:8080/loxa/v2/models/gemma-3-4b-it-q4/download",
      `http://127.0.0.1:8080/loxa/v2/nodes/${v2Ids.node}/slots/${v2Ids.slot}/load`,
      `http://127.0.0.1:8080/loxa/v2/nodes/${v2Ids.node}/slots/${v2Ids.slot}/unload`,
      `http://127.0.0.1:8080/loxa/v2/operations/${v2Ids.operation}/cancel`,
    ]);
    expect(mutationCalls.map(([, init]) => init?.body)).toEqual([
      "{}",
      JSON.stringify({ model_id: "gemma-3-4b-it-q4" }),
      "{}",
      "{}",
    ]);
    for (const [, init] of mutationCalls) {
      const headers = new Headers(init?.headers);
      expect(headers.get("authorization")).toBe(`Bearer ${token}`);
      expect(headers.get("content-type")).toBe("application/json");
      expect(headers.get("accept")).toBe("application/json");
    }
  });

  it("rejects malformed v2 mutation inputs before sending credentials", async () => {
    const { peer, fetch } = await createProvenPeer();
    const baseline = fetch.mock.calls.length;
    await expect(downloadV2Model(peer, " bad-model ")).rejects.toMatchObject({ kind: "invalid-response" });
    await expect(loadV2Slot(peer, v2Ids.nextEvent, v2Ids.slot, "gemma-3-4b-it-q4")).rejects.toMatchObject({
      kind: "invalid-response",
    });
    await expect(unloadV2Slot(peer, v2Ids.node, "not-a-uuid")).rejects.toMatchObject({
      kind: "invalid-response",
    });
    await expect(cancelV2Operation(peer, "not-a-uuid")).rejects.toMatchObject({ kind: "invalid-response" });
    await expect(downloadV2Model(peer, "\ud800")).rejects.toMatchObject({ kind: "invalid-response" });
    await expect(loadV2Slot(peer, v2Ids.node, v2Ids.slot, "\udc00")).rejects.toMatchObject({
      kind: "invalid-response",
    });
    expect(fetch.mock.calls).toHaveLength(baseline);
  });

  it("enforces v2 model identifiers at exact UTF-8 byte boundaries", async () => {
    const exact = "é".repeat(128);
    const { peer, fetch } = await createProvenPeer([Response.json(validV2OperationAccepted, { status: 202 })]);
    await expect(downloadV2Model(peer, exact)).resolves.toEqual(validV2OperationAccepted);
    const baseline = fetch.mock.calls.length;
    await expect(downloadV2Model(peer, `${exact}a`)).rejects.toMatchObject({ kind: "invalid-response" });
    expect(fetch.mock.calls).toHaveLength(baseline);
  });

  it("strictly decodes v2 mutation acceptance and typed overload errors", async () => {
    const malformed = await createProvenPeer([
      Response.json({ ...validV2OperationAccepted, extra: true }, { status: 202 }),
    ]);
    await expect(downloadV2Model(malformed.peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({
      kind: "invalid-response",
    });

    const overload = await createProvenPeer([
      Response.json(
        { code: "state_writer_overloaded", message: "The durable state writer is overloaded." },
        { status: 503 },
      ),
    ]);
    await expect(unloadV2Slot(overload.peer, v2Ids.node, v2Ids.slot)).rejects.toMatchObject({
      kind: "http",
      status: 503,
      code: "state_writer_overloaded",
    });

    const tooLarge = await createProvenPeer([
      Response.json({ code: "invalid_request", message: "The request body exceeds 4096 bytes." }, { status: 413 }),
    ]);
    await expect(downloadV2Model(tooLarge.peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({
      kind: "http",
      status: 413,
      code: "invalid_request",
    });

    const mismatched = await createProvenPeer([
      Response.json({ code: "operation_conflict", message: "Conflict." }, { status: 503 }),
    ]);
    await expect(cancelV2Operation(mismatched.peer, v2Ids.operation)).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("requires exact v2 mutation acceptance status, media type, and size", async () => {
    const wrongStatus = await createProvenPeer([Response.json(validV2OperationAccepted, { status: 200 })]);
    await expect(downloadV2Model(wrongStatus.peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({
      kind: "invalid-response",
    });

    const wrongMedia = await createProvenPeer([
      new Response(JSON.stringify(validV2OperationAccepted), {
        status: 202,
        headers: { "content-type": "text/plain" },
      }),
    ]);
    await expect(downloadV2Model(wrongMedia.peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({
      kind: "invalid-response",
    });

    const oversized = await createProvenPeer([
      new Response(" ".repeat(16 * 1024 + 1), {
        status: 202,
        headers: { "content-type": "application/json" },
      }),
    ]);
    await expect(downloadV2Model(oversized.peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("rejects a replacement after mutation without sending it follow-up credentials", async () => {
    let proofCount = 0;
    let nodesServed = false;
    let mutationServed = false;
    const fetch = vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/loxa/v1/node")) {
        proofCount += 1;
        const headers = new Headers(init?.headers);
        expect(headers.has("authorization")).toBe(false);
        const nonce = headers.get("x-loxa-challenge") ?? "";
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
      if (!mutationServed) {
        mutationServed = true;
        expect(new Headers(init?.headers).get("authorization")).toBe(`Bearer ${token}`);
        return Response.json(validV2OperationAccepted, { status: 202 });
      }
      throw new Error(`unexpected credentialed follow-up: ${input}`);
    });
    const peer = await proveV2ControlPeer("http://127.0.0.1:8080", token, { fetch });

    await expect(downloadV2Model(peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({ kind: "credential" });
    await expect(downloadV2Model(peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({ kind: "credential" });
    expect(proofCount).toBe(2);
  });

  it("times out and revokes a peer whose mutation response body stalls", async () => {
    const stalled = new Response(new ReadableStream({ start() {} }), {
      status: 202,
      headers: { "content-type": "application/json" },
    });
    const { peer, fetch } = await createProvenPeer([stalled], { timeoutMs: 10 });
    await expect(downloadV2Model(peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({ kind: "timeout" });
    const baseline = fetch.mock.calls.length;
    await expect(downloadV2Model(peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({ kind: "credential" });
    expect(fetch.mock.calls).toHaveLength(baseline);
  });

  it("honors caller cancellation through mutation body consumption and revokes the peer", async () => {
    const controller = new AbortController();
    const stalled = new Response(new ReadableStream({ start() {} }), {
      status: 202,
      headers: { "content-type": "application/json" },
    });
    const { peer, fetch } = await createProvenPeer([stalled], { timeoutMs: 5_000, signal: controller.signal });
    const mutation = downloadV2Model(peer, "gemma-3-4b-it-q4");
    await vi.waitFor(() => {
      expect(fetch.mock.calls.some(([, init]) => init?.method === "POST")).toBe(true);
    });
    controller.abort();
    await expect(mutation).rejects.toMatchObject({ kind: "aborted" });
    const baseline = fetch.mock.calls.length;
    await expect(downloadV2Model(peer, "gemma-3-4b-it-q4")).rejects.toMatchObject({ kind: "credential" });
    expect(fetch.mock.calls).toHaveLength(baseline);
  });

  it("serializes v2 mutations per proven peer across reproof", async () => {
    let releaseFirst: ReadableStreamDefaultController<Uint8Array> | undefined;
    const first = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          releaseFirst = controller;
        },
      }),
      { status: 202, headers: { "content-type": "application/json" } },
    );
    const secondAccepted = { ...validV2OperationAccepted, operation_id: v2Ids.nextEvent, revision: "11" };
    const { peer, fetch } = await createProvenPeer([first, Response.json(secondAccepted, { status: 202 })]);

    const download = downloadV2Model(peer, "gemma-3-4b-it-q4");
    const unload = unloadV2Slot(peer, v2Ids.node, v2Ids.slot);
    await vi.waitFor(() => {
      expect(fetch.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    });
    releaseFirst?.enqueue(new TextEncoder().encode(JSON.stringify(validV2OperationAccepted)));
    releaseFirst?.close();

    await expect(download).resolves.toEqual(validV2OperationAccepted);
    await expect(unload).resolves.toEqual(secondAccepted);
    expect(fetch.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(2);
  });

  it("preserves the requested operation UUID across v2 cancellation acceptance", async () => {
    const wrong = await createProvenPeer([
      Response.json({ ...validV2OperationAccepted, operation_id: v2Ids.nextEvent }, { status: 202 }),
    ]);
    await expect(cancelV2Operation(wrong.peer, v2Ids.operation)).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });
});
