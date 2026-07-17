import {
  ControlContractError,
  decodeCapabilities,
  decodeControlError,
  decodeInventory,
  decodeNodeIdentityProof,
  decodeNodeSnapshot,
  decodeOperation,
  decodeOperationAccepted,
  decodeV2ControlErrorJson,
  decodeV2NodeCollectionJson,
  decodeV2OperationCollectionJson,
  decodeV2OperationEnvelopeJson,
  decodeV2SlotCollectionJson,
  type Capabilities,
  type ModelInventoryEntry,
  type NodeIdentityProof,
  type NodeSnapshot,
  type OperationAccepted,
  type OperationView,
  type V2NodeCollection,
  type V2OperationCollection,
  type V2OperationEnvelope,
  type V2SlotCollection,
} from "./contracts";

export type ControlClientErrorKind =
  "credential" | "endpoint" | "transport" | "timeout" | "aborted" | "http" | "invalid-response";

export class ControlClientError extends Error {
  constructor(
    public readonly kind: ControlClientErrorKind,
    message: string,
    public readonly status?: number,
    public readonly code?: string,
  ) {
    super(message);
    this.name = "ControlClientError";
  }
}

export type ControlFetch = (input: string, init?: RequestInit) => Promise<Response>;
declare const provenControlPeerBrand: unique symbol;
export type ProvenControlPeer = { readonly [provenControlPeerBrand]: true };
export type ControlClientOptions = {
  fetch?: ControlFetch;
  timeoutMs?: number;
  signal?: AbortSignal;
};

const DEFAULT_TIMEOUT_MS = 5_000;
const MAX_JSON_BYTES = 2 * 1024 * 1024;
const MAX_V2_ERROR_BYTES = 16 * 1024;
const TOKEN_PATTERN = /^[0-9a-f]{64}$/;
const MODEL_ID_PATTERN = /^[a-z0-9][a-z0-9._-]{0,127}$/;
const NONCE_PATTERN = /^[0-9a-f]{64}$/;
const V2_UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

type ProvenPeerAuthority = {
  endpoint: string;
  token: string;
  nodeId: string;
  nodeInstanceId: string;
  fetch: ControlFetch;
  timeoutMs: number;
  signal?: AbortSignal;
};
const provenPeerAuthorities = new WeakMap<object, ProvenPeerAuthority>();

export function assertControlToken(token: string): void {
  if (!TOKEN_PATTERN.test(token)) {
    throw new ControlClientError("credential", "The local Loxa control credential is unavailable or unsafe.");
  }
}

export function controlUrl(endpoint: string, path: string): string {
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw new ControlClientError("endpoint", "The Loxa node endpoint is invalid.");
  }
  if (
    parsed.protocol !== "http:" ||
    parsed.hostname !== "127.0.0.1" ||
    parsed.port === "" ||
    parsed.username !== "" ||
    parsed.password !== "" ||
    (parsed.pathname !== "" && parsed.pathname !== "/") ||
    parsed.search !== "" ||
    parsed.hash !== ""
  ) {
    throw new ControlClientError("endpoint", "Control requests are restricted to an explicit IPv4 loopback endpoint.");
  }
  const port = Number(parsed.port);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new ControlClientError("endpoint", "The Loxa node endpoint port is invalid.");
  }
  return `http://127.0.0.1:${port}${path}`;
}

async function parseJson(response: Response): Promise<unknown> {
  const text = await readBoundedText(response);
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new ControlClientError("invalid-response", "The Loxa node returned invalid control JSON.");
  }
}

async function readBoundedText(response: Response): Promise<string> {
  const reader = response.body?.getReader();
  if (reader === undefined) return "";
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      total += result.value.byteLength;
      if (total > MAX_JSON_BYTES) {
        await Promise.resolve(reader.cancel()).catch(() => undefined);
        throw new ControlClientError("invalid-response", "The Loxa node returned an oversized control response.");
      }
      chunks.push(result.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

async function readBoundedBytes(response: Response, maxBytes: number): Promise<Uint8Array> {
  const reader = response.body?.getReader();
  if (reader === undefined) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      total += result.value.byteLength;
      if (total > maxBytes) {
        await Promise.resolve(reader.cancel()).catch(() => undefined);
        throw new ControlClientError("invalid-response", "The Loxa node returned an oversized control response.");
      }
      chunks.push(result.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

async function httpError(response: Response): Promise<ControlClientError> {
  try {
    const body = decodeControlError(await parseJson(response));
    return new ControlClientError("http", body.message, response.status, body.code);
  } catch (error) {
    if (error instanceof ControlClientError && error.kind !== "invalid-response") return error;
    return new ControlClientError("http", `The Loxa node returned HTTP ${response.status}.`, response.status);
  }
}

async function request(
  endpoint: string,
  path: string,
  token: string | null,
  init: RequestInit,
  options: ControlClientOptions,
): Promise<unknown> {
  if (token !== null) assertControlToken(token);
  const requestUrl = controlUrl(endpoint, path);
  if (options.signal?.aborted) {
    throw new ControlClientError("aborted", "The Loxa control request was cancelled.");
  }
  const controller = new AbortController();
  let abortCause: "caller" | "timeout" | null = null;
  const abort = (cause: "caller" | "timeout") => {
    if (abortCause !== null) return;
    abortCause = cause;
    controller.abort();
  };
  const callerAbort = () => abort("caller");
  options.signal?.addEventListener("abort", callerAbort, { once: true });
  const timeout = setTimeout(() => abort("timeout"), options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  const headers: Record<string, string> = { accept: "application/json" };
  if (token !== null) headers.authorization = `Bearer ${token}`;
  if (init.body !== undefined) headers["content-type"] = "application/json";
  try {
    const response = await (options.fetch ?? globalThis.fetch)(requestUrl, {
      ...init,
      headers: { ...headers, ...(init.headers ?? {}) },
      signal: controller.signal,
    });
    if (!response.ok) throw await httpError(response);
    return await parseJson(response);
  } catch (error) {
    if (controller.signal.aborted) {
      if (abortCause === "timeout") {
        throw new ControlClientError("timeout", "The Loxa control request timed out.");
      }
      throw new ControlClientError("aborted", "The Loxa control request was cancelled.");
    }
    if (error instanceof ControlClientError) throw error;
    throw new ControlClientError("transport", "Could not connect to the Loxa control service.");
  } finally {
    clearTimeout(timeout);
    options.signal?.removeEventListener("abort", callerAbort);
  }
}

function decode<T>(contract: () => T): T {
  try {
    return contract();
  } catch (error) {
    if (error instanceof ControlContractError) {
      throw new ControlClientError("invalid-response", "The Loxa node returned an invalid control payload.");
    }
    throw error;
  }
}

function bytesFromHex(value: string): Uint8Array {
  return Uint8Array.from(value.match(/../g) ?? [], (byte) => Number.parseInt(byte, 16));
}

function lengthPrefixed(value: string): Uint8Array {
  const bytes = new TextEncoder().encode(value);
  const prefixed = new Uint8Array(4 + bytes.byteLength);
  new DataView(prefixed.buffer).setUint32(0, bytes.byteLength, false);
  prefixed.set(bytes, 4);
  return prefixed;
}

async function verifyIdentityProof(token: string, nonce: string, proof: NodeIdentityProof): Promise<boolean> {
  const status = ["unloaded", "loading", "ready", "unloading", "recovery_required", "error"].indexOf(proof.status);
  if (status < 0 || status > 3) return false;
  const parts = [
    new TextEncoder().encode("loxa-control-node-identity-v1\0"),
    Uint8Array.of(0, 0, 0, 1),
    bytesFromHex(nonce),
    lengthPrefixed(proof.nodeId),
    lengthPrefixed(proof.runtimeIdentity),
    Uint8Array.of(status),
  ];
  const message = new Uint8Array(parts.reduce((length, part) => length + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    message.set(part, offset);
    offset += part.length;
  }
  try {
    const keyBytes = Uint8Array.from(bytesFromHex(token));
    const key = await crypto.subtle.importKey("raw", keyBytes.buffer, { name: "HMAC", hash: "SHA-256" }, false, [
      "sign",
    ]);
    const expected = new Uint8Array(await crypto.subtle.sign("HMAC", key, Uint8Array.from(message).buffer));
    const supplied = bytesFromHex(proof.challengeProof);
    if (expected.length !== supplied.length) return false;
    let difference = 0;
    for (let index = 0; index < expected.length; index += 1) difference |= expected[index]! ^ supplied[index]!;
    return difference === 0;
  } catch {
    return false;
  }
}

function peerAuthority(peer: ProvenControlPeer): ProvenPeerAuthority {
  const authority = provenPeerAuthorities.get(peer as object);
  if (!authority) throw new ControlClientError("credential", "The proven Loxa control peer is unavailable.");
  return authority;
}

export function assertProvenControlIdentity(peer: ProvenControlPeer, nodeId: string, nodeInstanceId: string): void {
  const authority = peerAuthority(peer);
  if (nodeId !== authority.nodeId || nodeInstanceId !== authority.nodeInstanceId) {
    throw new ControlClientError("invalid-response", "The proved Loxa node instance changed.");
  }
}

export async function fetchFromProvenControlPeer(
  peer: ProvenControlPeer,
  path: string,
  init: RequestInit,
): Promise<Response> {
  const authority = peerAuthority(peer);
  const controller = new AbortController();
  let timedOut = false;
  const abort = () => controller.abort();
  if (authority.signal?.aborted || init.signal?.aborted) {
    throw new ControlClientError("aborted", "The proven Loxa control request was cancelled.");
  }
  authority.signal?.addEventListener("abort", abort, { once: true });
  init.signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, authority.timeoutMs);
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${authority.token}`);
  try {
    return await authority.fetch(controlUrl(authority.endpoint, path), { ...init, headers, signal: controller.signal });
  } catch (error) {
    if (controller.signal.aborted) {
      throw new ControlClientError(
        timedOut ? "timeout" : "aborted",
        timedOut ? "The proven Loxa control request timed out." : "The proven Loxa control request was cancelled.",
      );
    }
    if (error instanceof ControlClientError) throw error;
    throw new ControlClientError("transport", "Could not connect to the proven Loxa control service.");
  } finally {
    clearTimeout(timeout);
    authority.signal?.removeEventListener("abort", abort);
    init.signal?.removeEventListener("abort", abort);
  }
}

export async function proveV2ControlPeer(
  endpoint: string,
  token: string,
  options: ControlClientOptions = {},
): Promise<ProvenControlPeer> {
  assertControlToken(token);
  controlUrl(endpoint, "");
  const nonce = freshNonce();
  const proof = await getNodeIdentityProof(endpoint, nonce, options);
  if (!(await verifyIdentityProof(token, nonce, proof))) {
    throw new ControlClientError("credential", "The Loxa control peer identity proof failed.");
  }
  const peer = Object.freeze({}) as ProvenControlPeer;
  provenPeerAuthorities.set(peer as object, {
    endpoint,
    token,
    nodeId: proof.nodeId,
    nodeInstanceId: proof.runtimeIdentity,
    fetch: options.fetch ?? globalThis.fetch,
    timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });
  try {
    await fetchV2NodeCollection(peer);
    return peer;
  } catch (error) {
    provenPeerAuthorities.delete(peer as object);
    throw error;
  }
}

function freshNonce(): string {
  const nonceBytes = new Uint8Array(32);
  crypto.getRandomValues(nonceBytes);
  return [...nonceBytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function reproveExactPeer(peer: ProvenControlPeer): Promise<void> {
  const authority = peerAuthority(peer);
  const nonce = freshNonce();
  const proof = await getNodeIdentityProof(authority.endpoint, nonce, {
    fetch: authority.fetch,
    timeoutMs: authority.timeoutMs,
    ...(authority.signal === undefined ? {} : { signal: authority.signal }),
  });
  if (
    proof.nodeId !== authority.nodeId ||
    proof.runtimeIdentity !== authority.nodeInstanceId ||
    !(await verifyIdentityProof(authority.token, nonce, proof))
  ) {
    throw new ControlClientError("credential", "The proved Loxa node instance was replaced.");
  }
}

export async function getNodeIdentityProof(
  endpoint: string,
  nonce: string,
  options: ControlClientOptions = {},
): Promise<NodeIdentityProof> {
  if (!NONCE_PATTERN.test(nonce)) {
    throw new ControlClientError("credential", "The node identity challenge is invalid.");
  }
  const payload = await request(
    endpoint,
    "/loxa/v1/node",
    null,
    {
      method: "GET",
      headers: { "x-loxa-challenge": nonce },
    },
    options,
  );
  return decode(() => decodeNodeIdentityProof(payload));
}

export async function getControlNode(
  endpoint: string,
  token: string,
  options: ControlClientOptions = {},
): Promise<NodeSnapshot> {
  const payload = await request(endpoint, "/loxa/v1/node", token, { method: "GET" }, options);
  return decode(() => decodeNodeSnapshot(payload));
}

export async function getCapabilities(
  endpoint: string,
  token: string,
  options: ControlClientOptions = {},
): Promise<Capabilities> {
  const payload = await request(endpoint, "/loxa/v1/capabilities", token, { method: "GET" }, options);
  return decode(() => decodeCapabilities(payload));
}

export async function getInventory(
  endpoint: string,
  token: string,
  options: ControlClientOptions = {},
): Promise<ModelInventoryEntry[]> {
  const payload = await request(endpoint, "/loxa/v1/models", token, { method: "GET" }, options);
  return decode(() => decodeInventory(payload));
}

export async function downloadModel(
  endpoint: string,
  token: string,
  modelId: string,
  options: ControlClientOptions = {},
): Promise<OperationAccepted> {
  if (!MODEL_ID_PATTERN.test(modelId)) {
    throw new ControlClientError("invalid-response", "The selected registry model ID is invalid.");
  }
  const payload = await request(
    endpoint,
    "/loxa/v1/models/download",
    token,
    {
      method: "POST",
      body: JSON.stringify({ model_id: modelId }),
    },
    options,
  );
  return decode(() => decodeOperationAccepted(payload));
}

export async function loadModel(
  endpoint: string,
  token: string,
  modelId: string,
  options: ControlClientOptions = {},
): Promise<OperationAccepted> {
  if (!MODEL_ID_PATTERN.test(modelId))
    throw new ControlClientError("invalid-response", "The selected registry model ID is invalid.");
  const payload = await request(
    endpoint,
    "/loxa/v1/models/load",
    token,
    {
      method: "POST",
      body: JSON.stringify({ model_id: modelId }),
    },
    options,
  );
  return decode(() => decodeOperationAccepted(payload));
}

export async function unloadModel(
  endpoint: string,
  token: string,
  options: ControlClientOptions = {},
): Promise<OperationAccepted> {
  const payload = await request(endpoint, "/loxa/v1/models/unload", token, { method: "POST" }, options);
  return decode(() => decodeOperationAccepted(payload));
}

export async function getOperation(
  endpoint: string,
  token: string,
  operationId: string,
  options: ControlClientOptions = {},
): Promise<OperationView> {
  const payload = await request(
    endpoint,
    `/loxa/v1/operations/${encodeURIComponent(operationId)}`,
    token,
    { method: "GET" },
    options,
  );
  return decode(() => decodeOperation(payload));
}

export async function cancelOperation(
  endpoint: string,
  token: string,
  operationId: string,
  options: ControlClientOptions = {},
): Promise<OperationView> {
  const payload = await request(
    endpoint,
    `/loxa/v1/operations/${encodeURIComponent(operationId)}/cancel`,
    token,
    { method: "POST" },
    options,
  );
  return decode(() => decodeOperation(payload));
}

async function fetchV2<T>(peer: ProvenControlPeer, path: string, contract: (body: Uint8Array) => T): Promise<T> {
  const response = await fetchFromProvenControlPeer(peer, path, {
    method: "GET",
    headers: { accept: "application/json" },
  });
  if (!response.ok) throw await v2ControlHttpError(response);
  const body = await readBoundedBytes(response, MAX_JSON_BYTES);
  if (response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    throw new ControlClientError("invalid-response", "The Loxa node returned an invalid control media type.");
  }
  return decode(() => contract(body));
}

export async function v2ControlHttpError(response: Response): Promise<ControlClientError> {
  const body = await readBoundedBytes(response, MAX_V2_ERROR_BYTES);
  if (response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    return new ControlClientError("http", `The Loxa node returned HTTP ${response.status}.`, response.status);
  }
  try {
    const error = decodeV2ControlErrorJson(body);
    return new ControlClientError("http", error.message, response.status, error.code);
  } catch {
    return new ControlClientError("http", `The Loxa node returned HTTP ${response.status}.`, response.status);
  }
}

function assertV2RouteId(value: string): void {
  if (!V2_UUID_PATTERN.test(value)) {
    throw new ControlClientError("invalid-response", "The Loxa v2 control identifier is invalid.");
  }
}

export function fetchV2NodeCollection(peer: ProvenControlPeer): Promise<V2NodeCollection> {
  return fetchV2(peer, "/loxa/v2/nodes", (body) => {
    const collection = decodeV2NodeCollectionJson(body);
    const authority = peerAuthority(peer);
    const node = collection.nodes[0];
    if (node?.node_id !== authority.nodeId || node.node_instance_id !== authority.nodeInstanceId) {
      throw new ControlContractError("proved v2 node identity");
    }
    return collection;
  });
}

export async function fetchV2SlotCollection(peer: ProvenControlPeer, nodeId: string): Promise<V2SlotCollection> {
  assertV2RouteId(nodeId);
  const authority = peerAuthority(peer);
  if (nodeId !== authority.nodeId) throw new ControlClientError("invalid-response", "The proved node ID changed.");
  const collection = await fetchV2(peer, `/loxa/v2/nodes/${nodeId}/slots`, (body) => {
    const collection = decodeV2SlotCollectionJson(body);
    if (collection.node_id !== authority.nodeId) throw new ControlContractError("proved v2 slot owner");
    return collection;
  });
  await reproveExactPeer(peer);
  return collection;
}

export async function fetchV2OperationCollection(peer: ProvenControlPeer): Promise<V2OperationCollection> {
  const collection = await fetchV2(peer, "/loxa/v2/operations", (body) => {
    const collection = decodeV2OperationCollectionJson(body);
    const nodeId = peerAuthority(peer).nodeId;
    if (collection.operations.some((operation) => operation.node_id !== nodeId)) {
      throw new ControlContractError("proved v2 operation owner");
    }
    return collection;
  });
  await reproveExactPeer(peer);
  return collection;
}

export async function fetchV2OperationEnvelope(
  peer: ProvenControlPeer,
  operationId: string,
): Promise<V2OperationEnvelope> {
  assertV2RouteId(operationId);
  const envelope = await fetchV2(peer, `/loxa/v2/operations/${operationId}`, (body) => {
    const envelope = decodeV2OperationEnvelopeJson(body);
    if (envelope.operation.operation_id !== operationId || envelope.operation.node_id !== peerAuthority(peer).nodeId) {
      throw new ControlContractError("proved v2 operation identity");
    }
    return envelope;
  });
  await reproveExactPeer(peer);
  return envelope;
}
