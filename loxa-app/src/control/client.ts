import {
  ControlContractError,
  decodeCapabilities,
  decodeControlError,
  decodeInventory,
  decodeNodeIdentityProof,
  decodeNodeSnapshot,
  decodeOperation,
  decodeOperationAccepted,
  type Capabilities,
  type ModelInventoryEntry,
  type NodeIdentityProof,
  type NodeSnapshot,
  type OperationAccepted,
  type OperationView,
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
export type ControlClientOptions = {
  fetch?: ControlFetch;
  timeoutMs?: number;
  signal?: AbortSignal;
};

const DEFAULT_TIMEOUT_MS = 5_000;
const MAX_JSON_BYTES = 2 * 1024 * 1024;
const TOKEN_PATTERN = /^[0-9a-f]{64}$/;
const MODEL_ID_PATTERN = /^[a-z0-9][a-z0-9._-]{0,127}$/;
const NONCE_PATTERN = /^[0-9a-f]{64}$/;

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
