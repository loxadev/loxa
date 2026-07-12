import {
  ContractError,
  decodeModelList,
  decodeNodeStatus,
  decodeOpenAIError,
  type ModelList,
  type NodeStatus,
  type OpenAIError,
} from "./contracts";

export type ClientErrorKind =
  | "transport"
  | "timeout"
  | "aborted"
  | "http"
  | "invalid-response";

export class NodeClientError extends Error {
  constructor(
    public readonly kind: ClientErrorKind,
    message: string,
    public readonly status?: number,
    public readonly openAI?: OpenAIError,
  ) {
    super(message);
    this.name = "NodeClientError";
  }
}

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export type ClientOptions = {
  fetch?: FetchLike;
  timeoutMs?: number;
  signal?: AbortSignal;
};

const DEFAULT_TIMEOUT_MS = 5_000;

function url(endpoint: string, path: string): string {
  return `${endpoint.replace(/\/$/, "")}${path}`;
}

async function parseJson(response: Response): Promise<unknown> {
  const text = await response.text();
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new NodeClientError(
      "invalid-response",
      "The Loxa node returned invalid JSON.",
    );
  }
}

async function openAIError(response: Response): Promise<OpenAIError | undefined> {
  try {
    return decodeOpenAIError(await parseJson(response));
  } catch (error) {
    if (error instanceof NodeClientError && error.kind === "invalid-response") {
      return undefined;
    }
    throw error;
  }
}

async function requestJson(
  endpoint: string,
  path: string,
  init: RequestInit,
  options: ClientOptions,
): Promise<unknown> {
  const controller = new AbortController();
  let abortCause: "caller" | "timeout" | null = null;
  const abortOnce = (cause: "caller" | "timeout") => {
    if (abortCause !== null) return;
    abortCause = cause;
    if (cause === "caller") clearTimeout(timeout);
    controller.abort();
  };
  const abortFromCaller = () => abortOnce("caller");
  if (options.signal?.aborted) {
    throw new NodeClientError("aborted", "The Loxa node request was cancelled.");
  }
  options.signal?.addEventListener("abort", abortFromCaller, { once: true });
  const timeout = setTimeout(
    () => abortOnce("timeout"),
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
  );
  try {
    const response = await (options.fetch ?? globalThis.fetch)(url(endpoint, path), {
      ...init,
      signal: controller.signal,
    });
    if (!response.ok) {
      const details = await openAIError(response);
      throw new NodeClientError(
        "http",
        details?.message ?? `The Loxa node returned HTTP ${response.status}.`,
        response.status,
        details,
      );
    }
    return await parseJson(response);
  } catch (error) {
    if (controller.signal.aborted) {
      if (abortCause === "timeout") {
        throw new NodeClientError("timeout", "The Loxa node request timed out.");
      }
      throw new NodeClientError("aborted", "The Loxa node request was cancelled.");
    }
    if (error instanceof NodeClientError) throw error;
    throw new NodeClientError("transport", "Could not connect to the Loxa node.");
  } finally {
    clearTimeout(timeout);
    options.signal?.removeEventListener("abort", abortFromCaller);
  }
}

export async function getStatus(
  endpoint: string,
  options: ClientOptions = {},
): Promise<NodeStatus> {
  const payload = await requestJson(endpoint, "/loxa/status", { method: "GET" }, options);
  try {
    return decodeNodeStatus(payload);
  } catch (error) {
    if (error instanceof NodeClientError) throw error;
    if (error instanceof ContractError) {
      throw new NodeClientError(
        "invalid-response",
        "The Loxa node returned an invalid status payload.",
      );
    }
    throw error;
  }
}

export async function getModels(
  endpoint: string,
  options: ClientOptions = {},
): Promise<ModelList> {
  const payload = await requestJson(endpoint, "/v1/models", { method: "GET" }, options);
  try {
    return decodeModelList(payload);
  } catch (error) {
    if (error instanceof NodeClientError) throw error;
    if (error instanceof ContractError) {
      throw new NodeClientError(
        "invalid-response",
        "The Loxa node returned an invalid model list.",
      );
    }
    throw error;
  }
}

export async function postChatCompletion<T = unknown>(
  endpoint: string,
  chatRequest: unknown,
  options: ClientOptions = {},
): Promise<T> {
  return (await requestJson(
    endpoint,
    "/v1/chat/completions",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(chatRequest),
    },
    options,
  )) as T;
}
