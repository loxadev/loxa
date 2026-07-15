import { SseDecodeError, SseDecoder } from "./sse";
import { emptyTurnMetrics, type ChatTurnMetrics } from "./turnMetrics";

export type HistoryClientErrorKind =
  "credential" | "endpoint" | "invalid-request" | "transport" | "timeout" | "aborted" | "http" | "invalid-response";

export class HistoryClientError extends Error {
  constructor(
    public readonly kind: HistoryClientErrorKind,
    message: string,
    public readonly status?: number,
    public readonly code?: string,
  ) {
    super(message);
    this.name = "HistoryClientError";
  }
}

export type HistoryFetch = (input: string, init?: RequestInit) => Promise<Response>;
export type HistoryClientOptions = {
  fetch?: HistoryFetch;
  timeoutMs?: number;
  signal?: AbortSignal;
};

export type ChatSummary = {
  id: string;
  title: string;
  createdAtMs: number;
  updatedAtMs: number;
};

export type ChatPage = { chats: ChatSummary[]; nextBefore: string | null };
export type TurnState = "queued" | "streaming" | "completed" | "cancelled" | "failed";
export type HistoryTurn = {
  id: string;
  chatId: string;
  ordinal: number;
  state: TurnState;
  modelAlias: "loxa";
  recipeId: string;
  engineName: string | null;
  engineVersion: string | null;
  errorCode: string | null;
  metrics: ChatTurnMetrics;
  createdAtMs: number;
  updatedAtMs: number;
};

export type TurnPage = { turns: HistoryTurn[]; nextAfter: string | null };
export type MessageRole = "user" | "assistant";
export type MessageSummary = {
  id: string;
  turnId: string;
  role: MessageRole;
  contentBytes: number;
  createdAtMs: number;
  updatedAtMs: number;
};
export type PersistentTurnTerminal =
  | { kind: "completed"; metrics: ChatTurnMetrics }
  | { kind: "cancelled"; metrics: ChatTurnMetrics }
  | { kind: "error"; message: string; metrics: ChatTurnMetrics };
export type PersistentTurnCallbacks = {
  onStarted(turnId: string, omittedTurns: number): void;
  onDelta(content: string): void;
  onTerminal(terminal: PersistentTurnTerminal): void;
};
export type PersistentTurnHandle = {
  cancel(): void;
  dispose(): void;
  finished: Promise<PersistentTurnTerminal>;
};
export type PageRequest = { limit?: number; before?: string };
export type TurnPageRequest = { limit?: number; after?: string };

const DEFAULT_TIMEOUT_MS = 5_000;
const MAX_JSON_BYTES = 1024 * 1024;
const TOKEN_PATTERN = /^[0-9a-f]{64}$/;
const ID_PATTERN = /^[0-9a-f]{32}$/;
const CURSOR_PATTERN = /^[A-Za-z0-9_-]{1,256}$/;
const ERROR_CODE_PATTERN = /^[a-z0-9_]{1,128}$/;
const MAX_TITLE_SCALARS = 160;
const MAX_DATE_MS = 8_640_000_000_000_000;
const MAX_MESSAGE_BYTES = 2 * 1024 * 1024;
const MAX_OMITTED_TURNS = 1_000_000;

export async function listChats(
  endpoint: string,
  token: string,
  page: PageRequest = {},
  options: HistoryClientOptions = {},
): Promise<ChatPage> {
  const query = pageQuery(page.limit, "before", page.before);
  return decodeChatPage(await request(endpoint, `/loxa/v1/chats${query}`, token, { method: "GET" }, options));
}

export async function createChat(
  endpoint: string,
  token: string,
  options: HistoryClientOptions = {},
): Promise<ChatSummary> {
  return decodeChat(await request(endpoint, "/loxa/v1/chats", token, { method: "POST" }, options));
}

export async function getChat(
  endpoint: string,
  token: string,
  chatId: string,
  options: HistoryClientOptions = {},
): Promise<ChatSummary> {
  assertId(chatId);
  return decodeChat(await request(endpoint, `/loxa/v1/chats/${chatId}`, token, { method: "GET" }, options));
}

export async function listTurns(
  endpoint: string,
  token: string,
  chatId: string,
  page: TurnPageRequest = {},
  options: HistoryClientOptions = {},
): Promise<TurnPage> {
  assertId(chatId);
  const query = pageQuery(page.limit, "after", page.after);
  return decodeTurnPage(
    await request(endpoint, `/loxa/v1/chats/${chatId}/turns${query}`, token, { method: "GET" }, options),
    chatId,
  );
}

export async function renameChat(
  endpoint: string,
  token: string,
  chatId: string,
  title: string,
  options: HistoryClientOptions = {},
): Promise<ChatSummary> {
  assertId(chatId);
  assertTitle(title);
  return decodeChat(
    await request(
      endpoint,
      `/loxa/v1/chats/${chatId}`,
      token,
      {
        method: "PATCH",
        body: JSON.stringify({ title }),
      },
      options,
    ),
  );
}

export async function deleteChat(
  endpoint: string,
  token: string,
  chatId: string,
  options: HistoryClientOptions = {},
): Promise<void> {
  assertId(chatId);
  await request(endpoint, `/loxa/v1/chats/${chatId}`, token, { method: "DELETE" }, options, true);
}

export async function clearChats(
  endpoint: string,
  token: string,
  options: HistoryClientOptions = {},
): Promise<{ deleted: number }> {
  return decodeClearResult(
    await request(
      endpoint,
      "/loxa/v1/chats/clear",
      token,
      {
        method: "POST",
        body: JSON.stringify({ confirm: "delete_all_chat_history" }),
      },
      options,
    ),
  );
}

export async function listMessageSummaries(
  endpoint: string,
  token: string,
  chatId: string,
  turnId: string,
  options: HistoryClientOptions = {},
): Promise<MessageSummary[]> {
  assertId(chatId);
  assertId(turnId);
  const value = await request(
    endpoint,
    `/loxa/v1/chats/${chatId}/turns/${turnId}/messages`,
    token,
    { method: "GET" },
    options,
  );
  if (!isRecord(value) || !hasExactKeys(value, ["messages"]) || !Array.isArray(value.messages)) throw invalidResponse();
  const messages = value.messages.map((entry) => decodeMessageSummary(entry, turnId));
  if (
    messages.length < 1 ||
    messages.length > 2 ||
    messages[0]?.role !== "user" ||
    (messages.length === 2 && messages[1]?.role !== "assistant") ||
    new Set(messages.map(({ id }) => id)).size !== messages.length
  )
    throw invalidResponse();
  return messages;
}

export async function getMessageContent(
  endpoint: string,
  token: string,
  chatId: string,
  turnId: string,
  messageId: string,
  options: HistoryClientOptions = {},
): Promise<string> {
  assertId(chatId);
  assertId(turnId);
  assertId(messageId);
  let segment = 0;
  let expectedCount: number | null = null;
  let content = "";
  let bytes = 0;
  while (true) {
    const value = await request(
      endpoint,
      `/loxa/v1/chats/${chatId}/turns/${turnId}/messages/${messageId}?segment=${segment}`,
      token,
      { method: "GET" },
      options,
    );
    const page = decodeMessagePage(value, messageId, turnId, segment, expectedCount);
    expectedCount = page.segmentCount;
    for (const part of page.contents) {
      bytes += new TextEncoder().encode(part).byteLength;
      if (bytes > MAX_MESSAGE_BYTES) throw invalidResponse();
      content += part;
    }
    if (page.nextSegment === null) return content;
    if (page.nextSegment <= segment) throw invalidResponse();
    segment = page.nextSegment;
  }
}

export function streamPersistentTurn(
  endpoint: string,
  token: string,
  chatId: string,
  content: string,
  callbacks: PersistentTurnCallbacks,
  signal?: AbortSignal,
  fetch: HistoryFetch = globalThis.fetch,
): PersistentTurnHandle {
  assertId(chatId);
  if (
    !TOKEN_PATTERN.test(token) ||
    content.trim() === "" ||
    content.includes("\0") ||
    new TextEncoder().encode(content).byteLength > MAX_MESSAGE_BYTES
  )
    invalidRequest();
  const controller = new AbortController();
  let disposed = false;
  let cancelRequested = false;
  let turnId: string | null = null;
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let notified = false;
  const callerAbort = () => {
    disposed = true;
    controller.abort();
    void reader?.cancel();
  };
  if (signal?.aborted) callerAbort();
  else signal?.addEventListener("abort", callerAbort, { once: true });
  const notify = (terminal: PersistentTurnTerminal) => {
    if (!disposed && !notified) {
      notified = true;
      callbacks.onTerminal(terminal);
    }
    return terminal;
  };
  const requestCancel = () => {
    if (turnId === null) return;
    void request(
      endpoint,
      `/loxa/v1/chats/${chatId}/turns/${turnId}/cancel`,
      token,
      {
        method: "POST",
        body: "",
      },
      { fetch, timeoutMs: DEFAULT_TIMEOUT_MS },
    ).catch(() => undefined);
  };
  const finished = (async (): Promise<PersistentTurnTerminal> => {
    if (disposed) return { kind: "cancelled", metrics: emptyTurnMetrics() };
    try {
      const response = await fetch(historyUrl(endpoint, `/loxa/v1/chats/${chatId}/turns`), {
        method: "POST",
        headers: { accept: "text/event-stream", authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ content, model: "loxa" }),
        signal: controller.signal,
      });
      if (!response.ok) throw await decodeHttpError(response);
      if (!response.body) throw invalidResponse();
      reader = response.body.getReader();
      const decoder = new SseDecoder();
      let terminal: PersistentTurnTerminal | null = null;
      while (true) {
        const result = await reader.read();
        const events = result.done ? decoder.finish() : decoder.push(result.value);
        for (const event of events) {
          if (terminal !== null) throw invalidResponse();
          const parsed = decodeTurnEvent(event.event, event.data, chatId, turnId);
          if (parsed.kind === "started") {
            if (turnId !== null) throw invalidResponse();
            turnId = parsed.turnId;
            callbacks.onStarted(turnId, parsed.omittedTurns);
            if (cancelRequested) requestCancel();
          } else if (parsed.kind === "delta") {
            callbacks.onDelta(parsed.content);
          } else {
            terminal = parsed.terminal;
          }
        }
        if (terminal !== null) return notify(terminal);
        if (result.done) throw invalidResponse();
      }
    } catch (error) {
      if (disposed || controller.signal.aborted) return { kind: "cancelled", metrics: emptyTurnMetrics() };
      if (error instanceof HistoryClientError && error.kind === "http")
        return notify({ kind: "error", message: error.message, metrics: emptyTurnMetrics() });
      return notify({
        kind: "error",
        message:
          error instanceof SseDecodeError
            ? "The Loxa node returned a malformed persistent chat stream."
            : "The persistent chat stream failed.",
        metrics: emptyTurnMetrics(),
      });
    } finally {
      signal?.removeEventListener("abort", callerAbort);
      try {
        await reader?.cancel();
      } catch {
        /* best effort */
      }
      reader?.releaseLock();
    }
  })();
  return {
    cancel: () => {
      if (cancelRequested || disposed) return;
      cancelRequested = true;
      requestCancel();
    },
    dispose: () => {
      if (disposed) return;
      disposed = true;
      controller.abort();
      void reader?.cancel();
    },
    finished,
  };
}

function pageQuery(limit: number | undefined, cursorName: "before" | "after", cursor: string | undefined): string {
  const actualLimit = limit ?? 30;
  if (!Number.isSafeInteger(actualLimit) || actualLimit < 1 || actualLimit > 100) invalidRequest();
  if (cursor !== undefined && !CURSOR_PATTERN.test(cursor)) invalidRequest();
  const query = new URLSearchParams({ limit: String(actualLimit) });
  if (cursor !== undefined) query.set(cursorName, cursor);
  return `?${query.toString()}`;
}

function assertId(value: string): void {
  if (!ID_PATTERN.test(value)) invalidRequest();
}

function assertTitle(value: string): void {
  if (value.trim().length === 0 || value.includes("\0") || [...value].length > MAX_TITLE_SCALARS) invalidRequest();
}

function invalidRequest(): never {
  throw new HistoryClientError("invalid-request", "The chat-history request is invalid.");
}

function historyUrl(endpoint: string, path: string): string {
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw new HistoryClientError("endpoint", "The Loxa node endpoint is invalid.");
  }
  const port = Number(parsed.port);
  if (
    parsed.protocol !== "http:" ||
    parsed.hostname !== "127.0.0.1" ||
    parsed.port === "" ||
    !Number.isSafeInteger(port) ||
    port < 1 ||
    port > 65_535 ||
    parsed.username !== "" ||
    parsed.password !== "" ||
    (parsed.pathname !== "" && parsed.pathname !== "/") ||
    parsed.search !== "" ||
    parsed.hash !== ""
  ) {
    throw new HistoryClientError("endpoint", "Chat history is restricted to an explicit IPv4 loopback endpoint.");
  }
  return `http://127.0.0.1:${port}${path}`;
}

async function request(
  endpoint: string,
  path: string,
  token: string,
  init: RequestInit,
  options: HistoryClientOptions,
  allowEmpty = false,
): Promise<unknown> {
  if (!TOKEN_PATTERN.test(token)) {
    throw new HistoryClientError("credential", "The local Loxa control credential is unavailable or unsafe.");
  }
  const url = historyUrl(endpoint, path);
  if (options.signal?.aborted) throw new HistoryClientError("aborted", "The chat-history request was cancelled.");

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

  try {
    const headers: Record<string, string> = {
      accept: "application/json",
      authorization: `Bearer ${token}`,
    };
    if (init.body !== undefined) headers["content-type"] = "application/json";
    const response = await (options.fetch ?? globalThis.fetch)(url, {
      ...init,
      headers: { ...headers, ...(init.headers ?? {}) },
      signal: controller.signal,
    });
    if (!response.ok) throw await decodeHttpError(response);
    if (allowEmpty && (response.status === 204 || response.body === null)) return undefined;
    return parseJson(await readBoundedText(response));
  } catch (error) {
    if (controller.signal.aborted) {
      throw new HistoryClientError(
        abortCause === "timeout" ? "timeout" : "aborted",
        abortCause === "timeout" ? "The chat-history request timed out." : "The chat-history request was cancelled.",
      );
    }
    if (error instanceof HistoryClientError) throw error;
    throw new HistoryClientError("transport", "Could not connect to Loxa chat history.");
  } finally {
    clearTimeout(timeout);
    options.signal?.removeEventListener("abort", callerAbort);
  }
}

async function decodeHttpError(response: Response): Promise<HistoryClientError> {
  try {
    const body = parseJson(await readBoundedText(response));
    const details = isRecord(body) && hasExactKeys(body, ["error"]) && isRecord(body.error) ? body.error : body;
    if (
      !isRecord(details) ||
      !hasExactKeys(details, ["code", "message"]) ||
      typeof details.code !== "string" ||
      !ERROR_CODE_PATTERN.test(details.code) ||
      typeof details.message !== "string" ||
      details.message.trim() === "" ||
      details.message.length > 512 ||
      details.message.includes("\0")
    ) {
      throw new Error("invalid error");
    }
    return new HistoryClientError("http", details.message, response.status, details.code);
  } catch {
    return new HistoryClientError("http", `The Loxa node returned HTTP ${response.status}.`, response.status);
  }
}

async function readBoundedText(response: Response): Promise<string> {
  const reader = response.body?.getReader();
  if (reader === undefined) return "";
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let bytes = 0;
  let output = "";
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) return output + decoder.decode();
      bytes += result.value.byteLength;
      if (bytes > MAX_JSON_BYTES) {
        await Promise.resolve(reader.cancel()).catch(() => undefined);
        throw invalidResponse();
      }
      output += decoder.decode(result.value, { stream: true });
    }
  } catch (error) {
    if (error instanceof HistoryClientError) throw error;
    throw invalidResponse();
  } finally {
    reader.releaseLock();
  }
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw invalidResponse();
  }
}

function invalidResponse(): HistoryClientError {
  return new HistoryClientError("invalid-response", "The Loxa node returned an invalid chat-history payload.");
}

function decodeChatPage(value: unknown): ChatPage {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ["chats", "next_before"]) ||
    !Array.isArray(value.chats) ||
    !nullableCursor(value.next_before)
  )
    throw invalidResponse();
  const chats = value.chats.map(decodeChat);
  if (new Set(chats.map((chat) => chat.id)).size !== chats.length) throw invalidResponse();
  for (let index = 1; index < chats.length; index += 1) {
    const previous = chats[index - 1];
    const current = chats[index];
    if (
      current.updatedAtMs > previous.updatedAtMs ||
      (current.updatedAtMs === previous.updatedAtMs && current.id >= previous.id)
    )
      throw invalidResponse();
  }
  return { chats, nextBefore: value.next_before };
}

function decodeChat(value: unknown): ChatSummary {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ["id", "title", "created_at_ms", "updated_at_ms"]) ||
    typeof value.id !== "string" ||
    !ID_PATTERN.test(value.id) ||
    typeof value.title !== "string" ||
    value.title.trim() === "" ||
    value.title.includes("\0") ||
    [...value.title].length > MAX_TITLE_SCALARS ||
    !isTimestamp(value.created_at_ms) ||
    !isTimestamp(value.updated_at_ms) ||
    value.updated_at_ms < value.created_at_ms
  )
    throw invalidResponse();
  return { id: value.id, title: value.title, createdAtMs: value.created_at_ms, updatedAtMs: value.updated_at_ms };
}

function decodeTurnPage(value: unknown, chatId: string): TurnPage {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ["turns", "next_after"]) ||
    !Array.isArray(value.turns) ||
    !nullableCursor(value.next_after)
  )
    throw invalidResponse();
  const turns = value.turns.map((turn) => decodeTurn(turn, chatId));
  if (new Set(turns.map((turn) => turn.id)).size !== turns.length) throw invalidResponse();
  for (let index = 1; index < turns.length; index += 1)
    if (turns[index].ordinal <= turns[index - 1].ordinal) throw invalidResponse();
  return { turns, nextAfter: value.next_after };
}

function decodeTurn(value: unknown, expectedChatId: string): HistoryTurn {
  const keys = [
    "id",
    "chat_id",
    "ordinal",
    "state",
    "provenance",
    "error_code",
    "metrics",
    "created_at_ms",
    "updated_at_ms",
  ];
  if (
    !isRecord(value) ||
    !hasExactKeys(value, keys) ||
    typeof value.id !== "string" ||
    !ID_PATTERN.test(value.id) ||
    value.chat_id !== expectedChatId ||
    !isTimestamp(value.ordinal) ||
    !isTurnState(value.state) ||
    !isRecord(value.provenance) ||
    !hasExactKeys(value.provenance, ["model_alias", "recipe_id", "engine_name", "engine_version"]) ||
    value.provenance.model_alias !== "loxa" ||
    !boundedString(value.provenance.recipe_id, 256, false) ||
    !nullableBoundedString(value.provenance.engine_name, 128) ||
    !nullableBoundedString(value.provenance.engine_version, 128) ||
    !(
      value.error_code === null ||
      (typeof value.error_code === "string" && ERROR_CODE_PATTERN.test(value.error_code))
    ) ||
    !isRecord(value.metrics) ||
    !isTimestamp(value.created_at_ms) ||
    !isTimestamp(value.updated_at_ms) ||
    value.updated_at_ms < value.created_at_ms
  )
    throw invalidResponse();
  return {
    id: value.id,
    chatId: value.chat_id,
    ordinal: value.ordinal,
    state: value.state,
    modelAlias: "loxa",
    recipeId: value.provenance.recipe_id,
    engineName: value.provenance.engine_name,
    engineVersion: value.provenance.engine_version,
    errorCode: value.error_code,
    metrics: decodeTurnMetrics(value.metrics),
    createdAtMs: value.created_at_ms,
    updatedAtMs: value.updated_at_ms,
  };
}

function decodeMessageSummary(value: unknown, expectedTurnId: string): MessageSummary {
  const keys = ["id", "turn_id", "role", "content_bytes", "created_at_ms", "updated_at_ms"];
  if (
    !isRecord(value) ||
    !hasExactKeys(value, keys) ||
    typeof value.id !== "string" ||
    !ID_PATTERN.test(value.id) ||
    value.turn_id !== expectedTurnId ||
    !isMessageRole(value.role) ||
    !isTimestamp(value.content_bytes) ||
    value.content_bytes > MAX_MESSAGE_BYTES ||
    !isTimestamp(value.created_at_ms) ||
    !isTimestamp(value.updated_at_ms) ||
    value.updated_at_ms < value.created_at_ms
  )
    throw invalidResponse();
  return {
    id: value.id,
    turnId: value.turn_id,
    role: value.role,
    contentBytes: value.content_bytes,
    createdAtMs: value.created_at_ms,
    updatedAtMs: value.updated_at_ms,
  };
}

function decodeMessagePage(
  value: unknown,
  messageId: string,
  turnId: string,
  startSegment: number,
  expectedCount: number | null,
): { segmentCount: number; contents: string[]; nextSegment: number | null } {
  const keys = ["message_id", "turn_id", "role", "segment_count", "segments", "next_segment"];
  if (
    !isRecord(value) ||
    !hasExactKeys(value, keys) ||
    value.message_id !== messageId ||
    value.turn_id !== turnId ||
    !isMessageRole(value.role) ||
    !isTimestamp(value.segment_count) ||
    value.segment_count < 1 ||
    (expectedCount !== null && value.segment_count !== expectedCount) ||
    !Array.isArray(value.segments) ||
    value.segments.length === 0 ||
    !(value.next_segment === null || (isTimestamp(value.next_segment) && value.next_segment < value.segment_count))
  )
    throw invalidResponse();
  let expectedIndex = startSegment;
  const contents = value.segments.map((segment) => {
    const segmentKeys = ["message_id", "turn_id", "role", "segment_index", "segment_count", "content"];
    if (
      !isRecord(segment) ||
      !hasExactKeys(segment, segmentKeys) ||
      segment.message_id !== messageId ||
      segment.turn_id !== turnId ||
      segment.role !== value.role ||
      segment.segment_count !== value.segment_count ||
      segment.segment_index !== expectedIndex ||
      typeof segment.content !== "string" ||
      segment.content.includes("\0")
    )
      throw invalidResponse();
    expectedIndex += 1;
    return segment.content;
  });
  if (value.next_segment !== null && value.next_segment !== expectedIndex) throw invalidResponse();
  if (value.next_segment === null && expectedIndex !== value.segment_count) throw invalidResponse();
  return { segmentCount: value.segment_count, contents, nextSegment: value.next_segment };
}

function decodeTurnEvent(
  event: string | undefined,
  data: string,
  expectedChatId: string,
  currentTurnId: string | null,
):
  | { kind: "started"; turnId: string; omittedTurns: number }
  | { kind: "delta"; content: string }
  | { kind: "terminal"; terminal: PersistentTurnTerminal } {
  const value = parseJson(data);
  if (!isRecord(value)) throw invalidResponse();
  if (event === "turn.started") {
    if (
      !hasExactKeys(value, ["chat_id", "turn_id", "state", "omitted_turns"]) ||
      value.chat_id !== expectedChatId ||
      typeof value.turn_id !== "string" ||
      !ID_PATTERN.test(value.turn_id) ||
      value.state !== "streaming" ||
      !Number.isSafeInteger(value.omitted_turns) ||
      (value.omitted_turns as number) < 0 ||
      (value.omitted_turns as number) > MAX_OMITTED_TURNS
    )
      throw invalidResponse();
    return { kind: "started", turnId: value.turn_id, omittedTurns: value.omitted_turns as number };
  }
  if (event === "turn.delta") {
    if (
      currentTurnId === null ||
      !hasExactKeys(value, ["turn_id", "content"]) ||
      value.turn_id !== currentTurnId ||
      typeof value.content !== "string" ||
      value.content.length === 0 ||
      value.content.includes("\0")
    )
      throw invalidResponse();
    return { kind: "delta", content: value.content };
  }
  if (event === "turn.completed" || event === "turn.cancelled" || event === "turn.failed") {
    if (
      currentTurnId === null ||
      !hasExactKeys(value, ["turn_id", "state", "error_code", "metrics"]) ||
      value.turn_id !== currentTurnId ||
      !(
        value.error_code === null ||
        (typeof value.error_code === "string" && ERROR_CODE_PATTERN.test(value.error_code))
      ) ||
      !isRecord(value.metrics)
    )
      throw invalidResponse();
    const metrics = decodeTurnMetrics(value.metrics);
    if (event === "turn.completed" && value.state === "completed" && value.error_code === null)
      return { kind: "terminal", terminal: { kind: "completed", metrics } };
    if (event === "turn.cancelled" && value.state === "cancelled" && value.error_code === null)
      return { kind: "terminal", terminal: { kind: "cancelled", metrics } };
    if (event === "turn.failed" && value.state === "failed")
      return {
        kind: "terminal",
        terminal: {
          kind: "error",
          message: value.error_code
            ? `Turn failed: ${value.error_code.replace(/_/g, " ")}.`
            : "The persistent turn failed.",
          metrics,
        },
      };
  }
  throw invalidResponse();
}

function decodeClearResult(value: unknown): { deleted: number } {
  if (!isRecord(value) || !hasExactKeys(value, ["deleted"]) || !isTimestamp(value.deleted)) throw invalidResponse();
  return { deleted: value.deleted };
}

function decodeTurnMetrics(value: Record<string, unknown>): ChatTurnMetrics {
  if (
    !hasExactKeys(value, ["output_tokens", "total_duration_ms", "ttft_ms", "stop_reason"]) ||
    !nullableSafeInteger(value.output_tokens) ||
    !nullableSafeInteger(value.total_duration_ms) ||
    !nullableSafeInteger(value.ttft_ms) ||
    !nullableBoundedString(value.stop_reason, 128) ||
    (value.ttft_ms !== null && value.total_duration_ms !== null && value.ttft_ms > value.total_duration_ms)
  ) {
    throw invalidResponse();
  }
  return {
    outputTokens: value.output_tokens,
    totalDurationMs: value.total_duration_ms,
    ttftMs: value.ttft_ms,
    stopReason: value.stop_reason,
  };
}

function nullableSafeInteger(value: unknown): value is number | null {
  return value === null || (Number.isSafeInteger(value) && (value as number) >= 0);
}

function nullableCursor(value: unknown): value is string | null {
  return value === null || (typeof value === "string" && CURSOR_PATTERN.test(value));
}

function isTimestamp(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= MAX_DATE_MS;
}

function boundedString(value: unknown, maxBytes: number, allowEmpty: boolean): value is string {
  return (
    typeof value === "string" &&
    !value.includes("\0") &&
    (allowEmpty || value.trim() !== "") &&
    new TextEncoder().encode(value).byteLength <= maxBytes
  );
}

function nullableBoundedString(value: unknown, maxBytes: number): value is string | null {
  return value === null || boundedString(value, maxBytes, false);
}

function isTurnState(value: unknown): value is TurnState {
  return (
    value === "queued" || value === "streaming" || value === "completed" || value === "cancelled" || value === "failed"
  );
}

function isMessageRole(value: unknown): value is MessageRole {
  return value === "user" || value === "assistant";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}
