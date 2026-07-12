import { SseDecodeError, SseDecoder } from "../chat/sse";
import { assertControlToken, controlUrl, type ControlFetch } from "./client";
import {
  ControlContractError,
  decodeControlEvent,
  decodeReconnectSnapshot,
  type ControlEvent,
  type ReconnectSnapshot,
} from "./contracts";

export type ControlStreamTerminal =
  | { kind: "cancelled"; cursor: number }
  | { kind: "error"; cursor: number; message: string };

export type ControlStreamCallbacks = {
  onSnapshot(snapshot: ReconnectSnapshot): void;
  onEvent(event: ControlEvent): void;
  onTerminal(terminal: ControlStreamTerminal): void;
};

export type ControlStreamHandle = {
  dispose(): void;
  cancel(): void;
  finished: Promise<ControlStreamTerminal>;
};

const EVENT_CONNECT_TIMEOUT_MS = 5_000;

export function streamControlEvents(
  endpoint: string,
  token: string,
  initialCursor: number,
  callbacks: ControlStreamCallbacks,
  signal?: AbortSignal,
  fetch: ControlFetch = globalThis.fetch,
): ControlStreamHandle {
  assertControlToken(token);
  if (!Number.isSafeInteger(initialCursor) || initialCursor < 0) {
    throw new ControlContractError("event cursor");
  }
  const url = controlUrl(endpoint, `/loxa/v1/events?cursor=${initialCursor}`);
  const controller = new AbortController();
  let cursor = initialCursor;
  let abortCause: "cancel" | "dispose" | "timeout" | null = null;
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let readerCancelled = false;
  let terminalNotified = false;

  const cancelReader = async () => {
    if (reader === null || readerCancelled) return;
    readerCancelled = true;
    await Promise.resolve(reader.cancel()).catch(() => undefined);
  };
  const abort = (cause: "cancel" | "dispose" | "timeout") => {
    if (abortCause !== null) return;
    abortCause = cause;
    controller.abort();
    void cancelReader();
  };
  const callerAbort = () => abort("cancel");
  if (signal?.aborted) abort("cancel");
  else signal?.addEventListener("abort", callerAbort, { once: true });

  const terminal = (value: ControlStreamTerminal): ControlStreamTerminal => {
    if (terminalNotified) return value;
    terminalNotified = true;
    if (abortCause !== "dispose") {
      try {
        callbacks.onTerminal(value);
      } catch {
        // A consumer exception cannot create a second terminal callback.
      }
    }
    return value;
  };
  const cancelled = () => terminal({ kind: "cancelled", cursor });
  const failed = (message: string) => terminal({ kind: "error", cursor, message });
  const connectTimeout = setTimeout(() => abort("timeout"), EVENT_CONNECT_TIMEOUT_MS);

  const finished = (async (): Promise<ControlStreamTerminal> => {
    if (abortCause !== null) return cancelled();
    let sawSnapshot = false;
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          accept: "text/event-stream",
          authorization: `Bearer ${token}`,
        },
        signal: controller.signal,
      });
      clearTimeout(connectTimeout);
      if (abortCause === "timeout") return failed("Connecting to live model updates timed out.");
      if (abortCause !== null) return cancelled();
      if (!response.ok) return failed(`The Loxa event service returned HTTP ${response.status}.`);
      if (!response.body) return failed("The Loxa event service returned no response body.");
      reader = response.body.getReader();
      if (abortCause !== null) {
        await cancelReader();
        return cancelled();
      }
      const decoder = new SseDecoder();
      while (true) {
        const result = await reader.read();
        if (abortCause !== null) return cancelled();
        if (result.done) {
          if (decoder.hasPendingFrame()) {
            throw new ControlContractError("unterminated final control event");
          }
          if (decoder.finish().length > 0) {
            throw new ControlContractError("unterminated final control event");
          }
          await cancelReader();
          return failed("Live model updates disconnected.");
        }
        for (const frame of decoder.push(result.value)) {
          if (abortCause !== null) return cancelled();
          const payload = JSON.parse(frame.data) as unknown;
          if (frame.event === "snapshot") {
            if (sawSnapshot) throw new ControlContractError("duplicate reconnect snapshot");
            const snapshot = decodeReconnectSnapshot(payload);
            if (snapshot.cursor < cursor) throw new ControlContractError("regressing reconnect snapshot");
            cursor = snapshot.cursor;
            sawSnapshot = true;
            callbacks.onSnapshot(snapshot);
          } else if (frame.event === "operation") {
            if (!sawSnapshot) throw new ControlContractError("event before reconnect snapshot");
            const event = decodeControlEvent(payload);
            if (event.sequence <= cursor || frame.id !== String(event.sequence)) {
              throw new ControlContractError("regressing control event");
            }
            cursor = event.sequence;
            callbacks.onEvent(event);
          } else {
            throw new ControlContractError("control event type");
          }
        }
      }
    } catch (error) {
      if (abortCause === "timeout") return failed("Connecting to live model updates timed out.");
      if (abortCause !== null || controller.signal.aborted) return cancelled();
      await cancelReader();
      if (error instanceof ControlContractError || error instanceof SseDecodeError || error instanceof SyntaxError) {
        return failed("The Loxa node returned an invalid model update stream.");
      }
      return failed(reader === null
        ? "Could not connect to live model updates."
        : "Live model updates failed while reading.");
    } finally {
      clearTimeout(connectTimeout);
      signal?.removeEventListener("abort", callerAbort);
      reader?.releaseLock();
    }
  })();

  return {
    cancel: () => abort("cancel"),
    dispose: () => abort("dispose"),
    finished,
  };
}
