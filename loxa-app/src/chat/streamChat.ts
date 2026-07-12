import { decodeOpenAIError } from "../node/contracts";
import { SseDecodeError, SseDecoder } from "./sse";

export type StreamTerminal =
  | { kind: "completed" }
  | { kind: "cancelled" }
  | { kind: "error"; message: string };

export type StreamCallbacks = {
  onDelta(text: string): void;
  onTerminal(result: StreamTerminal): void;
};

export type StreamHandle = {
  cancel(): void;
  dispose(): void;
  finished: Promise<StreamTerminal>;
};

export type StreamFetch = (input: string, init?: RequestInit) => Promise<Response>;

export function streamChat(
  endpoint: string,
  request: unknown,
  callbacks: StreamCallbacks,
  signal?: AbortSignal,
  fetch: StreamFetch = globalThis.fetch,
): StreamHandle {
  const controller = new AbortController();
  let abortCause: "caller" | "dispose" | null = null;
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let readerCancelled = false;
  let terminalNotified = false;

  const cancelReader = async () => {
    if (!reader || readerCancelled) return;
    readerCancelled = true;
    await Promise.resolve(reader.cancel()).catch(() => undefined);
  };

  const abortOnce = (cause: "caller" | "dispose") => {
    if (abortCause !== null) return;
    abortCause = cause;
    controller.abort();
    void cancelReader();
  };
  const abortFromCaller = () => abortOnce("caller");
  if (signal?.aborted) abortOnce("caller");
  else signal?.addEventListener("abort", abortFromCaller, { once: true });

  const notifyTerminal = (terminal: StreamTerminal): StreamTerminal => {
    if (terminalNotified) return terminal;
    terminalNotified = true;
    if (abortCause !== "dispose") {
      try {
        callbacks.onTerminal(terminal);
      } catch {
        // Consumer callback failures must not create a second terminal result.
      }
    }
    return terminal;
  };
  const settle = (terminal: StreamTerminal): StreamTerminal =>
    notifyTerminal(abortCause === null ? terminal : { kind: "cancelled" });

  const finished = (async (): Promise<StreamTerminal> => {
    if (abortCause !== null) {
      return settle({ kind: "cancelled" });
    }
    try {
      const response = await fetch(
        `${endpoint.replace(/\/$/, "")}/v1/chat/completions`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            ...(isRecord(request) ? request : {}),
            stream: true,
          }),
          signal: controller.signal,
        },
      );
      if (!response.ok) {
        if (abortCause !== null) {
          return settle({ kind: "cancelled" });
        }
        const message = await httpErrorMessage(response);
        if (abortCause !== null) {
          return settle({ kind: "cancelled" });
        }
        return settle({
          kind: "error",
          message,
        });
      }
      if (!response.body) {
        return notifyTerminal({
          kind: "error",
          message: "The Loxa node returned a stream without a response body.",
        });
      }

      reader = response.body.getReader();
      if (abortCause !== null) {
        await cancelReader();
        return settle({ kind: "cancelled" });
      }
      const decoder = new SseDecoder();
      while (true) {
        const result = await reader.read();
        if (abortCause !== null) {
          return settle({ kind: "cancelled" });
        }
        if (result.done) {
          for (const event of decoder.finish()) {
            const terminal = consumeEvent(event.data, callbacks, () => abortCause !== null);
            if (terminal) return settle(terminal);
          }
          await cancelReader();
          return settle({
            kind: "error",
            message: "The chat stream ended before [DONE].",
          });
        }
        for (const event of decoder.push(result.value)) {
          const terminal = consumeEvent(event.data, callbacks, () => abortCause !== null);
          if (terminal) {
            if (terminal.kind === "completed") await cancelReader();
            return settle(terminal);
          }
        }
      }
    } catch (error) {
      if (abortCause !== null || controller.signal.aborted) {
        return settle({ kind: "cancelled" });
      }
      await cancelReader();
      return settle({
        kind: "error",
        message:
          error instanceof SseDecodeError || error instanceof SyntaxError
            ? "The Loxa node returned a malformed chat stream."
            : reader
              ? "The chat stream failed while reading."
              : "Could not connect to the Loxa node.",
      });
    } finally {
      signal?.removeEventListener("abort", abortFromCaller);
      reader?.releaseLock();
    }
  })();

  return {
    cancel: () => abortOnce("caller"),
    dispose: () => abortOnce("dispose"),
    finished,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function httpErrorMessage(response: Response): Promise<string> {
  try {
    const details = decodeOpenAIError(JSON.parse(await response.text()) as unknown);
    return details.message;
  } catch {
    return `The Loxa node returned HTTP ${response.status}.`;
  }
}

function consumeEvent(
  data: string,
  callbacks: StreamCallbacks,
  isAborted: () => boolean,
): StreamTerminal | null {
  if (isAborted()) return { kind: "cancelled" };
  if (data.trim() === "[DONE]") return { kind: "completed" };
  const payload = JSON.parse(data) as unknown;
  if (!isRecord(payload) || !Array.isArray(payload.choices)) {
    throw new SyntaxError("invalid OpenAI stream chunk");
  }
  for (const choice of payload.choices) {
    if (isAborted()) return { kind: "cancelled" };
    if (!isRecord(choice) || !isRecord(choice.delta)) continue;
    const content = choice.delta.content;
    if (typeof content === "string" && content.length > 0) {
      if (isAborted()) return { kind: "cancelled" };
      callbacks.onDelta(content);
      if (isAborted()) return { kind: "cancelled" };
    }
  }
  return null;
}
