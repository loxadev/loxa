export const MAX_SSE_EVENT_BYTES = 2 * 1024 * 1024;

export class SseDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SseDecodeError";
  }
}

export type SseEvent = { data: string; event?: string; id?: string };

export class SseDecoder {
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });
  private buffer = "";
  private finished = false;

  hasPendingFrame(): boolean {
    return this.buffer.length > 0;
  }

  push(chunk: Uint8Array): SseEvent[] {
    if (this.finished) {
      throw new SseDecodeError("The SSE decoder is already finished.");
    }
    try {
      this.buffer += this.decoder.decode(chunk, { stream: true });
    } catch {
      throw new SseDecodeError("The SSE stream contains invalid UTF-8.");
    }
    return this.drain(false);
  }

  finish(): SseEvent[] {
    if (this.finished) return [];
    this.finished = true;
    try {
      this.buffer += this.decoder.decode();
    } catch {
      throw new SseDecodeError("The SSE stream contains invalid UTF-8.");
    }
    const events = this.drain(true);
    this.buffer = "";
    return events;
  }

  private drain(final: boolean): SseEvent[] {
    const events: SseEvent[] = [];
    let boundary = this.nextBoundary(final);
    while (boundary !== null) {
      const raw = this.buffer.slice(0, boundary.index);
      this.assertWithinLimit(raw);
      this.buffer = this.buffer.slice(boundary.index + boundary.length);
      const event = this.parse(raw);
      if (event) events.push(event);
      boundary = this.nextBoundary(final);
    }
    this.assertWithinLimit(this.buffer);
    return events;
  }

  private nextBoundary(final: boolean): { index: number; length: number } | null {
    let index = 0;
    while (index < this.buffer.length) {
      const first = this.lineEndingLength(index, final);
      if (first === null) return null;
      if (first === 0) {
        index += 1;
        continue;
      }
      const second = this.lineEndingLength(index + first, final);
      if (second === null) return null;
      if (second > 0) return { index, length: first + second };
      index += first;
    }
    return null;
  }

  private lineEndingLength(index: number, final: boolean): number | null {
    const character = this.buffer[index];
    if (character === "\n") return 1;
    if (character !== "\r") return 0;
    if (index + 1 === this.buffer.length) return final ? 1 : null;
    return this.buffer[index + 1] === "\n" ? 2 : 1;
  }

  private assertWithinLimit(value: string): void {
    if (new TextEncoder().encode(value).byteLength > MAX_SSE_EVENT_BYTES) {
      throw new SseDecodeError("The SSE event exceeds the gateway limit.");
    }
  }

  private parse(raw: string): SseEvent | null {
    const data: string[] = [];
    let event: string | undefined;
    let id: string | undefined;
    for (const line of raw.split(/\r\n|\n|\r/)) {
      if (line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      let value = colon === -1 ? "" : line.slice(colon + 1);
      if (value.startsWith(" ")) value = value.slice(1);
      if (field === "data") data.push(value);
      else if (field === "event") event = value;
      else if (field === "id" && !value.includes("\0")) id = value;
    }
    if (data.length === 0) return null;
    return {
      ...(event === undefined ? {} : { event }),
      ...(id === undefined ? {} : { id }),
      data: data.join("\n"),
    };
  }
}
