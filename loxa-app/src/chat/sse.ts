export const MAX_SSE_EVENT_BYTES = 1024 * 1024;

export class SseDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SseDecodeError";
  }
}

export type SseEvent = { data: string };

export class SseDecoder {
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });
  private buffer = "";
  private finished = false;

  push(chunk: Uint8Array): SseEvent[] {
    if (this.finished) {
      throw new SseDecodeError("The SSE decoder is already finished.");
    }
    try {
      this.buffer += this.decoder.decode(chunk, { stream: true });
    } catch {
      throw new SseDecodeError("The SSE stream contains invalid UTF-8.");
    }
    return this.drain();
  }

  finish(): SseEvent[] {
    if (this.finished) return [];
    this.finished = true;
    try {
      this.buffer += this.decoder.decode();
    } catch {
      throw new SseDecodeError("The SSE stream contains invalid UTF-8.");
    }
    const events = this.drain();
    this.buffer = "";
    return events;
  }

  private drain(): SseEvent[] {
    const events: SseEvent[] = [];
    let boundary = this.nextBoundary();
    while (boundary !== null) {
      const raw = this.buffer.slice(0, boundary.index);
      this.assertWithinLimit(raw);
      this.buffer = this.buffer.slice(boundary.index + boundary.length);
      const event = this.parse(raw);
      if (event) events.push(event);
      boundary = this.nextBoundary();
    }
    this.assertWithinLimit(this.buffer);
    return events;
  }

  private nextBoundary(): { index: number; length: number } | null {
    const match = /\r\n\r\n|\n\n|\r\r/.exec(this.buffer);
    return match ? { index: match.index, length: match[0].length } : null;
  }

  private assertWithinLimit(value: string): void {
    if (new TextEncoder().encode(value).byteLength > MAX_SSE_EVENT_BYTES) {
      throw new SseDecodeError("The SSE event exceeds the gateway limit.");
    }
  }

  private parse(raw: string): SseEvent | null {
    const data: string[] = [];
    for (const line of raw.split(/\r\n|\n|\r/)) {
      if (line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      let value = colon === -1 ? "" : line.slice(colon + 1);
      if (value.startsWith(" ")) value = value.slice(1);
      if (field === "data") data.push(value);
    }
    return data.length === 0 ? null : { data: data.join("\n") };
  }
}
