import { describe, expect, it } from "vitest";

import { MAX_SSE_EVENT_BYTES, SseDecodeError, SseDecoder } from "./sse";

const encode = (value: string) => new TextEncoder().encode(value);

describe("SseDecoder", () => {
  it("decodes an event across every possible byte split", () => {
    const source = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n";
    const bytes = encode(source);

    for (let split = 0; split <= bytes.length; split += 1) {
      const decoder = new SseDecoder();
      expect([
        ...decoder.push(bytes.slice(0, split)),
        ...decoder.push(bytes.slice(split)),
        ...decoder.finish(),
      ]).toEqual([{ data: '{"choices":[{"delta":{"content":"hello"}}]}' }]);
    }
  });

  it("preserves UTF-8 characters split between chunks", () => {
    const bytes = encode("data: 🙂 café\n\n");
    const emojiStart = bytes.findIndex((byte) => byte === 0xf0);
    const decoder = new SseDecoder();

    expect([
      ...decoder.push(bytes.slice(0, emojiStart + 2)),
      ...decoder.push(bytes.slice(emojiStart + 2)),
    ]).toEqual([{ data: "🙂 café" }]);
  });

  it("accepts CRLF framing, comments, keepalives, fields, and multiline data", () => {
    const decoder = new SseDecoder();
    const events = decoder.push(
      encode(
        ": heartbeat\r\n\r\n" +
          "event: message\r\nid: 7\r\ndata: first\r\ndata:second\r\nretry: 1000\r\n\r\n",
      ),
    );

    expect(events).toEqual([{ data: "first\nsecond" }]);
  });

  it("does not dispatch an unterminated final event", () => {
    const decoder = new SseDecoder();
    expect(decoder.push(encode("data: final"))).toEqual([]);
    expect(decoder.finish()).toEqual([]);

    const keepalive = new SseDecoder();
    keepalive.push(encode(": still alive"));
    expect(keepalive.finish()).toEqual([]);
  });

  it("supports bare-CR separators across every byte split without breaking CRLF", () => {
    const bytes = encode("data: one\r\rdata: two\r\ndata: three\r\n\r\n");
    for (let split = 0; split <= bytes.length; split += 1) {
      const decoder = new SseDecoder();
      expect([
        ...decoder.push(bytes.slice(0, split)),
        ...decoder.push(bytes.slice(split)),
      ]).toEqual([{ data: "one" }, { data: "two\nthree" }]);
    }
  });

  it("rejects invalid UTF-8 instead of replacing bytes", () => {
    const decoder = new SseDecoder();
    expect(() => decoder.push(Uint8Array.of(0xff, 0x0a, 0x0a))).toThrow(
      SseDecodeError,
    );
  });

  it("bounds a single event to the gateway limit", () => {
    const decoder = new SseDecoder();
    decoder.push(encode(`data: ${"x".repeat(MAX_SSE_EVENT_BYTES - 6)}`));
    expect(() => decoder.push(encode("x"))).toThrow(/exceeds/i);
  });

  it("cannot be used after finish", () => {
    const decoder = new SseDecoder();
    decoder.finish();
    expect(() => decoder.push(encode("data: late\n\n"))).toThrow(/finished/i);
  });
});
