import { describe, expect, it, vi } from "vitest";

import {
  NodeClientError,
  getModels,
  getStatus,
  postChatCompletion,
} from "./client";

const readyStatus = {
  node_id: "node-test",
  health: "ready",
  model: "loxa",
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
};

describe("node client", () => {
  it("requests and decodes gateway status", async () => {
    const fetch = vi.fn(async () => Response.json(readyStatus));

    await expect(
      getStatus("http://127.0.0.1:31000/", { fetch }),
    ).resolves.toEqual(readyStatus);
    expect(fetch).toHaveBeenCalledWith(
      "http://127.0.0.1:31000/loxa/status",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("rejects a successful response with malformed status JSON", async () => {
    const fetch = vi.fn(async () => Response.json({ health: "ready" }));

    await expect(getStatus("http://127.0.0.1:31000", { fetch })).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("requests and decodes only the stable model alias", async () => {
    const models = {
      object: "list",
      data: [{ id: "loxa", object: "model", owned_by: "loxa" }],
    };
    const fetch = vi.fn(async () => Response.json(models));

    await expect(getModels("http://127.0.0.1:31000", { fetch })).resolves.toEqual(models);
  });

  it("classifies connection refusal without pretending it is an HTTP response", async () => {
    const fetch = vi.fn(async () => {
      throw new TypeError("fetch failed");
    });

    await expect(getStatus("http://127.0.0.1:1", { fetch })).rejects.toMatchObject({
      kind: "transport",
      message: "Could not connect to the Loxa node.",
    });
  });

  it("aborts bounded requests and classifies timeout", async () => {
    const fetch = vi.fn((_url: string, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError")),
        );
      }),
    );

    await expect(
      getStatus("http://127.0.0.1:31000", { fetch, timeoutMs: 5 }),
    ).rejects.toMatchObject({ kind: "timeout" });
  });

  it("keeps the timeout active while decoding the response body", async () => {
    const fetch = vi.fn(async (_url: string, init?: RequestInit) => ({
      ok: true,
      status: 200,
      text: () =>
        new Promise<string>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }),
    }) as Response);

    await expect(
      getStatus("http://127.0.0.1:31000", { fetch, timeoutMs: 5 }),
    ).rejects.toMatchObject({ kind: "timeout" });
  }, 100);

  it("classifies a timeout while reading a non-2xx body as timeout", async () => {
    const fetch = vi.fn(async (_url: string, init?: RequestInit) => ({
      ok: false,
      status: 503,
      text: () =>
        new Promise<string>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }),
    }) as Response);

    await expect(
      getStatus("http://127.0.0.1:31000", { fetch, timeoutMs: 5 }),
    ).rejects.toMatchObject({ kind: "timeout" });
  }, 100);

  it("keeps caller cancellation distinct from timeout", async () => {
    const controller = new AbortController();
    const fetch = vi.fn((_url: string, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError")),
        );
      }),
    );
    const pending = getStatus("http://127.0.0.1:31000", {
      fetch,
      timeoutMs: 1_000,
      signal: controller.signal,
    });

    controller.abort();

    await expect(pending).rejects.toMatchObject({ kind: "aborted" });
  });

  it("honors a caller signal that was already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    const fetch = vi.fn(async () => Response.json(readyStatus));

    await expect(
      getStatus("http://127.0.0.1:31000", { fetch, signal: controller.signal }),
    ).rejects.toMatchObject({ kind: "aborted" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("keeps caller-first abort classification after a delayed request rejection", async () => {
    const caller = new AbortController();
    const fetch = vi.fn((_url: string, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          setTimeout(() => reject(new DOMException("aborted", "AbortError")), 20);
        });
      }),
    );
    const pending = getStatus("http://127.0.0.1:31000", {
      fetch,
      timeoutMs: 5,
      signal: caller.signal,
    });

    caller.abort();

    await expect(pending).rejects.toMatchObject({ kind: "aborted" });
  });

  it("keeps timeout-first classification after a later caller abort", async () => {
    const caller = new AbortController();
    const fetch = vi.fn((_url: string, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          setTimeout(() => reject(new DOMException("aborted", "AbortError")), 20);
        });
      }),
    );
    const pending = getStatus("http://127.0.0.1:31000", {
      fetch,
      timeoutMs: 5,
      signal: caller.signal,
    });
    setTimeout(() => caller.abort(), 10);

    await expect(pending).rejects.toMatchObject({ kind: "timeout" });
  });

  it("keeps caller-first abort classification during a delayed body rejection", async () => {
    const caller = new AbortController();
    const fetch = vi.fn(async (_url: string, init?: RequestInit) => ({
      ok: true,
      status: 200,
      text: () =>
        new Promise<string>((_resolve, reject) => {
          if (init?.signal?.aborted) {
            setTimeout(() => reject(new DOMException("aborted", "AbortError")), 20);
            return;
          }
          init?.signal?.addEventListener("abort", () => {
            setTimeout(() => reject(new DOMException("aborted", "AbortError")), 20);
          });
        }),
    }) as Response);
    const pending = getStatus("http://127.0.0.1:31000", {
      fetch,
      timeoutMs: 5,
      signal: caller.signal,
    });

    caller.abort();

    await expect(pending).rejects.toMatchObject({ kind: "aborted" });
  });

  it("keeps timeout-first classification during a delayed body rejection", async () => {
    const caller = new AbortController();
    const fetch = vi.fn(async (_url: string, init?: RequestInit) => ({
      ok: true,
      status: 200,
      text: () =>
        new Promise<string>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            setTimeout(() => reject(new DOMException("aborted", "AbortError")), 20);
          });
        }),
    }) as Response);
    const pending = getStatus("http://127.0.0.1:31000", {
      fetch,
      timeoutMs: 5,
      signal: caller.signal,
    });
    setTimeout(() => caller.abort(), 10);

    await expect(pending).rejects.toMatchObject({ kind: "timeout" });
  });

  it("preserves OpenAI-shaped details from a non-2xx response", async () => {
    const error = {
      error: {
        message: "the managed engine is temporarily unavailable",
        type: "server_error",
        param: null,
        code: "engine_unavailable",
      },
    };
    const fetch = vi.fn(async () => Response.json(error, { status: 503 }));

    await expect(
      postChatCompletion(
        "http://127.0.0.1:31000",
        { model: "loxa", messages: [] },
        { fetch },
      ),
    ).rejects.toMatchObject({ kind: "http", status: 503, openAI: error.error });
  });

  it("reports non-2xx responses with malformed errors safely", async () => {
    const fetch = vi.fn(async () => new Response("bad gateway", { status: 502 }));

    await expect(getStatus("http://127.0.0.1:31000", { fetch })).rejects.toMatchObject({
      kind: "http",
      status: 502,
      openAI: undefined,
    });
  });

  it("sends non-stream chat requests with the stable alias", async () => {
    const completion = { model: "loxa", choices: [] };
    const fetch = vi.fn(async () => Response.json(completion));

    await expect(
      postChatCompletion(
        "http://127.0.0.1:31000",
        { model: "loxa", messages: [], stream: false },
        { fetch },
      ),
    ).resolves.toEqual(completion);
    expect(fetch).toHaveBeenCalledWith(
      "http://127.0.0.1:31000/v1/chat/completions",
      expect.objectContaining({
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "loxa", messages: [], stream: false }),
      }),
    );
  });

  it("exports a distinct error type for safe presentation", () => {
    expect(new NodeClientError("timeout", "timed out")).toBeInstanceOf(Error);
  });
});
