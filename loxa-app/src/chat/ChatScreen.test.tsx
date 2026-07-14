import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { useState } from "react";

import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ChatScreen, type ChatNodeAvailability, type ChatScreenHistory, type ChatScreenServices } from "./ChatScreen";
import { ChatTranscript, type ChatTurn } from "./ChatTranscript";
import { streamPersistentTurn } from "./historyClient";
import type { StreamCallbacks, StreamHandle } from "./streamChat";
import type { ControlStreamCallbacks, ControlStreamHandle } from "../control/events";

afterEach(() => {
  vi.unstubAllGlobals();
});

function services(ready = true) {
  let callbacks: StreamCallbacks | undefined;
  let controlCallbacks: ControlStreamCallbacks | undefined;
  const callbackHistory: StreamCallbacks[] = [];
  const controlCallbackHistory: ControlStreamCallbacks[] = [];
  const controlHandles: ControlStreamHandle[] = [];
  const handle: StreamHandle = {
    cancel: vi.fn(),
    dispose: vi.fn(),
    finished: Promise.resolve({ kind: "completed" }),
  };
  const api: ChatScreenServices = {
    getStatus: vi.fn().mockResolvedValue(
      ready
        ? {
            node_id: "node-7",
            health: "ready",
            model: "loxa",
            engine: { name: "llama.cpp", version: "b9999" },
            runtime_model: "gemma",
            profile: "default",
          }
        : { node_id: "node-7", health: "unavailable", model: "loxa", engine: null, runtime_model: null, profile: null },
    ),
    getModels: vi.fn().mockResolvedValue({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] }),
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    getCapabilities: vi.fn().mockResolvedValue({
      documentInput: false,
      documentInputReason: "Document input is not supported by this model and backend.",
      textChat: true,
    }),
    getControlNode: vi
      .fn()
      .mockResolvedValue({ status: "ready", activeModelId: "gemma", operationId: null, error: null }),
    getInventory: vi.fn().mockResolvedValue([
      {
        id: "gemma",
        repo: "loxa/gemma",
        revision: "rev",
        filename: "gemma.gguf",
        sha256: "ab".repeat(32),
        sizeBytes: 1,
        license: "Apache-2.0",
        params: "4B",
        quant: "Q4",
        minFreeMemoryGiB: 1,
        artifact: { kind: "downloaded" },
        compatibility: { compatible: true, reason: "Compatible" },
        engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
      },
      {
        id: "other",
        repo: "loxa/other",
        revision: "rev",
        filename: "other.gguf",
        sha256: "cd".repeat(32),
        sizeBytes: 1,
        license: "Apache-2.0",
        params: "4B",
        quant: "Q4",
        minFreeMemoryGiB: 1,
        artifact: { kind: "downloaded" },
        compatibility: { compatible: true, reason: "Compatible" },
        engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
      },
    ]),
    loadModel: vi.fn().mockResolvedValue({ operationId: "op-load" }),
    getOperation: vi.fn().mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "succeeded",
      modelId: "other",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    }),
    createControlEventStream: vi.fn((_endpoint, _token, _cursor, next) => {
      controlCallbacks = next;
      controlCallbackHistory.push(next);
      const controlHandle = { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
      controlHandles.push(controlHandle);
      return controlHandle;
    }),
    createChatStream: vi.fn((_endpoint, _request, next) => {
      callbacks = next;
      callbackHistory.push(next);
      return handle;
    }),
    copyText: vi.fn().mockResolvedValue(undefined),
  };
  return {
    api,
    handle,
    callbacks: () => callbacks,
    controlCallbacks: () => controlCallbacks,
    callbackHistory,
    controlCallbackHistory,
    controlHandles,
  };
}

function historyTurn(id: string, chatId: string) {
  return {
    id,
    chatId,
    ordinal: 0,
    state: "completed" as const,
    modelAlias: "loxa" as const,
    recipeId: "gemma",
    engineName: "llama.cpp",
    engineVersion: null,
    errorCode: null,
    createdAtMs: 1,
    updatedAtMs: 2,
  };
}

function historyMessage(id: string, turnId: string, role: "user" | "assistant") {
  return { id, turnId, role, contentBytes: 8, createdAtMs: 1, updatedAtMs: 2 };
}

function history(selection: ChatScreenHistory["selection"]): ChatScreenHistory {
  return {
    selection,
    create: vi.fn(),
    reconcileSummary: vi.fn(),
  };
}

describe("ChatScreen", () => {
  it("presents one compact Chat heading with one polite atomic live status", async () => {
    const setup = services();
    const { container } = render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    expect(await screen.findByRole("heading", { name: "Chat" })).toBeInTheDocument();
    expect(screen.queryByText("Operational tool")).not.toBeInTheDocument();
    expect(container.querySelectorAll('[aria-live="polite"][aria-atomic="true"]')).toHaveLength(1);
  });

  it("offers Models only after authoritative unloaded truth and invokes navigation", async () => {
    const user = userEvent.setup();
    const setup = services(false);
    const onNavigateModels = vi.fn();
    vi.mocked(setup.api.getControlNode).mockResolvedValue({
      status: "unloaded",
      activeModelId: null,
      operationId: null,
      error: null,
    });
    vi.mocked(setup.api.getInventory).mockResolvedValue([]);

    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" onNavigateModels={onNavigateModels} />);

    const browse = await screen.findByRole("button", { name: "Browse models" });
    await user.click(browse);
    expect(onNavigateModels).toHaveBeenCalledOnce();
  });

  it.each([
    ["checking", "checking"],
    ["busy", "loading"],
    ["ready", "ready"],
    ["error", "error"],
  ] as const)("hides Browse models while model truth is %s", async (_label, status) => {
    const setup = services(status === "ready");
    if (status === "checking") {
      vi.mocked(setup.api.getControlNode).mockImplementation(() => new Promise(() => undefined));
    } else {
      vi.mocked(setup.api.getControlNode).mockResolvedValue({
        status,
        activeModelId: status === "ready" || status === "loading" ? "gemma" : null,
        operationId: status === "loading" ? "operation-1" : null,
        error: status === "error" ? "runtime failed" : null,
      });
    }

    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" onNavigateModels={vi.fn()} />);

    if (status !== "checking") await screen.findByRole("status");
    expect(screen.queryByRole("button", { name: "Browse models" })).not.toBeInTheDocument();
  });

  it("hides Browse models while the shared node session is reconciling", () => {
    const setup = services(false);
    render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={{ phase: "reconciling", proven: true, error: null }}
        onNavigateModels={vi.fn()}
      />,
    );

    expect(screen.queryByRole("button", { name: "Browse models" })).not.toBeInTheDocument();
  });

  it("uses owned controls and Lucide icons for attachment, send, and stop", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    const attachment = screen.getByRole("button", { name: "Attach document" });
    expect(attachment).toHaveAttribute("data-slot", "button");
    expect(attachment.querySelector("svg.lucide-paperclip")).not.toBeNull();

    const message = await screen.findByLabelText("Message");
    await user.type(message, "Explain this");
    const send = screen.getByRole("button", { name: "Send message" });
    expect(send).toHaveAttribute("data-slot", "button");
    expect(send.querySelector("svg.lucide-send")).not.toBeNull();
    await user.click(send);

    const stop = screen.getByRole("button", { name: "Stop response" });
    expect(stop).toHaveAttribute("data-slot", "button");
    expect(stop.querySelector("svg.lucide-square")).not.toBeNull();
  });

  it("uses a named scrolling transcript and a final-row message composer", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    expect(await screen.findByRole("log", { name: "Conversation" })).toBeInTheDocument();
    expect(screen.getByRole("form", { name: "Message composer" })).toBeInTheDocument();
  });

  it("shows shared history, restores Markdown, and sends through the persistent turn route", async () => {
    const user = userEvent.setup();
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    const turnId = "1123456789abcdef0123456789abcdef";
    const userMessageId = "2123456789abcdef0123456789abcdef";
    const assistantMessageId = "3123456789abcdef0123456789abcdef";
    const shellHistory = history({ chatId, revision: 1 });
    let persistentCallbacks: import("./historyClient").PersistentTurnCallbacks | undefined;
    Object.assign(setup.api, {
      getChat: vi.fn().mockResolvedValue({ id: chatId, title: "Next question", createdAtMs: 1, updatedAtMs: 5 }),
      listTurns: vi.fn().mockResolvedValue({
        turns: [
          {
            id: turnId,
            chatId,
            ordinal: 0,
            state: "completed",
            modelAlias: "loxa",
            recipeId: "gemma",
            engineName: "llama.cpp",
            engineVersion: null,
            errorCode: null,
            createdAtMs: 2,
            updatedAtMs: 3,
          },
        ],
        nextAfter: null,
      }),
      listMessageSummaries: vi.fn().mockResolvedValue([
        { id: userMessageId, turnId, role: "user", contentBytes: 5, createdAtMs: 2, updatedAtMs: 2 },
        { id: assistantMessageId, turnId, role: "assistant", contentBytes: 8, createdAtMs: 3, updatedAtMs: 3 },
      ]),
      getMessageContent: vi.fn((_endpoint: string, _token: string, _chat: string, _turn: string, messageId: string) =>
        Promise.resolve(messageId === userMessageId ? "Hello" : "**Answer**"),
      ),
      createPersistentTurn: vi.fn(
        (
          _endpoint: string,
          _token: string,
          _chat: string,
          _content: string,
          callbacks: import("./historyClient").PersistentTurnCallbacks,
        ) => {
          persistentCallbacks = callbacks;
          return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise(() => undefined) };
        },
      ),
    });

    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={shellHistory} />);
    expect(await screen.findByText("Answer", { selector: "strong" })).toBeInTheDocument();
    await user.type(screen.getByLabelText("Message"), "Next question");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    expect(setup.api.createPersistentTurn).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      chatId,
      "Next question",
      expect.any(Object),
      expect.any(AbortSignal),
    );
    act(() => persistentCallbacks?.onStarted("4123456789abcdef0123456789abcdef", 2));
    expect(screen.getByText("2 earlier turns were omitted from the model context.")).toBeVisible();
    act(() => persistentCallbacks?.onDelta("# Persisted"));
    act(() => persistentCallbacks?.onTerminal({ kind: "completed" }));
    expect(await screen.findByRole("heading", { name: "Persisted" })).toBeInTheDocument();
    await waitFor(() =>
      expect(shellHistory.reconcileSummary).toHaveBeenCalledWith({
        id: chatId,
        title: "Next question",
        createdAtMs: 1,
        updatedAtMs: 5,
      }),
    );
    const copyButtons = screen.getAllByRole("button", { name: "Copy response" });
    expect(copyButtons[copyButtons.length - 1]).toBeEnabled();
  });

  it("preserves the first persistent transcript while adopting its created selection and restores after remount", async () => {
    const user = userEvent.setup();
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    const turnId = "1123456789abcdef0123456789abcdef";
    const created = { id: chatId, title: "Created chat", createdAtMs: 1, updatedAtMs: 2 };
    const create = vi.fn().mockResolvedValue(created);
    const firstHistory: ChatScreenHistory = {
      selection: { chatId: null, revision: 0 },
      create,
      reconcileSummary: vi.fn(),
    };
    let callbacks: import("./historyClient").PersistentTurnCallbacks | undefined;
    Object.assign(setup.api, {
      listTurns: vi.fn().mockResolvedValue({ turns: [historyTurn(turnId, chatId)], nextAfter: null }),
      listMessageSummaries: vi
        .fn()
        .mockResolvedValue([
          historyMessage("2123456789abcdef0123456789abcdef", turnId, "user"),
          historyMessage("3123456789abcdef0123456789abcdef", turnId, "assistant"),
        ]),
      getMessageContent: vi.fn((_endpoint: string, _token: string, _chat: string, _turn: string, messageId: string) =>
        Promise.resolve(messageId.startsWith("2") ? "Created prompt" : "Restored after remount"),
      ),
      createPersistentTurn: vi.fn(
        (
          _endpoint: string,
          _token: string,
          _chat: string,
          _content: string,
          next: import("./historyClient").PersistentTurnCallbacks,
        ) => {
          callbacks = next;
          return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise(() => undefined) };
        },
      ),
    });
    const view = render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={firstHistory} />);
    await waitFor(() => expect(screen.getByLabelText("Message")).toBeEnabled());
    await user.type(screen.getByLabelText("Message"), "Created prompt");
    await waitFor(() => expect(screen.getByRole("button", { name: "Send message" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "Send message" }));
    await waitFor(() => expect(create).toHaveBeenCalledOnce());
    await waitFor(() => expect(setup.api.createPersistentTurn).toHaveBeenCalledOnce());

    const selectedHistory = history({ chatId, revision: 1 });
    view.rerender(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={selectedHistory} />);
    expect(screen.getByText("Created prompt")).toBeVisible();
    expect(setup.api.listTurns).not.toHaveBeenCalled();
    act(() => callbacks?.onStarted(turnId, 0));
    act(() => callbacks?.onDelta("Created answer"));
    act(() => callbacks?.onTerminal({ kind: "completed" }));
    expect(await screen.findByText("Created answer")).toBeVisible();

    view.unmount();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={selectedHistory} />);
    expect(await screen.findByText("Restored after remount")).toBeVisible();
    expect(setup.api.listTurns).toHaveBeenCalledTimes(1);
  });

  it("fails closed on repeated restore cursors", async () => {
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    Object.assign(setup.api, {
      listTurns: vi
        .fn()
        .mockResolvedValueOnce({ turns: [], nextAfter: "repeat" })
        .mockResolvedValueOnce({ turns: [], nextAfter: "repeat" }),
      listMessageSummaries: vi.fn(),
      getMessageContent: vi.fn(),
      createPersistentTurn: vi.fn(),
    });
    render(
      <ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={history({ chatId, revision: 1 })} />,
    );
    expect(await screen.findByText(/invalid chat history pagination/i)).toBeVisible();
  });

  it("aborts a stale restore when selection is superseded and ignores its late result", async () => {
    const setup = services();
    const staleChat = "1123456789abcdef0123456789abcdef";
    const freshChat = "2123456789abcdef0123456789abcdef";
    const staleTurn = "3123456789abcdef0123456789abcdef";
    const freshTurn = "4123456789abcdef0123456789abcdef";
    let resolveStale!: (page: Awaited<ReturnType<NonNullable<ChatScreenServices["listTurns"]>>>) => void;
    let staleSignal: AbortSignal | undefined;
    Object.assign(setup.api, {
      listTurns: vi.fn(
        (_endpoint: string, _token: string, chatId: string, _page: unknown, options?: { signal?: AbortSignal }) => {
          if (chatId === staleChat) {
            staleSignal = options?.signal;
            return new Promise((resolve) => {
              resolveStale = resolve;
            });
          }
          if (chatId === freshChat)
            return Promise.resolve({ turns: [historyTurn(freshTurn, freshChat)], nextAfter: null });
          return Promise.resolve({ turns: [], nextAfter: null });
        },
      ),
      listMessageSummaries: vi.fn((_endpoint: string, _token: string, _chat: string, turnId: string) =>
        Promise.resolve([
          historyMessage(`${turnId.slice(0, 31)}a`, turnId, "user"),
          historyMessage(`${turnId.slice(0, 31)}b`, turnId, "assistant"),
        ]),
      ),
      getMessageContent: vi.fn((_endpoint: string, _token: string, _chat: string, turnId: string, messageId: string) =>
        Promise.resolve(
          messageId.endsWith("a") ? "Prompt" : turnId === freshTurn ? "Fresh assistant" : "Stale assistant",
        ),
      ),
      createPersistentTurn: vi.fn(),
    });

    const view = render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        history={history({ chatId: staleChat, revision: 1 })}
      />,
    );
    await vi.waitFor(() => expect(staleSignal).toBeInstanceOf(AbortSignal));
    view.rerender(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        history={history({ chatId: freshChat, revision: 2 })}
      />,
    );
    expect(staleSignal?.aborted).toBe(true);
    expect(await screen.findByText("Fresh assistant")).toBeVisible();

    resolveStale({ turns: [historyTurn(staleTurn, staleChat)], nextAfter: null });
    await act(async () => undefined);
    expect(screen.queryByText("Stale assistant")).not.toBeInTheDocument();
    expect(screen.getByText("Fresh assistant")).toBeVisible();
  });

  it("aborts a pending history restore at the window lifecycle boundary", async () => {
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    let restoreSignal: AbortSignal | undefined;
    Object.assign(setup.api, {
      listTurns: vi.fn(
        (_endpoint: string, _token: string, _chatId: string, _page: unknown, options?: { signal?: AbortSignal }) => {
          restoreSignal = options?.signal;
          return new Promise((_resolve, reject) =>
            options?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), {
              once: true,
            }),
          );
        },
      ),
      listMessageSummaries: vi.fn(),
      getMessageContent: vi.fn(),
    });
    render(
      <ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={history({ chatId, revision: 1 })} />,
    );
    await vi.waitFor(() => expect(restoreSignal).toBeInstanceOf(AbortSignal));

    window.dispatchEvent(new Event("beforeunload"));
    expect(restoreSignal?.aborted).toBe(true);
    await act(async () => undefined);
    expect(screen.queryByText("aborted")).not.toBeInTheDocument();
  });

  it("aborts the production persistent client request on unmount", async () => {
    const user = userEvent.setup();
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    let requestSignal: AbortSignal | undefined;
    const productionFetch = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          requestSignal = init?.signal ?? undefined;
          init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), {
            once: true,
          });
        }),
    );
    Object.assign(setup.api, {
      listTurns: vi.fn().mockResolvedValue({ turns: [], nextAfter: null }),
      listMessageSummaries: vi.fn(),
      getMessageContent: vi.fn(),
      createPersistentTurn: (
        endpoint: string,
        token: string,
        selectedChat: string,
        content: string,
        callbacks: import("./historyClient").PersistentTurnCallbacks,
        signal?: AbortSignal,
      ) => streamPersistentTurn(endpoint, token, selectedChat, content, callbacks, signal, productionFetch),
    });
    const view = render(
      <ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={history({ chatId, revision: 1 })} />,
    );
    await waitFor(() => expect(setup.api.listTurns).toHaveBeenCalledOnce());
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    await vi.waitFor(() => expect(productionFetch).toHaveBeenCalledOnce());

    view.unmount();
    expect(requestSignal?.aborted).toBe(true);
  });

  it.each([
    ["checking", "Starting the private Loxa node"],
    ["starting", "Starting the private Loxa node"],
    ["stopping", "The app-owned node is stopping"],
    ["recovery-required", "Recovery required"],
    ["reconciling", "Refreshing authoritative model status"],
    ["error", "cold-start failed"],
    ["disconnected", "The Loxa node is disconnected"],
  ] as const)("keeps the full inert composer while the shared node phase is %s", async (phase, reason) => {
    const setup = services();
    render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={{ phase, proven: false, error: phase === "error" ? "cold-start failed" : null }}
      />,
    );

    expect(await screen.findByRole("log", { name: "Conversation" })).toBeVisible();
    expect(screen.getByRole("form", { name: "Message composer" })).toBeVisible();
    expect(screen.getAllByText(new RegExp(reason, "i"))).not.toHaveLength(0);
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Send message" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Attach document" })).toHaveAttribute("aria-disabled", "true");
    expect(setup.api.getStatus).not.toHaveBeenCalled();
    expect(setup.api.readControlToken).not.toHaveBeenCalled();
  });

  it("keeps a real proven reconciliation inert without starting route probes", async () => {
    const setup = services();
    render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={{ phase: "reconciling", proven: true, error: null }}
      />,
    );

    expect(await screen.findByRole("status")).toHaveTextContent(/refreshing authoritative model status/i);
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeDisabled();
    expect(setup.api.getStatus).not.toHaveBeenCalled();
    expect(setup.api.readControlToken).not.toHaveBeenCalled();
  });

  it("clears a blocked startup error before publishing a newly proven ready session", async () => {
    const setup = services();
    const view = render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={{ phase: "error", proven: false, error: "cold-start failed" }}
      />,
    );
    expect(await screen.findByRole("status")).toHaveTextContent("cold-start failed");

    view.rerender(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={{ phase: "ready", proven: true, error: null }}
      />,
    );

    await vi.waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Ready"));
    expect(screen.getByRole("status")).not.toHaveTextContent("cold-start failed");
    expect(screen.getByLabelText("Message")).toBeEnabled();
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeEnabled();
  });

  it("sends on Enter, preserves Shift+Enter newlines, and ignores Enter during composition", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const message = await screen.findByLabelText("Message");

    await user.type(message, "Composing");
    fireEvent.keyDown(message, { key: "Enter", code: "Enter", isComposing: true });
    expect(setup.api.createChatStream).not.toHaveBeenCalled();

    await user.clear(message);
    await user.type(message, "Line one{Shift>}{Enter}{/Shift}Line two");
    expect(message).toHaveValue("Line one\nLine two");
    expect(setup.api.createChatStream).not.toHaveBeenCalled();

    fireEvent.keyDown(message, { key: "Enter", code: "Enter" });
    expect(setup.api.createChatStream).toHaveBeenCalledOnce();
    expect(setup.api.createChatStream).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      { model: "loxa", messages: [{ role: "user", content: "Line one\nLine two" }] },
      expect.any(Object),
    );
  });

  it("replaces Send with Stop, cancels once, preserves partial text, and restores focus", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const message = await screen.findByLabelText("Message");
    await user.type(message, "Explain the node");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    setup.callbacks()?.onDelta("The node keeps ");

    expect(screen.queryByRole("button", { name: "Send message" })).not.toBeInTheDocument();
    expect(message).toBeDisabled();
    const stop = screen.getByRole("button", { name: "Stop response" });
    await user.click(stop);
    await user.click(stop);
    expect(setup.handle.cancel).toHaveBeenCalledOnce();

    act(() => setup.callbacks()?.onTerminal({ kind: "cancelled" }));
    expect(await screen.findByText("The node keeps")).toBeInTheDocument();
    expect(screen.getByText("Turn cancelled")).toBeInTheDocument();
    await vi.waitFor(() => expect(message).toHaveFocus());
  });

  it.each([
    [{ kind: "completed" as const }, "Turn completed"],
    [{ kind: "error" as const, message: "runtime failed" }, "Turn failed — runtime failed"],
  ])("restores composer focus after terminal result %#", async (terminal, label) => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const message = await screen.findByLabelText("Message");
    await user.type(message, "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    act(() => setup.callbacks()?.onTerminal(terminal));
    expect(await screen.findByText(label)).toBeInTheDocument();
    await vi.waitFor(() => expect(message).toHaveFocus());
  });

  it("publishes and clears the route-local interaction lock across terminal and unmount", async () => {
    const user = userEvent.setup();
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    const lock = vi.fn();
    let callbacks: import("./historyClient").PersistentTurnCallbacks | undefined;
    setup.api.createPersistentTurn = vi.fn((_endpoint, _token, _chat, _content, next) => {
      callbacks = next;
      return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
    });
    const view = render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        history={history({ chatId, revision: 1 })}
        onInteractionLockChange={lock}
      />,
    );
    await waitFor(() => expect(screen.getByLabelText("Message")).toBeEnabled());
    await user.type(screen.getByLabelText("Message"), "Lock the rail");
    await waitFor(() => expect(screen.getByRole("button", { name: "Send message" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "Send message" }));
    await waitFor(() => expect(lock).toHaveBeenLastCalledWith(true));
    act(() => callbacks?.onStarted("1123456789abcdef0123456789abcdef", 0));
    act(() => callbacks?.onTerminal({ kind: "completed" }));
    await waitFor(() => expect(lock).toHaveBeenLastCalledWith(false));

    await user.type(screen.getByLabelText("Message"), "Unmount while active");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    await waitFor(() => expect(lock).toHaveBeenLastCalledWith(true));
    view.unmount();
    expect(lock).toHaveBeenLastCalledWith(false);
  });

  it("terminalizes an active response before node reconciliation disposes its stream", async () => {
    const user = userEvent.setup();
    const setup = services();
    const lock = vi.fn();
    const ready: ChatNodeAvailability = { phase: "ready", proven: true, error: null };
    const view = render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={ready}
        onInteractionLockChange={lock}
      />,
    );
    const message = await screen.findByLabelText("Message");
    await user.type(message, "Preserve this turn");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    act(() => setup.callbacks()?.onDelta("Partial output"));
    await waitFor(() => expect(lock).toHaveBeenLastCalledWith(true));

    view.rerender(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={{ phase: "reconciling", proven: true, error: null }}
        onInteractionLockChange={lock}
      />,
    );

    expect(await screen.findByText("Partial output")).toBeVisible();
    expect(screen.getByText("Turn cancelled")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Stop response" })).not.toBeInTheDocument();
    expect(setup.handle.dispose).toHaveBeenCalledOnce();
    await waitFor(() => expect(lock).toHaveBeenLastCalledWith(false));

    view.rerender(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        nodeAvailability={ready}
        onInteractionLockChange={lock}
      />,
    );
    await waitFor(() => expect(message).toBeEnabled());
    await waitFor(() => expect(message).toHaveFocus());
  });

  it("defines a canonical responsive and accessible Chat module contract", () => {
    const paths = ["ChatScreen.module.css", "ChatComposer.module.css", "ChatTranscript.module.css"].map((name) =>
      resolve(process.cwd(), `src/chat/${name}`),
    );
    const css = paths.map((path) => (existsSync(path) ? readFileSync(path, "utf8") : "")).join("\n");
    const canonicalPath = resolve(process.cwd(), "src/styles/loxa.css");
    const canonicalCss = existsSync(canonicalPath) ? readFileSync(canonicalPath, "utf8") : "";
    const canonicalTokens = new Set(Array.from(canonicalCss.matchAll(/(--loxa-[\w-]+)\s*:/g), ([, token]) => token));
    const chatTokens = new Set(Array.from(css.matchAll(/var\((--loxa-[\w-]+)/g), ([, token]) => token));

    expect(css).toContain("grid-template-rows");
    expect(css).toMatch(/\.chatMain\s*\{[^}]*grid-template-rows:\s*auto minmax\(0, 1fr\) auto/s);
    expect(css).not.toMatch(/\.chatWorkspace\s*\{/);
    expect(css).toMatch(/@media \(max-width:[\s\S]*?\.chatMain\s*\{[^}]*overflow:\s*hidden/s);
    expect(css).toMatch(/overflow-y:\s*auto/);
    expect(css).toContain("var(--loxa-component-minimum-interactive-target)");
    expect(css).toContain(":focus-visible");
    expect(css).toContain("@media (max-width:");
    expect(css).toContain("@media (prefers-reduced-motion: reduce)");
    expect(css).toContain("@media (prefers-contrast: more)");
    expect(css).toContain("@media (forced-colors: active)");
    expect(css).not.toMatch(/#[0-9a-f]{3,8}\b/i);
    expect(css).not.toMatch(/gradient|backdrop-filter|box-shadow/i);
    expect(Array.from(chatTokens).filter((token) => !canonicalTokens.has(token))).toEqual([]);
  });

  it("keeps model-operation blocking visible instead of reporting Ready", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });

    act(() =>
      setup.controlCallbacks()?.onSnapshot({
        cursor: 4,
        cursorGap: false,
        operations: [
          {
            id: "load-4",
            kind: "load",
            status: "running",
            modelId: "other",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
        ],
        events: [],
      }),
    );

    expect(screen.getByRole("status")).toHaveTextContent(/model operation in progress/i);
    expect(screen.getByRole("status")).not.toHaveTextContent("Ready");
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeDisabled();
    expect(screen.getAllByText(/model operation in progress/i)).not.toHaveLength(0);
  });

  it("reconnects from the terminal cursor and restores controls after a fresh snapshot", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });

    act(() =>
      setup.controlCallbackHistory[0].onTerminal({
        kind: "error",
        cursor: 7,
        message: "Live model updates disconnected.",
      }),
    );
    expect(screen.getByRole("status")).toHaveTextContent(/reconnecting to live model updates/i);
    expect(screen.getByRole("status")).not.toHaveTextContent("Ready");
    expect(screen.getByLabelText("Message")).toBeDisabled();
    await vi.waitFor(() => expect(setup.api.createControlEventStream).toHaveBeenCalledTimes(2));
    expect(setup.api.createControlEventStream).toHaveBeenLastCalledWith(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      7,
      expect.any(Object),
      expect.any(AbortSignal),
    );

    act(() => setup.controlCallbackHistory[1].onSnapshot({ cursor: 7, cursorGap: false, operations: [], events: [] }));
    expect(screen.getByLabelText("Message")).toBeEnabled();
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeEnabled();
  });

  it("auto-scrolls near the bottom but preserves a reader's scroll-away position", () => {
    const first: ChatTurn = { id: 1, model: "gemma", prompt: "Hello", response: "One", status: "streaming", error: "" };
    const copyText = vi.fn().mockResolvedValue(undefined);
    const view = render(<ChatTranscript turns={[first]} emptyMessage="Empty" copyText={copyText} />);
    const transcript = screen.getByRole("log", { name: "Conversation" });
    Object.defineProperty(transcript, "scrollHeight", { configurable: true, value: 1_000 });
    Object.defineProperty(transcript, "clientHeight", { configurable: true, value: 200 });
    transcript.scrollTop = 760;
    fireEvent.scroll(transcript);
    view.rerender(
      <ChatTranscript turns={[{ ...first, response: "One two" }]} emptyMessage="Empty" copyText={copyText} />,
    );
    expect(transcript.scrollTop).toBe(1_000);

    transcript.scrollTop = 200;
    fireEvent.scroll(transcript);
    Object.defineProperty(transcript, "scrollHeight", { configurable: true, value: 1_200 });
    view.rerender(
      <ChatTranscript turns={[{ ...first, response: "One two three" }]} emptyMessage="Empty" copyText={copyText} />,
    );
    expect(transcript.scrollTop).toBe(200);
  });

  it("shows disconnected explicitly and does not offer send", async () => {
    const { api } = services(false);
    render(<ChatScreen services={api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("status")).toHaveTextContent("Disconnected");
    expect(screen.getByRole("button", { name: "Send message" })).toBeDisabled();
  });

  it("uses the public model alias in requests and streams incremental output", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    expect(setup.api.createChatStream).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      { model: "loxa", messages: [{ role: "user", content: "Hello" }] },
      expect.any(Object),
    );
    expect(screen.getByRole("status")).toHaveTextContent("Queued");
    setup.callbacks()?.onDelta("Hel");
    setup.callbacks()?.onDelta("lo");
    await vi.waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Streaming"));
    expect(screen.getAllByText("Hello")).not.toHaveLength(0);
    expect(screen.getByRole("combobox", { name: "Choose model" })).toHaveValue("gemma");
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeDisabled();
  });

  it("coalesces streamed display updates to one animation frame and flushes before terminal", async () => {
    const user = userEvent.setup();
    let callback: FrameRequestCallback | undefined;
    const request = vi.fn((next: FrameRequestCallback) => {
      callback = next;
      return 41;
    });
    const cancel = vi.fn();
    vi.stubGlobal("requestAnimationFrame", request);
    vi.stubGlobal("cancelAnimationFrame", cancel);
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    act(() => {
      setup.callbacks()?.onDelta("**Hel");
      setup.callbacks()?.onDelta("lo**");
    });
    expect(request).toHaveBeenCalledOnce();
    expect(screen.queryByText("Hello", { selector: "strong" })).not.toBeInTheDocument();
    act(() => callback?.(1));
    expect(screen.getByText("Hello", { selector: "strong" })).toBeInTheDocument();

    act(() => setup.callbacks()?.onDelta(" final"));
    expect(request).toHaveBeenCalledTimes(2);
    act(() => setup.callbacks()?.onTerminal({ kind: "completed" }));
    expect(cancel).toHaveBeenCalledWith(41);
    expect(screen.getByRole("article", { name: "Chat turn using gemma" })).toHaveTextContent("Hello final");
    expect(screen.getByText("Turn completed")).toBeInTheDocument();
    vi.unstubAllGlobals();
  });

  it("keeps streamed tokens out of live regions and exposes one concise terminal announcement", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    act(() => setup.callbacks()?.onDelta("Partial"));
    await vi.waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Streaming"));

    const log = screen.getByRole("log", { name: "Conversation" });
    expect(log).toHaveAttribute("aria-live", "off");
    expect(within(log).queryByRole("status")).not.toBeInTheDocument();
    act(() => setup.callbacks()?.onTerminal({ kind: "completed" }));
    expect(screen.getAllByRole("status")).toHaveLength(1);
    expect(screen.getByRole("status")).toHaveTextContent("Completed");
  });

  it("cancels a pending streamed display frame on window close and unmount", async () => {
    const user = userEvent.setup();
    const request = vi.fn(() => 72);
    const cancel = vi.fn();
    vi.stubGlobal("requestAnimationFrame", request);
    vi.stubGlobal("cancelAnimationFrame", cancel);
    const setup = services();
    const view = render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    act(() => setup.callbacks()?.onDelta("pending"));

    window.dispatchEvent(new Event("beforeunload"));
    view.unmount();
    expect(cancel).toHaveBeenCalledOnce();
    expect(cancel).toHaveBeenCalledWith(72);
    vi.unstubAllGlobals();
  });

  it("keeps the active model authoritative until an explicit switch succeeds", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getControlNode)
      .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma", operationId: null, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "other", operationId: null, error: null });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const picker = await screen.findByRole("combobox", { name: "Choose model" });
    await user.selectOptions(picker, "other");
    expect(screen.getByText("Active model: gemma")).toBeInTheDocument();
    expect(setup.api.loadModel).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    expect(setup.api.loadModel).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      "other",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(await screen.findByText("Active model: other")).toBeInTheDocument();
  });

  it("does not abort its own model switch when shared truth enters reconciliation", async () => {
    const user = userEvent.setup();
    const setup = services();
    let resolveOperation!: (operation: Awaited<ReturnType<ChatScreenServices["getOperation"]>>) => void;
    vi.mocked(setup.api.getOperation).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveOperation = resolve;
        }),
    );
    vi.mocked(setup.api.getControlNode)
      .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma", operationId: null, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "other", operationId: null, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "other", operationId: null, error: null });
    function Harness() {
      const [availability, setAvailability] = useState<ChatNodeAvailability>({
        phase: "ready",
        proven: true,
        error: null,
      });
      return (
        <ChatScreen
          services={setup.api}
          endpoint="http://127.0.0.1:8080"
          nodeAvailability={availability}
          onModelMutationStart={() => setAvailability({ phase: "reconciling", proven: true, error: null })}
        />
      );
    }
    render(<Harness />);

    const picker = await screen.findByRole("combobox", { name: "Choose model" });
    await user.selectOptions(picker, "other");
    await user.click(screen.getByRole("button", { name: "Switch to other" }));

    await vi.waitFor(() => expect(setup.api.getOperation).toHaveBeenCalled());
    const operationSignal = vi.mocked(setup.api.getOperation).mock.calls[0][3]?.signal;
    expect(operationSignal?.aborted).toBe(false);
    resolveOperation({
      id: "op-load",
      kind: "load",
      status: "succeeded",
      modelId: "other",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    await vi.waitFor(() => expect(setup.api.getControlNode).toHaveBeenCalledTimes(2));
    expect(operationSignal?.aborted).toBe(false);
    expect(screen.getByLabelText("Message")).toBeDisabled();
  });

  it("does not settle an accepted model switch when the operation read fails before terminal", async () => {
    const setup = services();
    const user = userEvent.setup();
    const onModelMutationStart = vi.fn();
    const onModelMutationSettled = vi.fn();
    vi.mocked(setup.api.getOperation).mockRejectedValueOnce(new Error("operation read unavailable"));
    render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        onModelMutationStart={onModelMutationStart}
        onModelMutationSettled={onModelMutationSettled}
      />,
    );

    await user.selectOptions(await screen.findByRole("combobox", { name: "Choose model" }), "other");
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    expect(onModelMutationStart).toHaveBeenCalledWith("op-load");
    await vi.waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("operation read unavailable"));
    expect(onModelMutationSettled).not.toHaveBeenCalled();

    act(() =>
      setup.controlCallbacks()?.onEvent({
        sequence: 6,
        operation: {
          id: "op-load",
          kind: "load",
          status: "succeeded",
          modelId: "other",
          progress: null,
          error: null,
          createdAtUnixMs: 1,
          updatedAtUnixMs: 2,
        },
      }),
    );
    await vi.waitFor(() => expect(onModelMutationSettled).toHaveBeenCalledOnce());
    expect(onModelMutationSettled).toHaveBeenCalledWith("op-load");
  });

  it("forwards active and terminal lifecycle ids from reconnect snapshots", async () => {
    const setup = services();
    const onModelMutationStart = vi.fn();
    const onModelMutationSettled = vi.fn();
    render(
      <ChatScreen
        services={setup.api}
        endpoint="http://127.0.0.1:8080"
        onModelMutationStart={onModelMutationStart}
        onModelMutationSettled={onModelMutationSettled}
      />,
    );
    await screen.findByRole("combobox", { name: "Choose model" });

    act(() =>
      setup.controlCallbacks()?.onSnapshot({
        cursor: 14,
        cursorGap: true,
        operations: [
          {
            id: "op-active",
            kind: "load",
            status: "running",
            modelId: "other",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
          {
            id: "op-current",
            kind: "load",
            status: "succeeded",
            modelId: "other",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
          {
            id: "op-old",
            kind: "unload",
            status: "failed",
            modelId: null,
            progress: null,
            error: "old",
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
        ],
        events: [],
      }),
    );

    expect(onModelMutationStart).toHaveBeenCalledWith("op-active");
    expect(onModelMutationSettled).toHaveBeenCalledWith("op-current");
    expect(onModelMutationSettled).toHaveBeenCalledWith("op-old");
  });

  it("loads explicitly from an unloaded node and enables chat only after ready confirmation", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getControlNode)
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma", operationId: null, error: null });
    vi.mocked(setup.api.getOperation).mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "succeeded",
      modelId: "gemma",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("status")).toHaveTextContent("No model is loaded");
    expect(screen.getByRole("status")).not.toHaveTextContent("Disconnected");
    expect(screen.getByLabelText("Message")).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Load gemma" }));
    expect(await screen.findByRole("status")).toHaveTextContent("Ready");
    expect(screen.getByLabelText("Message")).toBeEnabled();
  });

  it("restores history and unlocks the composer after an unloaded-node model load publishes fresh request-model evidence", async () => {
    const user = userEvent.setup();
    const setup = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    const turnId = "1123456789abcdef0123456789abcdef";
    vi.mocked(setup.api.getModels)
      .mockResolvedValueOnce({ object: "list", data: [] } as unknown as Awaited<
        ReturnType<ChatScreenServices["getModels"]>
      >)
      .mockResolvedValueOnce({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] });
    vi.mocked(setup.api.getControlNode)
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma", operationId: null, error: null });
    vi.mocked(setup.api.getOperation).mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "succeeded",
      modelId: "gemma",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    Object.assign(setup.api, {
      listTurns: vi.fn().mockResolvedValue({ turns: [historyTurn(turnId, chatId)], nextAfter: null }),
      listMessageSummaries: vi
        .fn()
        .mockResolvedValue([
          historyMessage("2123456789abcdef0123456789abcdef", turnId, "user"),
          historyMessage("3123456789abcdef0123456789abcdef", turnId, "assistant"),
        ]),
      getMessageContent: vi.fn((_endpoint: string, _token: string, _chat: string, _turn: string, messageId: string) =>
        Promise.resolve(messageId.startsWith("2") ? "Earlier prompt" : "Earlier answer"),
      ),
      createPersistentTurn: vi.fn(),
    });
    const view = render(
      <ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" history={history({ chatId, revision: 1 })} />,
    );
    expect(await screen.findByText("Earlier answer")).toBeVisible();
    expect(screen.getByLabelText("Message")).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Load gemma" }));

    await waitFor(() => expect(setup.api.getModels).toHaveBeenCalledTimes(2));
    expect(screen.getByLabelText("Message")).toBeEnabled();
    view.unmount();
  });

  it("keeps the previous active model when a switch fails", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getOperation).mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "failed",
      modelId: "other",
      progress: null,
      error: "readiness failed",
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await user.selectOptions(await screen.findByRole("combobox", { name: "Choose model" }), "other");
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    expect(await screen.findByRole("status")).toHaveTextContent("readiness failed");
    expect(screen.getByText("Active model: gemma")).toBeInTheDocument();
  });

  it.each(["rejected", "empty"] as const)(
    "keeps the composer disabled when fresh post-load request-model evidence is %s",
    async (outcome) => {
      const user = userEvent.setup();
      const setup = services();
      vi.mocked(setup.api.getControlNode)
        .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma", operationId: null, error: null })
        .mockResolvedValue({ status: "ready", activeModelId: "other", operationId: null, error: null });
      vi.mocked(setup.api.getOperation).mockResolvedValue({
        id: "op-load",
        kind: "load",
        status: "succeeded",
        modelId: "other",
        progress: null,
        error: null,
        createdAtUnixMs: 1,
        updatedAtUnixMs: 2,
      });
      vi.mocked(setup.api.getModels).mockResolvedValueOnce({
        object: "list",
        data: [{ id: "loxa", object: "model", owned_by: "loxa" }],
      });
      if (outcome === "rejected")
        vi.mocked(setup.api.getModels).mockRejectedValueOnce(new Error("fresh models unavailable"));
      else
        vi.mocked(setup.api.getModels).mockResolvedValueOnce({ object: "list", data: [] } as unknown as Awaited<
          ReturnType<ChatScreenServices["getModels"]>
        >);
      render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
      const picker = await screen.findByRole("combobox", { name: "Choose model" });
      await user.selectOptions(picker, "other");
      await user.click(screen.getByRole("button", { name: "Switch to other" }));

      await waitFor(() => expect(setup.api.getModels).toHaveBeenCalledTimes(2));
      expect(screen.getByLabelText("Message")).toBeDisabled();
    },
  );

  it("blocks model switching while the node reports an active operation", async () => {
    const setup = services();
    vi.mocked(setup.api.getControlNode).mockResolvedValue({
      status: "loading",
      activeModelId: "gemma",
      operationId: "op-existing",
      error: null,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("combobox", { name: "Choose model" })).toBeDisabled();
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Send message" })).toBeDisabled();
    expect(setup.api.loadModel).not.toHaveBeenCalled();
  });

  it("reconciles after a rejected local switch without wedging the composer", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.loadModel).mockRejectedValue(new Error("operation conflict"));
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const picker = await screen.findByRole("combobox", { name: "Choose model" });
    await user.selectOptions(picker, "other");
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    expect(await screen.findByRole("status")).toHaveTextContent("operation conflict");
    expect(picker).toBeEnabled();
    expect(screen.getByLabelText("Message")).toBeEnabled();
  });

  it("ignores an older lifecycle refresh that resolves after newer terminal truth", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    let resolveOlder!: (value: Awaited<ReturnType<ChatScreenServices["getControlNode"]>>) => void;
    vi.mocked(setup.api.getControlNode)
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveOlder = resolve;
          }),
      )
      .mockResolvedValueOnce({ status: "ready", activeModelId: "other", operationId: null, error: null });
    const terminal = (id: string, modelId: string) => ({
      sequence: id === "old" ? 2 : 3,
      operation: {
        id,
        kind: "load" as const,
        status: "succeeded" as const,
        modelId,
        progress: null,
        error: null,
        createdAtUnixMs: 1,
        updatedAtUnixMs: id === "old" ? 2 : 3,
      },
    });
    act(() => setup.controlCallbacks()?.onEvent(terminal("old", "gemma")));
    act(() => setup.controlCallbacks()?.onEvent(terminal("new", "other")));
    expect(await screen.findByText("Active model: other")).toBeInTheDocument();
    resolveOlder({ status: "ready", activeModelId: "gemma", operationId: null, error: null });
    await Promise.resolve();
    expect(screen.getByText("Active model: other")).toBeInTheDocument();
  });

  it("keeps chat blocked until terminal lifecycle truth finishes reconciling", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    let resolveNode!: (value: Awaited<ReturnType<ChatScreenServices["getControlNode"]>>) => void;
    vi.mocked(setup.api.getControlNode).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveNode = resolve;
        }),
    );
    act(() =>
      setup.controlCallbacks()?.onEvent({
        sequence: 2,
        operation: {
          id: "load-2",
          kind: "load",
          status: "succeeded",
          modelId: "other",
          progress: null,
          error: null,
          createdAtUnixMs: 1,
          updatedAtUnixMs: 2,
        },
      }),
    );
    expect(screen.getByLabelText("Message")).toBeDisabled();
    await vi.waitFor(() => expect(setup.api.getControlNode).toHaveBeenCalledTimes(2));
    resolveNode({ status: "ready", activeModelId: "other", operationId: null, error: null });
    await vi.waitFor(() => expect(screen.getByLabelText("Message")).toBeEnabled());
  });

  it.each([
    ["unloaded", /No model is loaded/i],
    ["loading", /loading a model/i],
    ["unloading", /unloading the active model/i],
    ["error", /reported an error/i],
    ["recovery_required", /Recovery required/i],
  ] as const)("blocks chat when authoritative node status is %s", async (status, reason) => {
    const setup = services();
    vi.mocked(setup.api.getControlNode).mockResolvedValue({
      status,
      activeModelId: status === "loading" ? "gemma" : null,
      operationId: null,
      error: null,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("status")).toHaveTextContent(reason);
    expect(screen.getByRole("status")).not.toHaveTextContent("Disconnected");
    expect(screen.getByLabelText("Message")).toBeDisabled();
  });

  it("keeps a running switch blocked and aborts its polling on window close", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getOperation).mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "running",
      modelId: "other",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const picker = await screen.findByRole("combobox", { name: "Choose model" });
    await user.selectOptions(picker, "other");
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    await vi.waitFor(() => expect(setup.api.getOperation).toHaveBeenCalled());
    const signal = vi.mocked(setup.api.getOperation).mock.calls[0][3]?.signal;
    expect(picker).toBeDisabled();
    window.dispatchEvent(new Event("beforeunload"));
    expect(signal?.aborted).toBe(true);
    expect(setup.api.loadModel).toHaveBeenCalledOnce();
  });

  it.each([
    [{ kind: "cancelled" as const }, "Cancelled"],
    [{ kind: "completed" as const }, "Completed"],
    [{ kind: "error" as const, message: "The Loxa node returned HTTP 500." }, "The Loxa node returned HTTP 500."],
    [{ kind: "error" as const, message: "The Loxa node returned a malformed chat stream." }, "malformed chat stream"],
  ])("announces terminal state %#", async (terminal, text) => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    setup.callbacks()?.onTerminal(terminal);
    expect(await screen.findByRole("status")).toHaveTextContent(text);
    expect(screen.getByLabelText("Message")).toBeEnabled();
  });

  it("preserves cancelled partial output and safely starts a later turn", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "First");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    setup.callbackHistory[0].onDelta("Partial answer");
    setup.callbackHistory[0].onTerminal({ kind: "cancelled" });

    expect(await screen.findByText("Partial answer")).toBeInTheDocument();
    expect(screen.getByText("Turn cancelled")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Message"));
    await user.type(screen.getByLabelText("Message"), "Second");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    expect(setup.api.createChatStream).toHaveBeenCalledTimes(2);
    expect(setup.api.createChatStream).toHaveBeenLastCalledWith(
      "http://127.0.0.1:8080",
      { model: "loxa", messages: [{ role: "user", content: "Second" }] },
      expect.any(Object),
    );
    expect(screen.getAllByText("gemma")).not.toHaveLength(0);
  });

  it("disables chat with a clear reason when the backend reports text chat unsupported", async () => {
    const setup = services();
    vi.mocked(setup.api.getCapabilities).mockResolvedValue({
      documentInput: false,
      documentInputReason: "Documents are unavailable.",
      textChat: false,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    expect(await screen.findAllByText(/text chat is not supported by this node/i)).not.toHaveLength(0);
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Send message" })).toBeDisabled();
  });

  it("cancels on demand and disposes on unmount", async () => {
    const user = userEvent.setup();
    const setup = services();
    const view = render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    await user.click(screen.getByRole("button", { name: "Stop response" }));
    expect(setup.handle.cancel).toHaveBeenCalledOnce();
    view.unmount();
    expect(setup.handle.dispose).toHaveBeenCalledOnce();
  });

  it("disposes chat and control streams exactly once across window close and unmount", async () => {
    const user = userEvent.setup();
    const setup = services();
    const view = render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    window.dispatchEvent(new Event("beforeunload"));
    view.unmount();

    expect(setup.handle.dispose).toHaveBeenCalledOnce();
    expect(setup.controlHandles[0].dispose).toHaveBeenCalledOnce();
  });

  it("keeps document attachment visible but disabled with the capability-derived reason", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    const attachment = await screen.findByRole("button", { name: "Attach document" });
    expect(attachment).not.toBeDisabled();
    expect(attachment).toHaveAttribute("aria-disabled", "true");
    expect(attachment).toHaveAttribute("aria-describedby", "attachment-support-reason");
    expect(screen.getByRole("tooltip")).toHaveTextContent("Document input is not supported by this model and backend.");
    expect(setup.api.getCapabilities).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(setup.api.readControlToken).toHaveBeenCalledWith("http://127.0.0.1:8080");
  });

  it("omits the document tooltip when no capability reason exists", async () => {
    const setup = services();
    vi.mocked(setup.api.getCapabilities).mockResolvedValue({
      documentInput: false,
      documentInputReason: "",
      textChat: true,
    });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    const attachment = await screen.findByRole("button", { name: "Attach document" });
    await waitFor(() => expect(setup.api.getCapabilities).toHaveBeenCalled());
    expect(attachment).not.toHaveAttribute("aria-describedby");
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("puts authoritative active-model status in the Chat header without duplicating it under the selector", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    const picker = await screen.findByRole("combobox", { name: "Choose model" });
    const header = screen.getByRole("heading", { name: "Chat" }).closest("header");
    expect(header).not.toBeNull();
    expect(within(header!).getByText("Active model: gemma")).toBeVisible();
    expect(picker).toHaveAccessibleName("Choose model");
    expect(screen.queryByText(/Active:\s*gemma/)).not.toBeInTheDocument();
  });

  it("aborts status, model, and capability checks before window close and suppresses late results", async () => {
    const setup = services();
    let resolveStatus!: (value: Awaited<ReturnType<ChatScreenServices["getStatus"]>>) => void;
    let resolveModels!: (value: Awaited<ReturnType<ChatScreenServices["getModels"]>>) => void;
    let resolveCapabilities!: (value: Awaited<ReturnType<ChatScreenServices["getCapabilities"]>>) => void;
    vi.mocked(setup.api.getStatus).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveStatus = resolve;
        }),
    );
    vi.mocked(setup.api.getModels).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveModels = resolve;
        }),
    );
    vi.mocked(setup.api.getCapabilities).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveCapabilities = resolve;
        }),
    );
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await vi.waitFor(() => expect(setup.api.getCapabilities).toHaveBeenCalledOnce());
    const statusSignal = vi.mocked(setup.api.getStatus).mock.calls[0][1]?.signal;
    const modelSignal = vi.mocked(setup.api.getModels).mock.calls[0][1]?.signal;
    const capabilitySignal = vi.mocked(setup.api.getCapabilities).mock.calls[0][2]?.signal;

    window.dispatchEvent(new Event("beforeunload"));
    expect(statusSignal?.aborted).toBe(true);
    expect(modelSignal?.aborted).toBe(true);
    expect(capabilitySignal?.aborted).toBe(true);
    resolveStatus({
      node_id: "node-7",
      health: "ready",
      model: "loxa",
      engine: { name: "llama.cpp", version: "b9999" },
      runtime_model: "late-model",
      profile: "default",
    });
    resolveModels({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] });
    resolveCapabilities({ documentInput: false, documentInputReason: "late", textChat: true });
    await Promise.resolve();
    expect(screen.getByRole("status")).toHaveTextContent("Checking node");
    expect(screen.queryByText("late-model")).not.toBeInTheDocument();
  });
});
