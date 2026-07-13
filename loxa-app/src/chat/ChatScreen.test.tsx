import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ChatScreen, type ChatScreenServices } from "./ChatScreen";
import { ChatTranscript, type ChatTurn } from "./ChatTranscript";
import type { StreamCallbacks, StreamHandle } from "./streamChat";
import type { ControlStreamCallbacks, ControlStreamHandle } from "../control/events";

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
    getStatus: vi.fn().mockResolvedValue(ready ? {
      node_id: "node-7", health: "ready", model: "loxa",
      engine: { name: "llama.cpp", version: "b9999" }, runtime_model: "gemma", profile: "default",
    } : { node_id: "node-7", health: "unavailable", model: "loxa", engine: null, runtime_model: null, profile: null }),
    getModels: vi.fn().mockResolvedValue({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] }),
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    getCapabilities: vi.fn().mockResolvedValue({
      documentInput: false,
      documentInputReason: "Document input is not supported by this model and backend.",
      textChat: true,
    }),
    getControlNode: vi.fn().mockResolvedValue({ status: "ready", activeModelId: "gemma", operationId: null, error: null }),
    getInventory: vi.fn().mockResolvedValue([
      { id: "gemma", repo: "loxa/gemma", revision: "rev", filename: "gemma.gguf", sha256: "ab".repeat(32), sizeBytes: 1, license: "Apache-2.0", params: "4B", quant: "Q4", minFreeMemoryGiB: 1, artifact: { kind: "downloaded" }, compatibility: { compatible: true, reason: "Compatible" }, engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" } },
      { id: "other", repo: "loxa/other", revision: "rev", filename: "other.gguf", sha256: "cd".repeat(32), sizeBytes: 1, license: "Apache-2.0", params: "4B", quant: "Q4", minFreeMemoryGiB: 1, artifact: { kind: "downloaded" }, compatibility: { compatible: true, reason: "Compatible" }, engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" } },
    ]),
    loadModel: vi.fn().mockResolvedValue({ operationId: "op-load" }),
    getOperation: vi.fn().mockResolvedValue({ id: "op-load", kind: "load", status: "succeeded", modelId: "other", progress: null, error: null, createdAtUnixMs: 1, updatedAtUnixMs: 2 }),
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
  };
  return { api, handle, callbacks: () => callbacks, controlCallbacks: () => controlCallbacks, callbackHistory, controlCallbackHistory, controlHandles };
}

describe("ChatScreen", () => {
  it("uses a named scrolling transcript and a final-row message composer", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);

    expect(await screen.findByRole("log", { name: "Conversation" })).toBeInTheDocument();
    expect(screen.getByRole("form", { name: "Message composer" })).toBeInTheDocument();
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

  it("defines a canonical responsive and accessible Chat module contract", () => {
    const path = resolve(process.cwd(), "src/chat/ChatScreen.module.css");
    const css = existsSync(path) ? readFileSync(path, "utf8") : "";
    const canonicalPath = resolve(process.cwd(), "src/styles/loxa.css");
    const canonicalCss = existsSync(canonicalPath) ? readFileSync(canonicalPath, "utf8") : "";
    const canonicalTokens = new Set(Array.from(canonicalCss.matchAll(/(--loxa-[\w-]+)\s*:/g), ([, token]) => token));
    const chatTokens = new Set(Array.from(css.matchAll(/var\((--loxa-[\w-]+)/g), ([, token]) => token));

    expect(css).toContain("grid-template-rows");
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

    act(() => setup.controlCallbacks()?.onSnapshot({
      cursor: 4,
      cursorGap: false,
      operations: [{ id: "load-4", kind: "load", status: "running", modelId: "other", progress: null, error: null, createdAtUnixMs: 1, updatedAtUnixMs: 2 }],
      events: [],
    }));

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

    act(() => setup.controlCallbackHistory[0].onTerminal({ kind: "error", cursor: 7, message: "Live model updates disconnected." }));
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
    expect(screen.getByRole("status")).toHaveTextContent("Ready");
    expect(screen.getByLabelText("Message")).toBeEnabled();
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeEnabled();
  });

  it("auto-scrolls near the bottom but preserves a reader's scroll-away position", () => {
    const first: ChatTurn = { id: 1, model: "gemma", prompt: "Hello", response: "One", status: "streaming", error: "" };
    const view = render(<ChatTranscript turns={[first]} emptyMessage="Empty" />);
    const transcript = screen.getByRole("log", { name: "Conversation" });
    Object.defineProperty(transcript, "scrollHeight", { configurable: true, value: 1_000 });
    Object.defineProperty(transcript, "clientHeight", { configurable: true, value: 200 });
    transcript.scrollTop = 760;
    fireEvent.scroll(transcript);
    view.rerender(<ChatTranscript turns={[{ ...first, response: "One two" }]} emptyMessage="Empty" />);
    expect(transcript.scrollTop).toBe(1_000);

    transcript.scrollTop = 200;
    fireEvent.scroll(transcript);
    Object.defineProperty(transcript, "scrollHeight", { configurable: true, value: 1_200 });
    view.rerender(<ChatTranscript turns={[{ ...first, response: "One two three" }]} emptyMessage="Empty" />);
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
    expect(await screen.findByText("Hello")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Streaming");
    expect(screen.getByRole("combobox", { name: "Choose model" })).toHaveValue("gemma");
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeDisabled();
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
    expect(screen.getByText("gemma", { selector: ".technical-value" })).toBeInTheDocument();
    expect(setup.api.loadModel).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    expect(setup.api.loadModel).toHaveBeenCalledWith("http://127.0.0.1:8080", "ab".repeat(32), "other", expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(await screen.findByText("other", { selector: ".technical-value" })).toBeInTheDocument();
  });

  it("loads explicitly from an unloaded node and enables chat only after ready confirmation", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getControlNode)
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma", operationId: null, error: null });
    vi.mocked(setup.api.getOperation).mockResolvedValue({ id: "op-load", kind: "load", status: "succeeded", modelId: "gemma", progress: null, error: null, createdAtUnixMs: 1, updatedAtUnixMs: 2 });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("status")).toHaveTextContent("No model is loaded");
    expect(screen.getByRole("status")).not.toHaveTextContent("Disconnected");
    expect(screen.getByLabelText("Message")).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Load gemma" }));
    expect(await screen.findByRole("status")).toHaveTextContent("Ready");
    expect(screen.getByLabelText("Message")).toBeEnabled();
  });

  it("keeps the previous active model when a switch fails", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getOperation).mockResolvedValue({ id: "op-load", kind: "load", status: "failed", modelId: "other", progress: null, error: "readiness failed", createdAtUnixMs: 1, updatedAtUnixMs: 2 });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await user.selectOptions(await screen.findByRole("combobox", { name: "Choose model" }), "other");
    await user.click(screen.getByRole("button", { name: "Switch to other" }));
    expect(await screen.findByRole("status")).toHaveTextContent("readiness failed");
    expect(screen.getByText("gemma", { selector: ".technical-value" })).toBeInTheDocument();
  });

  it("blocks model switching while the node reports an active operation", async () => {
    const setup = services();
    vi.mocked(setup.api.getControlNode).mockResolvedValue({ status: "loading", activeModelId: "gemma", operationId: "op-existing", error: null });
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
      .mockImplementationOnce(() => new Promise((resolve) => { resolveOlder = resolve; }))
      .mockResolvedValueOnce({ status: "ready", activeModelId: "other", operationId: null, error: null });
    const terminal = (id: string, modelId: string) => ({ sequence: id === "old" ? 2 : 3, operation: { id, kind: "load" as const, status: "succeeded" as const, modelId, progress: null, error: null, createdAtUnixMs: 1, updatedAtUnixMs: id === "old" ? 2 : 3 } });
    act(() => setup.controlCallbacks()?.onEvent(terminal("old", "gemma")));
    act(() => setup.controlCallbacks()?.onEvent(terminal("new", "other")));
    expect(await screen.findByText("other", { selector: ".technical-value" })).toBeInTheDocument();
    resolveOlder({ status: "ready", activeModelId: "gemma", operationId: null, error: null });
    await Promise.resolve();
    expect(screen.getByText("other", { selector: ".technical-value" })).toBeInTheDocument();
  });

  it("keeps chat blocked until terminal lifecycle truth finishes reconciling", async () => {
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByRole("combobox", { name: "Choose model" });
    let resolveNode!: (value: Awaited<ReturnType<ChatScreenServices["getControlNode"]>>) => void;
    vi.mocked(setup.api.getControlNode).mockImplementationOnce(() => new Promise((resolve) => { resolveNode = resolve; }));
    act(() => setup.controlCallbacks()?.onEvent({ sequence: 2, operation: { id: "load-2", kind: "load", status: "succeeded", modelId: "other", progress: null, error: null, createdAtUnixMs: 1, updatedAtUnixMs: 2 } }));
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
    vi.mocked(setup.api.getControlNode).mockResolvedValue({ status, activeModelId: status === "loading" ? "gemma" : null, operationId: null, error: null });
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("status")).toHaveTextContent(reason);
    expect(screen.getByRole("status")).not.toHaveTextContent("Disconnected");
    expect(screen.getByLabelText("Message")).toBeDisabled();
  });

  it("keeps a running switch blocked and aborts its polling on window close", async () => {
    const user = userEvent.setup();
    const setup = services();
    vi.mocked(setup.api.getOperation).mockResolvedValue({ id: "op-load", kind: "load", status: "running", modelId: "other", progress: null, error: null, createdAtUnixMs: 1, updatedAtUnixMs: 2 });
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
    expect(attachment).toBeDisabled();
    expect(attachment).toHaveAttribute("aria-describedby", "attachment-support-reason");
    expect(screen.getByText("Document input is not supported by this model and backend.")).toBeInTheDocument();
    expect(setup.api.getCapabilities).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(setup.api.readControlToken).toHaveBeenCalledWith("http://127.0.0.1:8080");
  });

  it("aborts status, model, and capability checks before window close and suppresses late results", async () => {
    const setup = services();
    let resolveStatus!: (value: Awaited<ReturnType<ChatScreenServices["getStatus"]>>) => void;
    let resolveModels!: (value: Awaited<ReturnType<ChatScreenServices["getModels"]>>) => void;
    let resolveCapabilities!: (value: Awaited<ReturnType<ChatScreenServices["getCapabilities"]>>) => void;
    vi.mocked(setup.api.getStatus).mockImplementation(() => new Promise((resolve) => { resolveStatus = resolve; }));
    vi.mocked(setup.api.getModels).mockImplementation(() => new Promise((resolve) => { resolveModels = resolve; }));
    vi.mocked(setup.api.getCapabilities).mockImplementation(() => new Promise((resolve) => { resolveCapabilities = resolve; }));
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
      node_id: "node-7", health: "ready", model: "loxa",
      engine: { name: "llama.cpp", version: "b9999" }, runtime_model: "late-model", profile: "default",
    });
    resolveModels({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] });
    resolveCapabilities({ documentInput: false, documentInputReason: "late", textChat: true });
    await Promise.resolve();
    expect(screen.getByRole("status")).toHaveTextContent("Checking node");
    expect(screen.queryByText("late-model")).not.toBeInTheDocument();
  });
});
