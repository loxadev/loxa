import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ChatScreen, type ChatScreenServices } from "./ChatScreen";
import type { StreamCallbacks, StreamHandle } from "./streamChat";

function services(ready = true) {
  let callbacks: StreamCallbacks | undefined;
  const callbackHistory: StreamCallbacks[] = [];
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
    createChatStream: vi.fn((_endpoint, _request, next) => {
      callbacks = next;
      callbackHistory.push(next);
      return handle;
    }),
  };
  return { api, handle, callbacks: () => callbacks, callbackHistory };
}

describe("ChatScreen", () => {
  it("shows disconnected explicitly and does not offer send", async () => {
    const { api } = services(false);
    render(<ChatScreen services={api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByRole("status")).toHaveTextContent("Disconnected");
    expect(screen.getByRole("button", { name: "Send message" })).toBeDisabled();
  });

  it("loads the public model alias and streams incremental output", async () => {
    const user = userEvent.setup();
    const setup = services();
    render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    expect(await screen.findByText("loxa")).toHaveClass("technical-value");
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
    expect(screen.getByRole("combobox", { name: "Active model" })).toHaveValue("gemma");
    expect(screen.getByRole("combobox", { name: "Active model" })).toBeDisabled();
    expect(screen.getByText(/load and switch controls are not available yet/i)).toBeInTheDocument();
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
    await screen.findByText("loxa");
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
    await screen.findByText("loxa");
    await user.type(screen.getByLabelText("Message"), "First");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    setup.callbackHistory[0].onDelta("Partial answer");
    setup.callbackHistory[0].onTerminal({ kind: "cancelled" });

    expect(await screen.findByText("Partial answer")).toBeInTheDocument();
    expect(screen.getByText("Cancelled")).toBeInTheDocument();
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

    expect(await screen.findByText(/text chat is not supported by this node/i)).toBeInTheDocument();
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Send message" })).toBeDisabled();
  });

  it("cancels on demand and disposes on unmount", async () => {
    const user = userEvent.setup();
    const setup = services();
    const view = render(<ChatScreen services={setup.api} endpoint="http://127.0.0.1:8080" />);
    await screen.findByText("loxa");
    await user.type(screen.getByLabelText("Message"), "Hello");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    await user.click(screen.getByRole("button", { name: "Cancel response" }));
    expect(setup.handle.cancel).toHaveBeenCalledOnce();
    view.unmount();
    expect(setup.handle.dispose).toHaveBeenCalledOnce();
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
