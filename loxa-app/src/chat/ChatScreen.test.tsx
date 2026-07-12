import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ChatScreen, type ChatScreenServices } from "./ChatScreen";
import type { StreamCallbacks, StreamHandle } from "./streamChat";

function services(ready = true) {
  let callbacks: StreamCallbacks | undefined;
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
    createChatStream: vi.fn((_endpoint, _request, next) => { callbacks = next; return handle; }),
  };
  return { api, handle, callbacks: () => callbacks };
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
    setup.callbacks()?.onDelta("Hel");
    setup.callbacks()?.onDelta("lo");
    expect(await screen.findByText("Hello")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Streaming");
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
});
