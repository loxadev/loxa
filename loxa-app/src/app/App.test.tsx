import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { App, type AppServices } from "./App";
import type { ControlStreamTerminal } from "../control/events";
import type { BootstrapSnapshot } from "../node/NodeSession";
import type { NodeStatus } from "../node/contracts";

function services(): AppServices {
  return {
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue({
        ownership: "none",
        endpoint: "http://127.0.0.1:8080",
        childRunning: false,
        error: null,
      }),
      start: vi.fn().mockResolvedValue({
        ownership: "owned",
        endpoint: "http://127.0.0.1:8080",
        childRunning: true,
        error: null,
      }),
      attach: vi.fn(),
      stop: vi.fn(),
    },
    getStatus: vi.fn().mockResolvedValue({
      node_id: "node-7",
      health: "unavailable",
      model: "loxa",
      engine: null,
      runtime_model: null,
      profile: null,
    }),
    getModels: vi.fn().mockResolvedValue({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] }),
    getCapabilities: vi.fn().mockResolvedValue({ documentInput: false, documentInputReason: "Not supported.", textChat: true }),
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    getControlNode: vi.fn().mockResolvedValue({ status: "unloaded", activeModelId: null, operationId: null, error: null }),
    getInventory: vi.fn().mockResolvedValue([]),
    downloadModel: vi.fn(),
    loadModel: vi.fn(),
    unloadModel: vi.fn(),
    getOperation: vi.fn(),
    cancelOperation: vi.fn(),
    createControlEventStream: vi.fn(() => ({
      cancel: vi.fn(),
      dispose: vi.fn(),
      finished: new Promise<ControlStreamTerminal>(() => undefined),
    })),
    createChatStream: vi.fn(),
    copyText: vi.fn(),
  };
}

describe("App", () => {
  it("opens on Node, keeps Models primary, and exposes Chat as the secondary tool", async () => {
    const user = userEvent.setup();
    render(<App services={services()} />);

    expect(await screen.findByRole("heading", { name: "Node" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Node" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "Models" }));
    expect(screen.getByRole("heading", { name: "Models" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Models" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "Chat" }));
    expect(screen.getByRole("heading", { name: "Chat" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Chat" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "Settings" }));
    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("aria-current", "page");
  });

  it("has a logical keyboard focus order and no unsupported controls", async () => {
    const user = userEvent.setup();
    render(<App services={services()} />);
    await screen.findByText("Node ready — no model loaded");

    await user.tab();
    expect(screen.getByRole("link", { name: "Node" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Models" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Chat" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Settings" })).toHaveFocus();

    expect(screen.queryByRole("button", { name: /load|unload|switch|download/i })).not.toBeInTheDocument();
  });

  it("gates route clients until native bootstrap and the public status probe succeed", async () => {
    const api = services();
    let resolveStart!: (snapshot: BootstrapSnapshot) => void;
    let resolveStatus!: (status: NodeStatus) => void;
    api.bootstrap.start = vi.fn(() => new Promise<BootstrapSnapshot>((resolve) => { resolveStart = resolve; }));
    api.getStatus = vi.fn(() => new Promise<NodeStatus>((resolve) => { resolveStatus = resolve; }));
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(screen.getByRole("link", { name: "Models" }));
    expect(screen.getByRole("heading", { name: "Models" })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Starting the private Loxa node");
    expect(api.readControlToken).not.toHaveBeenCalled();

    await act(async () => {
      resolveStart({
        ownership: "owned",
        endpoint: "http://127.0.0.1:8080",
        childRunning: true,
        error: null,
      });
    });

    expect(api.readControlToken).not.toHaveBeenCalled();

    await act(async () => {
      resolveStatus({
        node_id: "node-7",
        health: "unavailable",
        model: "loxa",
        engine: null,
        runtime_model: null,
        profile: null,
      });
    });

    await waitFor(() => expect(api.readControlToken).toHaveBeenCalled());
  });

  it("closes route clients while an owned node is stopping and until retry proves it again", async () => {
    const api = services();
    let resolveStop!: (snapshot: BootstrapSnapshot) => void;
    api.bootstrap.stop = vi.fn(() => new Promise<BootstrapSnapshot>((resolve) => { resolveStop = resolve; }));
    const user = userEvent.setup();
    render(<App services={api} />);

    const stop = await screen.findByRole("button", { name: "Stop node" });
    await user.click(stop);
    await user.click(screen.getByRole("link", { name: "Models" }));
    expect(screen.getByRole("status")).toHaveTextContent(/stopping|node/i);
    expect(api.readControlToken).not.toHaveBeenCalled();

    await act(async () => {
      resolveStop({
        ownership: "none",
        endpoint: "http://127.0.0.1:8080",
        childRunning: false,
        error: null,
      });
    });
    expect(await screen.findByRole("button", { name: "Retry node startup" })).toBeEnabled();
    expect(api.readControlToken).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Retry node startup" }));
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalled());
  });
});
