import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { App, type AppServices } from "./App";
import type { ControlStreamTerminal } from "../control/events";

function services(): AppServices {
  return {
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue({
        ownership: "none",
        endpoint: "http://127.0.0.1:8080",
        childRunning: false,
        error: null,
      }),
      start: vi.fn(),
      attach: vi.fn(),
      stop: vi.fn(),
    },
    getStatus: vi.fn(),
    getModels: vi.fn(),
    getCapabilities: vi.fn(),
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    getControlNode: vi.fn().mockResolvedValue({ status: "unloaded", activeModelId: null, operationId: null, error: null }),
    getInventory: vi.fn().mockResolvedValue([]),
    downloadModel: vi.fn(),
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
    await screen.findByText("Disconnected");

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
});
