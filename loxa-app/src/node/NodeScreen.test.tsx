import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { NodeScreen, type NodeScreenServices } from "./NodeScreen";
import {
  NodeSessionProvider,
  type BootstrapSnapshot,
  type NodeSessionServices,
} from "./NodeSession";

const endpoint = "http://127.0.0.1:8080";
const readyStatus = {
  node_id: "node-7",
  health: "ready" as const,
  model: "loxa" as const,
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
};
const unloadedStatus = {
  node_id: "node-7",
  health: "unavailable" as const,
  model: "loxa" as const,
  engine: null,
  runtime_model: null,
  profile: null,
};

function snapshot(overrides: Partial<BootstrapSnapshot> = {}): BootstrapSnapshot {
  return { ownership: "owned", endpoint, childRunning: true, error: null, ...overrides };
}

function services(overrides: Partial<NodeSessionServices & NodeScreenServices> = {}) {
  return {
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue(snapshot({ ownership: "none", childRunning: false })),
      start: vi.fn().mockResolvedValue(snapshot()),
      attach: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      stop: vi.fn().mockResolvedValue(snapshot({ ownership: "none", childRunning: false })),
    },
    getStatus: vi.fn().mockResolvedValue(unloadedStatus),
    copyText: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

function renderNode(api = services()) {
  return {
    api,
    ...render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <NodeScreen services={api} />
      </NodeSessionProvider>,
    ),
  };
}

describe("NodeScreen", () => {
  it("automatically ensures the node and renders unloaded as a successful state", async () => {
    const { api } = renderNode();
    expect(await screen.findByText("Node ready — no model loaded")).toBeInTheDocument();
    expect(api.bootstrap.start).toHaveBeenCalledWith({ endpoint });
    expect(screen.getByText("App-owned node")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop node" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: /attach/i })).not.toBeInTheDocument();
  });

  it("renders starting and recovery-required as live state", async () => {
    const pending = new Promise<BootstrapSnapshot>(() => undefined);
    const first = renderNode(services({
      bootstrap: { ...services().bootstrap, start: vi.fn(() => pending) },
    }));
    expect(await screen.findByRole("status")).toHaveTextContent("Starting");
    first.unmount();

    renderNode(services({
      bootstrap: {
        ...services().bootstrap,
        start: vi.fn().mockRejectedValue(new Error("Recovery required after unsafe child exit.")),
      },
    }));
    expect(await screen.findByRole("status")).toHaveTextContent("Recovery required");
  });

  it("shows ready only from authoritative status and exposes technical fields", async () => {
    renderNode(services({
      bootstrap: {
        ...services().bootstrap,
        start: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      },
      getStatus: vi.fn().mockResolvedValue(readyStatus),
    }));
    expect(await screen.findByRole("status")).toHaveTextContent("Ready");
    expect(screen.getByText("Externally attached")).toBeInTheDocument();
    for (const value of [endpoint, "node-7", "llama.cpp", "b9999", "gemma-3-4b-it-q4", "default"]) {
      expect(screen.getByText(value)).toHaveClass("technical-value");
    }
    expect(screen.queryByRole("button", { name: "Stop node" })).not.toBeInTheDocument();
  });

  it("stops only the app-owned node", async () => {
    const user = userEvent.setup();
    const { api } = renderNode();
    await screen.findByText("Node ready — no model loaded");
    await user.click(screen.getByRole("button", { name: "Stop node" }));
    expect(api.bootstrap.stop).toHaveBeenCalledTimes(1);
    expect(await screen.findByRole("status")).toHaveTextContent("Disconnected");
    expect(screen.getByRole("button", { name: "Retry node startup" })).toBeEnabled();
  });

  it("keeps safe owned-child recovery available when the public probe fails", async () => {
    renderNode(services({ getStatus: vi.fn().mockRejectedValue(new Error("Public status unavailable.")) }));
    expect(await screen.findByRole("status")).toHaveTextContent("Public status unavailable.");
    expect(screen.getByText("App-owned node")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop node" })).toBeEnabled();
  });

  it("copies the stable endpoint and announces feedback", async () => {
    const user = userEvent.setup();
    const { api } = renderNode();
    await screen.findByText("Node ready — no model loaded");
    await user.click(screen.getByRole("button", { name: "Copy endpoint" }));
    expect(api.copyText).toHaveBeenCalledWith(endpoint);
    expect(screen.getByText("Endpoint copied")).toHaveAttribute("aria-live", "polite");
  });

  it("applies the canonical 44px target contract", async () => {
    renderNode();
    expect(await screen.findByRole("button", { name: "Stop node" })).toHaveClass("interactive-target");
  });
});
