import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { NodeScreen, type BootstrapSnapshot, type NodeScreenServices } from "./NodeScreen";

const endpoint = "http://127.0.0.1:8080";
const readyStatus = {
  node_id: "node-7",
  health: "ready" as const,
  model: "loxa" as const,
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
};

function snapshot(overrides: Partial<BootstrapSnapshot> = {}): BootstrapSnapshot {
  return { ownership: "none", endpoint, childRunning: false, error: null, ...overrides };
}

function services(initial = snapshot()): NodeScreenServices {
  return {
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue(initial),
      start: vi.fn().mockResolvedValue(snapshot({ ownership: "owned", childRunning: true })),
      attach: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      stop: vi.fn().mockResolvedValue(snapshot()),
    },
    getStatus: vi.fn().mockResolvedValue(readyStatus),
    copyText: vi.fn().mockResolvedValue(undefined),
  };
}

describe("NodeScreen", () => {
  it("renders explicit disconnected state and only safe actions", async () => {
    render(<NodeScreen services={services()} />);
    expect(await screen.findByText("Disconnected")).toBeInTheDocument();
    expect(screen.getByText("No node ownership")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start node" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Attach or retry" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Stop node" })).not.toBeInTheDocument();
  });

  it("renders every transitional and failure state as live text", async () => {
    const cases = [
      ["connecting", "Connecting"],
      ["starting", "Starting"],
      ["stopping", "Stopping"],
      ["recovery-required", "Recovery required"],
      ["error", "Error"],
    ] as const;
    for (const [phase, label] of cases) {
      const view = render(
        <NodeScreen
          services={services(snapshot({ error: phase === "recovery-required" ? "Ownership could not be proven." : null }))}
          initialPhase={phase}
        />,
      );
      expect(await screen.findByRole("status")).toHaveTextContent(label);
      view.unmount();
    }
  });

  it("shows ready only from authoritative status and exposes selectable technical fields", async () => {
    render(<NodeScreen services={services(snapshot({ ownership: "attached" }))} />);
    expect(await screen.findByRole("status")).toHaveTextContent("Ready");
    expect(screen.getByText("Externally attached")).toBeInTheDocument();
    for (const value of [endpoint, "node-7", "llama.cpp", "b9999", "gemma-3-4b-it-q4", "default"]) {
      expect(screen.getByText(value)).toHaveClass("technical-value");
    }
    expect(screen.queryByRole("button", { name: "Stop node" })).not.toBeInTheDocument();
  });

  it("starts through typed bootstrap, announces progress, and permits stop only for ownership", async () => {
    const user = userEvent.setup();
    const api = services();
    render(<NodeScreen services={api} />);
    await screen.findByText("Disconnected");
    await user.click(screen.getByRole("button", { name: "Start node" }));
    expect(api.bootstrap.start).toHaveBeenCalledWith({ endpoint });
    expect(await screen.findByText("App-owned node")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop node" })).toBeEnabled();
  });

  it("copies the stable endpoint and announces feedback", async () => {
    const user = userEvent.setup();
    const api = services();
    render(<NodeScreen services={api} />);
    await screen.findByText("Disconnected");
    await user.click(screen.getByRole("button", { name: "Copy endpoint" }));
    expect(api.copyText).toHaveBeenCalledWith(endpoint);
    expect(screen.getByText("Endpoint copied")).toHaveAttribute("aria-live", "polite");
  });

  it("applies the canonical 44px target contract", async () => {
    render(<NodeScreen services={services()} />);
    await screen.findByText("Disconnected");
    expect(screen.getByRole("button", { name: "Start node" })).toHaveClass("interactive-target");
  });
});
