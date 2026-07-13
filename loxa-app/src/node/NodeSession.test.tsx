import { StrictMode } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import {
  NodeSessionProvider,
  useNodeSession,
  type BootstrapSnapshot,
  type NodeSessionServices,
} from "./NodeSession";

const endpoint = "http://127.0.0.1:8080";

const unavailableStatus = {
  node_id: "node-7",
  health: "unavailable" as const,
  model: "loxa" as const,
  engine: null,
  runtime_model: null,
  profile: null,
};

const readyStatus = {
  node_id: "node-7",
  health: "ready" as const,
  model: "loxa" as const,
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
};

function snapshot(overrides: Partial<BootstrapSnapshot> = {}): BootstrapSnapshot {
  return {
    ownership: "owned",
    endpoint,
    childRunning: true,
    error: null,
    ...overrides,
  };
}

function services(overrides: Partial<NodeSessionServices> = {}): NodeSessionServices {
  return {
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue(snapshot({ ownership: "none", childRunning: false })),
      start: vi.fn().mockResolvedValue(snapshot()),
      attach: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      stop: vi.fn().mockResolvedValue(snapshot({ ownership: "none", childRunning: false })),
    },
    getStatus: vi.fn().mockResolvedValue(unavailableStatus),
    ...overrides,
  };
}

function Probe() {
  const session = useNodeSession();
  return (
    <div>
      <output aria-label="phase">{session.phase}</output>
      <output aria-label="ownership">{session.ownership}</output>
      <output aria-label="endpoint">{session.endpoint}</output>
      <output aria-label="error">{session.error ?? ""}</output>
      <button type="button" onClick={() => void session.retry()}>Retry</button>
      <button type="button" onClick={() => void session.stop()}>Stop</button>
    </div>
  );
}

describe("NodeSessionProvider", () => {
  it("deduplicates automatic ensure-start under React Strict Mode and publishes unloaded", async () => {
    const api = services();

    render(
      <StrictMode>
        <NodeSessionProvider services={api} endpoint={endpoint}>
          <Probe />
        </NodeSessionProvider>
      </StrictMode>,
    );

    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    expect(api.bootstrap.start).toHaveBeenCalledTimes(1);
    expect(api.bootstrap.start).toHaveBeenCalledWith({ endpoint });
    expect(api.getStatus).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText("ownership")).toHaveTextContent("owned");
  });

  it("publishes ready only after the authoritative status probe", async () => {
    const api = services({ getStatus: vi.fn().mockResolvedValue(readyStatus) });
    render(<NodeSessionProvider services={api} endpoint={endpoint}><Probe /></NodeSessionProvider>);

    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(screen.getByLabelText("endpoint")).toHaveTextContent(endpoint);
  });

  it("publishes an actionable error and retries the ensure operation", async () => {
    const start = vi.fn()
      .mockRejectedValueOnce(new Error("Private node exited before readiness."))
      .mockResolvedValueOnce(snapshot());
    const api = services({ bootstrap: { ...services().bootstrap, start } });
    const user = userEvent.setup();
    render(<NodeSessionProvider services={api} endpoint={endpoint}><Probe /></NodeSessionProvider>);

    expect(await screen.findByText("error")).toBeInTheDocument();
    expect(screen.getByLabelText("error")).toHaveTextContent("Private node exited before readiness.");

    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    expect(start).toHaveBeenCalledTimes(2);
  });

  it("retains exact native ownership when the public status probe fails", async () => {
    const api = services({ getStatus: vi.fn().mockRejectedValue(new Error("Public status unavailable.")) });
    const user = userEvent.setup();
    render(<NodeSessionProvider services={api} endpoint={endpoint}><Probe /></NodeSessionProvider>);

    expect(await screen.findByText("error")).toBeInTheDocument();
    expect(screen.getByLabelText("ownership")).toHaveTextContent("owned");
    await user.click(screen.getByRole("button", { name: "Stop" }));
    await waitFor(() => expect(api.bootstrap.stop).toHaveBeenCalledTimes(1));
  });

  it("stops only an app-owned node", async () => {
    const owned = services();
    const user = userEvent.setup();
    const view = render(<NodeSessionProvider services={owned} endpoint={endpoint}><Probe /></NodeSessionProvider>);
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Stop" }));
    await waitFor(() => expect(owned.bootstrap.stop).toHaveBeenCalledTimes(1));
    expect(screen.getByLabelText("ownership")).toHaveTextContent("none");
    view.unmount();

    const attached = services({
      bootstrap: {
        ...services().bootstrap,
        start: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      },
    });
    const attachedView = render(<NodeSessionProvider services={attached} endpoint={endpoint}><Probe /></NodeSessionProvider>);
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Stop" }));
    expect(attached.bootstrap.stop).not.toHaveBeenCalled();
    expect(screen.getByLabelText("ownership")).toHaveTextContent("attached");
    attachedView.unmount();
    expect(attached.bootstrap.stop).not.toHaveBeenCalled();
  });

  it("aborts its authoritative probe on unmount", async () => {
    let signal: AbortSignal | undefined;
    const api = services({
      getStatus: vi.fn((_endpoint, options) => {
        signal = options?.signal;
        return new Promise<never>(() => undefined);
      }),
    });
    const view = render(<NodeSessionProvider services={api} endpoint={endpoint}><Probe /></NodeSessionProvider>);
    await waitFor(() => expect(api.getStatus).toHaveBeenCalledTimes(1));

    act(() => view.unmount());
    expect(signal?.aborted).toBe(true);
  });
});
