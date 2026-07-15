import { StrictMode, useEffect } from "react";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import {
  NodeSessionProvider,
  useNodeSession,
  type BootstrapSnapshot,
  type NodeSessionServices,
  type NodeSessionValue,
} from "./NodeSession";
import type { ControlStreamCallbacks } from "../control/events";

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
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    createControlEventStream: vi.fn(() => ({
      cancel: vi.fn(),
      dispose: vi.fn(),
      finished: new Promise<never>(() => undefined),
    })),
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
      <output aria-label="runtime model">{session.status?.runtime_model ?? ""}</output>
      <button type="button" onClick={() => void session.retry()}>
        Retry
      </button>
      <button type="button" onClick={() => session.invalidateModelTruth()}>
        Invalidate model truth
      </button>
      <button type="button" onClick={() => session.invalidateModelTruth("op-1")}>
        Track current operation
      </button>
      <button type="button" onClick={() => void session.settleModelMutation("op-1")}>
        Settle current operation
      </button>
      <button type="button" onClick={() => void session.refreshStatus()}>
        Refresh status
      </button>
      <button type="button" onClick={() => void session.stop()}>
        Stop
      </button>
    </div>
  );
}

let capturedSession: NodeSessionValue | null = null;

function CapturingProbe() {
  const session = useNodeSession();
  useEffect(() => {
    capturedSession = session;
  }, [session]);
  return <Probe />;
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
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );

    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(screen.getByLabelText("endpoint")).toHaveTextContent(endpoint);
  });

  it("fails closed while reconciling a model mutation and publishes only the refreshed model", async () => {
    let resolveRefresh!: (status: typeof readyStatus) => void;
    const getStatus = vi
      .fn()
      .mockResolvedValueOnce(readyStatus)
      .mockImplementationOnce(
        () =>
          new Promise<typeof readyStatus>((resolve) => {
            resolveRefresh = resolve;
          }),
      );
    const api = services({ getStatus });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );

    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(screen.getByLabelText("runtime model")).toHaveTextContent("gemma-3-4b-it-q4");

    await user.click(screen.getByRole("button", { name: "Invalidate model truth" }));
    expect(screen.getByLabelText("phase")).toHaveTextContent("reconciling");
    expect(screen.getByLabelText("runtime model")).toBeEmptyDOMElement();

    await user.click(screen.getByRole("button", { name: "Refresh status" }));
    expect(screen.getByLabelText("phase")).toHaveTextContent("reconciling");
    resolveRefresh({ ...readyStatus, runtime_model: "qwen-ready" });
    expect(await screen.findByText("qwen-ready")).toBeInTheDocument();
    expect(screen.getByLabelText("phase")).toHaveTextContent("ready");
  });

  it("publishes an actionable error and retries the ensure operation", async () => {
    const start = vi
      .fn()
      .mockRejectedValueOnce(new Error("Private node exited before readiness."))
      .mockResolvedValueOnce(snapshot());
    const api = services({ bootstrap: { ...services().bootstrap, start } });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );

    expect(await screen.findByText("error")).toBeInTheDocument();
    expect(screen.getByLabelText("error")).toHaveTextContent("Private node exited before readiness.");

    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    expect(start).toHaveBeenCalledTimes(2);
  });

  it("retains exact native ownership when the public status probe fails", async () => {
    const api = services({ getStatus: vi.fn().mockRejectedValue(new Error("Public status unavailable.")) });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );

    expect(await screen.findByText("error")).toBeInTheDocument();
    expect(screen.getByLabelText("ownership")).toHaveTextContent("owned");
    await user.click(screen.getByRole("button", { name: "Stop" }));
    await waitFor(() => expect(api.bootstrap.stop).toHaveBeenCalledTimes(1));
  });

  it("stops only an app-owned node", async () => {
    const owned = services();
    const user = userEvent.setup();
    const view = render(
      <NodeSessionProvider services={owned} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Stop" }));
    await waitFor(() => expect(owned.bootstrap.stop).toHaveBeenCalledTimes(1));
    expect(screen.getByLabelText("phase")).toHaveTextContent("stopped");
    expect(screen.getByLabelText("ownership")).toHaveTextContent("none");
    view.unmount();

    const attached = services({
      bootstrap: {
        ...services().bootstrap,
        start: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      },
    });
    const attachedView = render(
      <NodeSessionProvider services={attached} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Stop" }));
    expect(attached.bootstrap.stop).not.toHaveBeenCalled();
    expect(screen.getByLabelText("ownership")).toHaveTextContent("attached");
    attachedView.unmount();
    expect(attached.bootstrap.stop).not.toHaveBeenCalled();
  });

  it("publishes stopped when stop races a late lifecycle terminal without probing again", async () => {
    let callbacks!: ControlStreamCallbacks;
    let resolveStop!: (value: BootstrapSnapshot) => void;
    const dispose = vi.fn();
    const stop = vi.fn(
      () =>
        new Promise<BootstrapSnapshot>((resolve) => {
          resolveStop = resolve;
        }),
    );
    const api = services({
      bootstrap: { ...services().bootstrap, stop },
      createControlEventStream: vi.fn((_endpoint, _token, _cursor, nextCallbacks) => {
        callbacks = nextCallbacks;
        return { cancel: vi.fn(), dispose, finished: new Promise<never>(() => undefined) };
      }),
    });
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );

    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await waitFor(() => expect(callbacks).toBeDefined());

    act(() => {
      fireEvent.click(screen.getByRole("button", { name: "Stop" }));
      callbacks.onEvent({
        sequence: 4,
        operation: {
          id: "op-late",
          kind: "load",
          status: "succeeded",
          modelId: "gemma-ready",
          progress: null,
          error: null,
          createdAtUnixMs: 1,
          updatedAtUnixMs: 2,
        },
      });
    });

    expect(api.getStatus).toHaveBeenCalledTimes(1);
    act(() => resolveStop(snapshot({ ownership: "none", childRunning: false })));
    expect(await screen.findByText("stopped")).toBeInTheDocument();
    expect(api.getStatus).toHaveBeenCalledTimes(1);
    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it("settles only the tracked operation from a snapshot with historical terminals", async () => {
    let callbacks!: ControlStreamCallbacks;
    let resolveProof!: (status: typeof readyStatus) => void;
    const proofSignals: AbortSignal[] = [];
    const getStatus = vi
      .fn()
      .mockResolvedValueOnce(readyStatus)
      .mockImplementation((_endpoint, options) => {
        if (options?.signal) proofSignals.push(options.signal);
        return new Promise<typeof readyStatus>((resolve) => {
          resolveProof = resolve;
        });
      });
    const api = services({
      getStatus,
      createControlEventStream: vi.fn((_endpoint, _token, _cursor, nextCallbacks) => {
        callbacks = nextCallbacks;
        return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
      }),
    });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("ready")).toBeInTheDocument();
    await waitFor(() => expect(callbacks).toBeDefined());
    await user.click(screen.getByRole("button", { name: "Track current operation" }));

    act(() =>
      callbacks.onSnapshot({
        cursor: 9,
        cursorGap: false,
        operations: ["op-old-1", "op-1", "op-old-2"].map((id) => ({
          id,
          kind: "load" as const,
          status: "succeeded" as const,
          modelId: "gemma-ready",
          progress: null,
          error: null,
          createdAtUnixMs: 1,
          updatedAtUnixMs: 2,
        })),
        events: [],
      }),
    );

    expect(getStatus).toHaveBeenCalledTimes(2);
    expect(proofSignals).toHaveLength(1);
    expect(proofSignals[0]?.aborted).toBe(false);
    act(() => resolveProof(readyStatus));
    expect(await screen.findByText("gemma-3-4b-it-q4")).toBeInTheDocument();
    expect(proofSignals[0]?.aborted).toBe(false);
  });

  it("retries an operation proof after a transient rejection", async () => {
    let callbacks!: ControlStreamCallbacks;
    const getStatus = vi
      .fn()
      .mockResolvedValueOnce(readyStatus)
      .mockRejectedValueOnce(new Error("status temporarily unavailable"))
      .mockResolvedValueOnce(readyStatus);
    const api = services({
      getStatus,
      createControlEventStream: vi.fn((_endpoint, _token, _cursor, nextCallbacks) => {
        callbacks = nextCallbacks;
        return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
      }),
    });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("ready")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Track current operation" }));
    const terminal = {
      sequence: 2,
      operation: {
        id: "op-1",
        kind: "load" as const,
        status: "succeeded" as const,
        modelId: "gemma-ready",
        progress: null,
        error: null,
        createdAtUnixMs: 1,
        updatedAtUnixMs: 2,
      },
    };
    act(() => callbacks.onEvent(terminal));
    expect(await screen.findByText("error")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Settle current operation" }));
    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(getStatus).toHaveBeenCalledTimes(3);
  });

  it("keeps an operation retryable when its proof is superseded", async () => {
    let callbacks!: ControlStreamCallbacks;
    const proofSignals: AbortSignal[] = [];
    const getStatus = vi
      .fn()
      .mockResolvedValueOnce(readyStatus)
      .mockImplementationOnce(
        (_endpoint, options) =>
          new Promise<typeof readyStatus>((_resolve, reject) => {
            if (options?.signal) {
              proofSignals.push(options.signal);
              options.signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), {
                once: true,
              });
            }
          }),
      )
      .mockResolvedValueOnce(readyStatus)
      .mockResolvedValueOnce(readyStatus);
    const api = services({
      getStatus,
      createControlEventStream: vi.fn((_endpoint, _token, _cursor, nextCallbacks) => {
        callbacks = nextCallbacks;
        return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
      }),
    });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("ready")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Track current operation" }));
    const terminal = {
      sequence: 2,
      operation: {
        id: "op-1",
        kind: "load" as const,
        status: "succeeded" as const,
        modelId: "gemma-ready",
        progress: null,
        error: null,
        createdAtUnixMs: 1,
        updatedAtUnixMs: 2,
      },
    };
    act(() => callbacks.onEvent(terminal));
    await waitFor(() => expect(getStatus).toHaveBeenCalledTimes(2));
    await user.click(screen.getByRole("button", { name: "Refresh status" }));
    expect(proofSignals[0]?.aborted).toBe(true);
    await waitFor(() => expect(screen.getByLabelText("phase")).toHaveTextContent("ready"));
    act(() => callbacks.onEvent({ ...terminal, sequence: 3 }));
    await waitFor(() => expect(getStatus).toHaveBeenCalledTimes(4));
  });

  it("resets completed operation ids for a new node epoch", async () => {
    let callbacks!: ControlStreamCallbacks;
    const api = services({ getStatus: vi.fn().mockResolvedValue(readyStatus) });
    const user = userEvent.setup();
    vi.mocked(api.createControlEventStream).mockImplementation((_endpoint, _token, _cursor, nextCallbacks) => {
      callbacks = nextCallbacks;
      return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
    });
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("ready")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Track current operation" }));
    const terminal = {
      sequence: 2,
      operation: {
        id: "op-1",
        kind: "load" as const,
        status: "succeeded" as const,
        modelId: "gemma-ready",
        progress: null,
        error: null,
        createdAtUnixMs: 1,
        updatedAtUnixMs: 2,
      },
    };
    act(() => callbacks.onEvent(terminal));
    await waitFor(() => expect(api.getStatus).toHaveBeenCalledTimes(2));
    await user.click(screen.getByRole("button", { name: "Stop" }));
    expect(await screen.findByText("stopped")).toBeInTheDocument();
    const previousCallbacks = callbacks;
    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(await screen.findByText("ready")).toBeInTheDocument();
    await waitFor(() => expect(callbacks).not.toBe(previousCallbacks));
    await user.click(screen.getByRole("button", { name: "Track current operation" }));
    act(() => callbacks.onEvent({ ...terminal, sequence: 1 }));
    await waitFor(() => expect(api.getStatus).toHaveBeenCalledTimes(4));
  });

  it("bounds retryable terminal tracking without pruning an active operation", async () => {
    const getStatus = vi.fn().mockResolvedValueOnce(readyStatus).mockRejectedValue(new Error("proof unavailable"));
    const api = services({ getStatus });
    capturedSession = null;
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <CapturingProbe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(capturedSession).not.toBeNull();

    act(() => capturedSession?.invalidateModelTruth("op-active"));
    for (let index = 0; index < 129; index += 1) {
      const operationId = `op-failed-${index}`;
      act(() => capturedSession?.invalidateModelTruth(operationId));
      await act(async () => {
        await capturedSession?.settleModelMutation(operationId);
      });
    }
    expect(getStatus).toHaveBeenCalledTimes(130);

    await act(async () => {
      await capturedSession?.settleModelMutation("op-failed-0");
    });
    expect(getStatus).toHaveBeenCalledTimes(130);
    await act(async () => {
      await capturedSession?.settleModelMutation("op-active");
    });
    expect(getStatus).toHaveBeenCalledTimes(131);
  });

  it("cancels an initial stream reconnect when stop closes the epoch", async () => {
    const readControlToken = vi.fn().mockRejectedValue(new Error("token unavailable"));
    const api = services({ readControlToken });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await waitFor(() => expect(readControlToken).toHaveBeenCalledTimes(1));

    await user.click(screen.getByRole("button", { name: "Stop" }));
    expect(await screen.findByText("stopped")).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(readControlToken).toHaveBeenCalledTimes(1);
  });

  it("cancels an initial stream reconnect on unmount", async () => {
    const readControlToken = vi.fn().mockRejectedValue(new Error("token unavailable"));
    const api = services({ readControlToken });
    const view = render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await waitFor(() => expect(readControlToken).toHaveBeenCalledTimes(1));

    view.unmount();
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(readControlToken).toHaveBeenCalledTimes(1);
  });

  it("cancels the old stream reconnect when retry starts a new epoch", async () => {
    const readControlToken = vi
      .fn()
      .mockRejectedValueOnce(new Error("token unavailable"))
      .mockResolvedValue("ab".repeat(32));
    const api = services({ readControlToken });
    const user = userEvent.setup();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await waitFor(() => expect(readControlToken).toHaveBeenCalledTimes(1));

    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await waitFor(() => expect(readControlToken).toHaveBeenCalledTimes(2));
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(readControlToken).toHaveBeenCalledTimes(2);
  });

  it("caps pre-snapshot flapping retries and resets the budget only after a valid snapshot", async () => {
    const callbackHistory: ControlStreamCallbacks[] = [];
    const api = services({
      createControlEventStream: vi.fn((_endpoint, _token, _cursor, callbacks) => {
        callbackHistory.push(callbacks);
        return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
      }),
    });
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await waitFor(() => expect(callbackHistory).toHaveLength(1));
    vi.useFakeTimers();
    try {
      for (const [retryIndex, delay] of [100, 200, 400, 800, 1_600, 1_600].entries()) {
        act(() =>
          callbackHistory[callbackHistory.length - 1]?.onTerminal({
            kind: "error",
            cursor: retryIndex,
            message: "stream ended before snapshot",
          }),
        );
        await act(async () => {
          await vi.advanceTimersByTimeAsync(delay - 1);
        });
        expect(callbackHistory).toHaveLength(retryIndex + 1);
        await act(async () => {
          await vi.advanceTimersByTimeAsync(1);
        });
        expect(callbackHistory).toHaveLength(retryIndex + 2);
      }

      act(() =>
        callbackHistory[callbackHistory.length - 1]?.onTerminal({
          kind: "error",
          cursor: 7,
          message: "stream still flapping",
        }),
      );
      await act(async () => {
        await vi.advanceTimersByTimeAsync(10_000);
      });
      expect(callbackHistory).toHaveLength(7);

      act(() =>
        callbackHistory[callbackHistory.length - 1]?.onSnapshot({
          cursor: 7,
          cursorGap: false,
          operations: [],
          events: [],
        }),
      );
      act(() =>
        callbackHistory[callbackHistory.length - 1]?.onTerminal({
          kind: "error",
          cursor: 7,
          message: "later independent disconnect",
        }),
      );
      await act(async () => {
        await vi.advanceTimersByTimeAsync(99);
      });
      expect(callbackHistory).toHaveLength(7);
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1);
      });
      expect(callbackHistory).toHaveLength(8);
    } finally {
      vi.useRealTimers();
    }
  });

  it("aborts its authoritative probe on unmount", async () => {
    let signal: AbortSignal | undefined;
    const api = services({
      getStatus: vi.fn((_endpoint, options) => {
        signal = options?.signal;
        return new Promise<never>(() => undefined);
      }),
    });
    const view = render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    await waitFor(() => expect(api.getStatus).toHaveBeenCalledTimes(1));

    act(() => view.unmount());
    expect(signal?.aborted).toBe(true);
  });
});
