import { StrictMode } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { decodeV2ControlEvent, decodeV2OperationAccepted } from "../control/contracts";
import { validV2Event, validV2Operation, validV2OperationAccepted, v2Ids } from "../control/testSupport";
import { NodeSessionProvider, useNodeSession } from "./NodeSession";
import { controlSnapshot, scriptedV2Control, servicesWithControl, testEndpoint, testPeer } from "./testSupport";

function Probe({ afterStop, afterRetry }: { afterStop?: () => void; afterRetry?: () => void } = {}) {
  const session = useNodeSession();
  return (
    <div>
      <output aria-label="phase">{session.phase}</output>
      <output aria-label="model">{session.status?.runtime_model ?? "No Models Loaded"}</output>
      <output aria-label="error">{session.error ?? ""}</output>
      <button type="button" onClick={() => void session.downloadModel("gemma-3-4b-it-q4").catch(() => undefined)}>
        Download
      </button>
      <button type="button" onClick={() => void session.loadModel("gemma-3-4b-it-q4").catch(() => undefined)}>
        Load
      </button>
      <button type="button" onClick={() => void session.unloadModel().catch(() => undefined)}>
        Unload
      </button>
      <button type="button" onClick={() => void session.cancelOperation(v2Ids.operation).catch(() => undefined)}>
        Cancel
      </button>
      <button type="button" onClick={() => void session.stop()}>
        Stop
      </button>
      <button
        type="button"
        onClick={() => {
          void session.stop();
          afterStop?.();
        }}
      >
        Stop with stale callback
      </button>
      <button
        type="button"
        onClick={() => {
          void session.retry();
          afterRetry?.();
        }}
      >
        Retry with stale callback
      </button>
    </div>
  );
}

describe("NodeSessionProvider v2 authority", () => {
  it("deduplicates bootstrap in StrictMode and establishes one proved v2 peer", async () => {
    const control = scriptedV2Control();
    const services = servicesWithControl(control);
    render(
      <StrictMode>
        <NodeSessionProvider services={services} endpoint={testEndpoint}>
          <Probe />
        </NodeSessionProvider>
      </StrictMode>,
    );

    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    expect(services.bootstrap.start).toHaveBeenCalledTimes(1);
    expect(services.readControlToken).toHaveBeenCalledTimes(1);
    expect(services.proveV2ControlPeer).toHaveBeenCalledTimes(1);
    expect(services.proveV2ControlPeer).toHaveBeenCalledWith(testEndpoint, "ab".repeat(32), {
      signal: expect.any(AbortSignal),
    });
    expect(control.openV2Events).toHaveBeenCalledWith(testPeer, undefined, expect.any(Object), expect.any(AbortSignal));
    expect(services.getInventory).not.toHaveBeenCalled();
  });

  it("fully replaces node/default-slot truth on epoch change and ignores an old-epoch event", async () => {
    const control = scriptedV2Control();
    const services = servicesWithControl(control);
    render(
      <NodeSessionProvider services={services} endpoint={testEndpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    expect(screen.getByText("No Models Loaded")).toBeInTheDocument();

    const replacement = controlSnapshot({
      epoch: v2Ids.oldEpoch,
      cursor: "20",
      revision: "20",
      cursorGap: true,
      slot: { status: "ready", model_id: "gemma-3-4b-it-q4", operation_id: null },
    });
    act(() => control.emitReplacement(replacement));
    expect(await screen.findByText("gemma-3-4b-it-q4")).toBeInTheDocument();

    act(() => control.emitEvent(decodeV2ControlEvent(validV2Event)));
    expect(screen.getByLabelText("model")).toHaveTextContent("gemma-3-4b-it-q4");
  });

  it("routes all model mutations through the same peer and authoritative default slot IDs", async () => {
    const user = userEvent.setup();
    const services = servicesWithControl();
    render(
      <NodeSessionProvider services={services} endpoint={testEndpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Download" }));
    await user.click(screen.getByRole("button", { name: "Load" }));
    await user.click(screen.getByRole("button", { name: "Unload" }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    expect(services.downloadV2Model).toHaveBeenCalledWith(testPeer, "gemma-3-4b-it-q4");
    expect(services.loadV2Slot).toHaveBeenCalledWith(testPeer, v2Ids.node, v2Ids.slot, "gemma-3-4b-it-q4");
    expect(services.unloadV2Slot).toHaveBeenCalledWith(testPeer, v2Ids.node, v2Ids.slot);
    expect(services.cancelV2Operation).toHaveBeenCalledWith(testPeer, v2Ids.operation);
  });

  it("fails closed when v2 proof fails and never opens a durable stream", async () => {
    const control = scriptedV2Control();
    const services = servicesWithControl(control, {
      proveV2ControlPeer: vi.fn().mockRejectedValue(new Error("identity replaced")),
    });
    render(
      <NodeSessionProvider services={services} endpoint={testEndpoint}>
        <Probe />
      </NodeSessionProvider>,
    );

    expect(await screen.findByText("identity replaced")).toBeInTheDocument();
    expect(screen.getByLabelText("phase")).toHaveTextContent("error");
    expect(control.openV2Events).not.toHaveBeenCalled();
  });

  it("clears the authority before stopping an owned node", async () => {
    const user = userEvent.setup();
    const services = servicesWithControl();
    render(
      <NodeSessionProvider services={services} endpoint={testEndpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Stop" }));
    await waitFor(() => expect(screen.getByLabelText("phase")).toHaveTextContent("stopped"));
    expect(services.bootstrap.stop).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "Download" }));
    expect(services.downloadV2Model).not.toHaveBeenCalled();
  });

  it("fails closed immediately when the durable stream disconnects and restores only from a replacement snapshot", async () => {
    const user = userEvent.setup();
    const control = scriptedV2Control();
    const services = servicesWithControl(control);
    render(
      <NodeSessionProvider services={services} endpoint={testEndpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();

    act(() => control.terminate({ kind: "error", cursor: "11", message: "stream lost" }));
    expect(screen.getByLabelText("phase")).toHaveTextContent("reconciling");
    await user.click(screen.getByRole("button", { name: "Download" }));
    expect(services.downloadV2Model).not.toHaveBeenCalled();

    act(() => control.emitReplacement(controlSnapshot({ revision: "12", cursor: "12" })));
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Download" }));
    expect(services.downloadV2Model).toHaveBeenCalledOnce();
  });

  it("rejects queued callbacks from an old peer after stop or retry tears down authority", async () => {
    const user = userEvent.setup();
    const stopServices = servicesWithControl();
    stopServices.bootstrap.stop = vi.fn(() => new Promise<never>(() => undefined));
    const stopCalls = vi.mocked(stopServices.openV2Events!).mock.calls;
    const stopped = render(
      <NodeSessionProvider services={stopServices} endpoint={testEndpoint}>
        <Probe
          afterStop={() =>
            stopCalls[0]?.[2].onSnapshot(
              controlSnapshot({ slot: { status: "ready", model_id: "stale-stop", operation_id: null } }),
            )
          }
        />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Stop with stale callback" }));
    expect(screen.queryByText("stale-stop")).not.toBeInTheDocument();
    expect(screen.getByLabelText("phase")).toHaveTextContent("stopping");
    stopped.unmount();

    const retryServices = servicesWithControl();
    const retryCalls = vi.mocked(retryServices.openV2Events!).mock.calls;
    render(
      <NodeSessionProvider services={retryServices} endpoint={testEndpoint}>
        <Probe
          afterRetry={() =>
            retryCalls[0]?.[2].onSnapshot(
              controlSnapshot({ slot: { status: "ready", model_id: "stale-retry", operation_id: null } }),
            )
          }
        />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    retryServices.bootstrap.start = vi.fn(() => new Promise<never>(() => undefined));
    await user.click(screen.getByRole("button", { name: "Retry with stale callback" }));
    expect(screen.queryByText("stale-retry")).not.toBeInTheDocument();
    expect(screen.getByLabelText("phase")).toHaveTextContent("starting");
  });

  it("exhausts the reconnect budget under immediate snapshot-terminal flapping", async () => {
    vi.useFakeTimers();
    try {
      const services = servicesWithControl();
      services.openV2Events = vi.fn((_peer, _resume, callbacks) => {
        queueMicrotask(() => {
          callbacks.onSnapshot(controlSnapshot());
          callbacks.onTerminal({ kind: "error", cursor: "11", message: "flapping" });
        });
        return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<never>(() => undefined) };
      });
      render(
        <NodeSessionProvider services={services} endpoint={testEndpoint}>
          <Probe />
        </NodeSessionProvider>,
      );
      await act(async () => {
        await Promise.resolve();
        await Promise.resolve();
      });
      for (let index = 0; index < 8; index += 1) {
        await act(async () => {
          await vi.advanceTimersByTimeAsync(3_000);
        });
      }

      expect(screen.getByLabelText("phase")).toHaveTextContent("disconnected");
      expect(services.openV2Events).toHaveBeenCalledTimes(7);
    } finally {
      vi.useRealTimers();
    }
  });

  it("reconciles an accepted UUID immediately when its terminal snapshot arrived before the response", async () => {
    const user = userEvent.setup();
    const control = scriptedV2Control();
    const services = servicesWithControl(control);
    const accepted = decodeV2OperationAccepted({
      ...validV2OperationAccepted,
      operation_id: v2Ids.nextEvent,
      revision: "12",
    });
    let resolveLoad!: (value: typeof accepted) => void;
    services.loadV2Slot = vi.fn(
      () =>
        new Promise<typeof accepted>((resolve) => {
          resolveLoad = resolve;
        }),
    );
    render(
      <NodeSessionProvider services={services} endpoint={testEndpoint}>
        <Probe />
      </NodeSessionProvider>,
    );
    expect(await screen.findByText("unloaded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Load" }));
    expect(services.loadV2Slot).toHaveBeenCalledOnce();

    act(() =>
      control.emitReplacement(
        controlSnapshot({
          revision: "12",
          cursor: "12",
          operations: [
            { ...validV2Operation, operation_id: v2Ids.nextEvent, status: "succeeded", updated_revision: "12" },
          ],
        }),
      ),
    );
    await act(async () => resolveLoad(accepted));

    expect(screen.getByLabelText("phase")).toHaveTextContent("unloaded");
  });
});
