import { useState } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { decodeV2OperationAccepted } from "../control/contracts";
import { validV2Operation, validV2OperationAccepted, v2Ids } from "../control/testSupport";
import {
  SessionHarness,
  controlSnapshot,
  modelFixture,
  scriptedV2Control,
  servicesWithControl,
  testPeer,
} from "../node/testSupport";
import { ModelsScreen } from "./ModelsScreen";

function renderModels(
  options: {
    snapshot?: ReturnType<typeof controlSnapshot>;
    inventory?: ReturnType<typeof modelFixture>[];
    onStart?: (operationId: string) => void;
    onSettled?: (operationId: string) => void;
    confirmGlobalDownloadCancel?: () => boolean;
  } = {},
) {
  const control = scriptedV2Control(options.snapshot);
  const services = servicesWithControl(control, {
    getInventory: vi.fn().mockResolvedValue(options.inventory ?? [modelFixture()]),
    confirmGlobalDownloadCancel: options.confirmGlobalDownloadCancel ?? vi.fn().mockReturnValue(false),
  });
  render(
    <SessionHarness services={services}>
      <ModelsScreen
        endpoint="http://127.0.0.1:8080"
        services={services}
        onModelMutationStart={options.onStart}
        onModelMutationSettled={options.onSettled}
      />
    </SessionHarness>,
  );
  return { control, services };
}

describe("ModelsScreen v2 authority", () => {
  it("preserves the searchable installed-model workspace while inventory remains metadata-only v1", async () => {
    const user = userEvent.setup();
    const alpha = modelFixture("alpha-model");
    const beta = modelFixture("beta-model");
    const { services } = renderModels({ inventory: [alpha, beta] });

    expect(await screen.findByRole("region", { name: "Installed models" })).toBeInTheDocument();
    expect(screen.getByRole("complementary", { name: "Model details" })).toHaveTextContent("alpha-model");
    await user.type(screen.getByRole("searchbox", { name: "Search models" }), "beta");
    expect(screen.getByRole("complementary", { name: "Model details" })).toHaveTextContent("beta-model");
    expect(services.getInventory).toHaveBeenCalledWith(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(services.proveV2ControlPeer).toHaveBeenCalledTimes(1);
    expect(services.openV2Events).toHaveBeenCalledTimes(1);
  });

  it("downloads through v2 and never calls the v1 mutation adapter", async () => {
    const user = userEvent.setup();
    const entry = { ...modelFixture(), artifact: { kind: "not_downloaded" as const } };
    const { services } = renderModels({ inventory: [entry] });

    await user.click(await screen.findByRole("button", { name: `Download ${entry.id}` }));
    await waitFor(() => expect(services.downloadV2Model).toHaveBeenCalledWith(testPeer, entry.id));
  });

  it("keeps unrelated download and lifecycle actions available during an active download", async () => {
    const activeDownload = {
      ...validV2Operation,
      kind: "download" as const,
      slot_id: null,
      model_id: "active-download",
      status: "running" as const,
    };
    const loadable = modelFixture("loadable-model");
    const downloadable = {
      ...modelFixture("other-download"),
      artifact: { kind: "not_downloaded" as const },
    };
    renderModels({
      snapshot: controlSnapshot({ operations: [activeDownload] }),
      inventory: [loadable, downloadable],
    });

    expect(await screen.findByRole("button", { name: "Load loadable-model" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Download other-download" })).toBeEnabled();
  });

  it("tracks the exact accepted UUID for a v2 default-slot load", async () => {
    const user = userEvent.setup();
    const onStart = vi.fn();
    const accepted = decodeV2OperationAccepted({
      epoch: v2Ids.epoch,
      operation_id: v2Ids.nextEvent,
      revision: "12",
    });
    const { services } = renderModels({ onStart });
    vi.mocked(services.loadV2Slot!).mockResolvedValue(accepted);

    await user.click(await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" }));
    await waitFor(() => expect(onStart).toHaveBeenCalledWith(v2Ids.nextEvent));
    expect(services.loadV2Slot).toHaveBeenCalledWith(testPeer, v2Ids.node, v2Ids.slot, "gemma-3-4b-it-q4");
  });

  it("keeps an accepted mutation pending and settles only its exact correlated terminal UUID", async () => {
    const user = userEvent.setup();
    const onStart = vi.fn();
    const onSettled = vi.fn();
    const accepted = decodeV2OperationAccepted({
      epoch: v2Ids.epoch,
      operation_id: v2Ids.nextEvent,
      revision: "12",
    });
    const { control, services } = renderModels({ onStart, onSettled });
    vi.mocked(services.loadV2Slot!).mockResolvedValue(accepted);

    const load = await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" });
    await user.click(load);
    await waitFor(() => expect(onStart).toHaveBeenCalledWith(v2Ids.nextEvent));
    expect(load).toBeDisabled();

    act(() =>
      control.emitReplacement(
        controlSnapshot({
          revision: "11",
          cursor: "11",
          operations: [{ ...validV2Operation, status: "succeeded" }],
        }),
      ),
    );
    expect(onSettled).not.toHaveBeenCalled();
    expect(load).toBeDisabled();

    act(() =>
      control.emitReplacement(
        controlSnapshot({
          revision: "14",
          cursor: "14",
          operations: [{ ...validV2Operation, operation_id: v2Ids.nextEvent, status: "succeeded" }],
        }),
      ),
    );
    await waitFor(() => expect(onSettled).toHaveBeenCalledWith(v2Ids.nextEvent));
    expect(load).toBeEnabled();
  });

  it("cancels the authoritative v2 operation UUID without a v1 lookup", async () => {
    const user = userEvent.setup();
    const operation = {
      ...validV2Operation,
      kind: "download" as const,
      slot_id: null,
      model_id: "gemma-3-4b-it-q4",
      progress: { completed_bytes: "512", total_bytes: "1024" },
    };
    const confirmGlobalDownloadCancel = vi.fn().mockReturnValue(true);
    const { services } = renderModels({
      snapshot: controlSnapshot({ operations: [operation] }),
      confirmGlobalDownloadCancel,
    });

    const cancel = await screen.findByRole("button", { name: "Cancel download gemma-3-4b-it-q4" });
    expect(screen.getByRole("progressbar")).toHaveAttribute("value", "512");
    expect(screen.getByRole("progressbar")).toHaveAttribute("max", "1024");
    await user.click(cancel);
    expect(confirmGlobalDownloadCancel).toHaveBeenCalledOnce();
    await waitFor(() => expect(services.cancelV2Operation).toHaveBeenCalledWith(testPeer, v2Ids.operation));
  });

  it("keeps a shared download running when global cancellation is declined", async () => {
    const user = userEvent.setup();
    const confirmGlobalDownloadCancel = vi.fn().mockReturnValue(false);
    const operation = {
      ...validV2Operation,
      kind: "download" as const,
      slot_id: null,
      model_id: "gemma-3-4b-it-q4",
    };
    const { services } = renderModels({
      snapshot: controlSnapshot({ operations: [operation] }),
      confirmGlobalDownloadCancel,
    });

    await user.click(await screen.findByRole("button", { name: "Cancel download gemma-3-4b-it-q4" }));

    expect(confirmGlobalDownloadCancel).toHaveBeenCalledOnce();
    expect(services.cancelV2Operation).not.toHaveBeenCalled();
  });

  it("fails closed on active-download cancellation during slot recovery", async () => {
    const operation = {
      ...validV2Operation,
      kind: "download" as const,
      slot_id: null,
      model_id: "gemma-3-4b-it-q4",
    };
    renderModels({
      snapshot: controlSnapshot({
        slot: {
          status: "recovery",
          model_id: null,
          operation_id: null,
          error: { code: "lifecycle_recovery_required", message: "Reconcile lifecycle state." },
        },
        operations: [operation],
      }),
    });

    expect(await screen.findByRole("button", { name: "Cancel download gemma-3-4b-it-q4" })).toBeDisabled();
  });

  it("keeps an accepted cancellation pending until that exact operation is terminal", async () => {
    const user = userEvent.setup();
    const onSettled = vi.fn();
    const operation = {
      ...validV2Operation,
      kind: "download" as const,
      slot_id: null,
      model_id: "gemma-3-4b-it-q4",
    };
    const { control, services } = renderModels({
      snapshot: controlSnapshot({ operations: [operation] }),
      onSettled,
      confirmGlobalDownloadCancel: vi.fn().mockReturnValue(true),
    });

    const cancel = await screen.findByRole("button", { name: "Cancel download gemma-3-4b-it-q4" });
    await user.click(cancel);
    await waitFor(() => expect(services.cancelV2Operation).toHaveBeenCalledWith(testPeer, v2Ids.operation));
    expect(cancel).toBeDisabled();

    act(() =>
      control.emitReplacement(
        controlSnapshot({
          revision: "12",
          cursor: "12",
          operations: [{ ...operation, status: "cancelled", updated_revision: "12" }],
        }),
      ),
    );
    await waitFor(() => expect(onSettled).toHaveBeenCalledWith(v2Ids.operation));
    expect(screen.queryByRole("button", { name: "Cancel download gemma-3-4b-it-q4" })).not.toBeInTheDocument();
  });

  it("keeps model controls gated across a route unmount until the exact accepted UUID is terminal", async () => {
    const user = userEvent.setup();
    const control = scriptedV2Control();
    const services = servicesWithControl(control, {
      getInventory: vi.fn().mockResolvedValue([modelFixture()]),
    });
    function Routes() {
      const [showModels, setShowModels] = useState(true);
      return (
        <>
          <button type="button" onClick={() => setShowModels((current) => !current)}>
            {showModels ? "Leave models" : "Return to models"}
          </button>
          {showModels ? <ModelsScreen endpoint="http://127.0.0.1:8080" services={services} /> : <p>Node route</p>}
        </>
      );
    }
    render(
      <SessionHarness services={services}>
        <Routes />
      </SessionHarness>,
    );

    await user.click(await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" }));
    await waitFor(() => expect(services.loadV2Slot).toHaveBeenCalledOnce());
    await user.click(screen.getByRole("button", { name: "Leave models" }));
    await user.click(screen.getByRole("button", { name: "Return to models" }));
    const gatedLoad = await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" });
    expect(gatedLoad).toBeDisabled();

    act(() =>
      control.emitReplacement(
        controlSnapshot({
          revision: "12",
          cursor: "12",
          operations: [{ ...validV2Operation, status: "succeeded", updated_revision: "12" }],
        }),
      ),
    );
    await waitFor(() => expect(gatedLoad).toBeEnabled());
  });

  it("replaces model truth from a new epoch snapshot instead of replaying old state", async () => {
    const { control } = renderModels();
    expect(await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" })).toBeInTheDocument();
    act(() =>
      control.emitReplacement(
        controlSnapshot({
          epoch: v2Ids.oldEpoch,
          cursor: "20",
          revision: "20",
          cursorGap: true,
          slot: { status: "ready", model_id: "gemma-3-4b-it-q4", operation_id: null },
        }),
      ),
    );
    expect(await screen.findByRole("button", { name: "Unload gemma-3-4b-it-q4" })).toBeInTheDocument();
  });

  it("releases UI-local mutation tracking when a gap snapshot no longer retains the accepted operation", async () => {
    const user = userEvent.setup();
    const onSettled = vi.fn();
    const accepted = decodeV2OperationAccepted({
      epoch: v2Ids.epoch,
      operation_id: v2Ids.nextEvent,
      revision: "12",
    });
    const { control, services } = renderModels({ onSettled });
    vi.mocked(services.loadV2Slot!).mockResolvedValue(accepted);
    const load = await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" });
    await user.click(load);
    await waitFor(() => expect(load).toBeDisabled());

    act(() =>
      control.emitReplacement(controlSnapshot({ revision: "13", cursor: "13", cursorGap: true, operations: [] })),
    );

    await waitFor(() => expect(onSettled).toHaveBeenCalledWith(v2Ids.nextEvent));
    expect(load).toBeEnabled();
  });

  it("does not publish mutation-start for an admission returned by an obsolete epoch", async () => {
    const user = userEvent.setup();
    const onStart = vi.fn();
    const control = scriptedV2Control();
    const services = servicesWithControl(control, { getInventory: vi.fn().mockResolvedValue([modelFixture()]) });
    const accepted = decodeV2OperationAccepted(validV2OperationAccepted);
    let resolveLoad!: (value: typeof accepted) => void;
    services.loadV2Slot = vi.fn(
      () =>
        new Promise<typeof accepted>((resolve) => {
          resolveLoad = resolve;
        }),
    );
    render(
      <SessionHarness services={services}>
        <ModelsScreen endpoint="http://127.0.0.1:8080" services={services} onModelMutationStart={onStart} />
      </SessionHarness>,
    );
    await user.click(await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" }));
    act(() =>
      control.emitReplacement(
        controlSnapshot({ epoch: v2Ids.oldEpoch, revision: "20", cursor: "20", cursorGap: true }),
      ),
    );

    await act(async () => resolveLoad(accepted));

    expect(onStart).not.toHaveBeenCalled();
    expect(await screen.findByText(/accepted operation no longer belongs/)).toBeInTheDocument();
  });

  it("fails closed when the one shared v2 proof fails", async () => {
    const control = scriptedV2Control();
    const services = servicesWithControl(control, {
      proveV2ControlPeer: vi.fn().mockRejectedValue(new Error("credential unavailable")),
    });
    render(
      <SessionHarness services={services}>
        <ModelsScreen endpoint="http://127.0.0.1:8080" services={services} />
      </SessionHarness>,
    );

    expect(await screen.findByText("Controls unavailable")).toBeInTheDocument();
    expect(services.getInventory).not.toHaveBeenCalled();
    expect(control.openV2Events).not.toHaveBeenCalled();
  });

  it("honors the finite verification polling budget across inventory replacements", async () => {
    vi.useFakeTimers();
    try {
      const verificationRequired = {
        ...modelFixture(),
        artifact: { kind: "invalid" as const, reason: "verification_required" },
      };
      const control = scriptedV2Control();
      const services = servicesWithControl(control, {
        getInventory: vi.fn().mockImplementation(async () => [{ ...verificationRequired }]),
      });
      render(
        <SessionHarness services={services}>
          <ModelsScreen
            endpoint="http://127.0.0.1:8080"
            services={services}
            verificationPollMs={1}
            verificationPollLimit={2}
          />
        </SessionHarness>,
      );

      await act(async () => {
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(services.getInventory).toHaveBeenCalledTimes(1);
      for (let index = 0; index < 10; index += 1) {
        await act(async () => {
          await vi.advanceTimersByTimeAsync(10);
        });
      }
      expect(services.getInventory).toHaveBeenCalledTimes(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("blocks lifecycle mutations while the default slot requires recovery", async () => {
    const { services } = renderModels({
      snapshot: controlSnapshot({
        slot: {
          status: "recovery",
          model_id: "gemma-3-4b-it-q4",
          operation_id: null,
          error: { code: "lifecycle_recovery_required", message: "Reconcile the previous lifecycle operation." },
        },
      }),
    });

    expect(
      await screen.findByText(
        "Recovery required. Model and chat controls are blocked until the node is safely restarted.",
      ),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Load|Unload|Switch to/ })).not.toBeInTheDocument();
    expect(services.loadV2Slot).not.toHaveBeenCalled();
  });

  it("preserves keyboard navigation between installed and discovery workspaces", async () => {
    const user = userEvent.setup();
    renderModels();
    const installed = await screen.findByRole("tab", { name: "Installed" });
    const discover = screen.getByRole("tab", { name: "Discover" });
    installed.focus();
    await user.keyboard("{ArrowRight}");
    expect(discover).toHaveFocus();
    expect(discover).toHaveAttribute("aria-selected", "true");
  });

  it("aborts the metadata inventory request when the model workspace unmounts", async () => {
    let inventorySignal: AbortSignal | undefined;
    const control = scriptedV2Control();
    const services = servicesWithControl(control, {
      getInventory: vi.fn((_endpoint, _token, options) => {
        inventorySignal = options?.signal;
        return new Promise<never>(() => undefined);
      }),
    });
    const mounted = render(
      <SessionHarness services={services}>
        <ModelsScreen endpoint="http://127.0.0.1:8080" services={services} />
      </SessionHarness>,
    );
    await waitFor(() => expect(inventorySignal).toBeDefined());
    mounted.unmount();
    expect(inventorySignal?.aborted).toBe(true);
  });
});
