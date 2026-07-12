import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { ControlStreamCallbacks, ControlStreamHandle } from "../control/events";
import type { ArtifactState, ModelInventoryEntry, OperationView } from "../control/contracts";
import { ModelsScreen, type ModelsScreenServices } from "./ModelsScreen";

const token = "ab".repeat(32);

function model(id: string, artifact: ArtifactState, compatible = true): ModelInventoryEntry {
  return {
    id,
    repo: `loxa/${id}`,
    revision: "0123456789abcdef",
    filename: `${id}.gguf`,
    sha256: "ab".repeat(32),
    sizeBytes: 1024,
    license: "Apache-2.0",
    params: "4B",
    quant: "Q4_K_M",
    minFreeMemoryGiB: 6,
    artifact,
    compatibility: {
      compatible,
      reason: compatible ? "Available memory meets the verified recipe minimum." : "Requires 12 GiB free memory.",
    },
    engine: { engine: "llama-cpp", eligible: true, reason: "Verified for llama.cpp." },
  };
}

function operation(status: OperationView["status"] = "running"): OperationView {
  return {
    id: "op-1",
    kind: "download",
    status,
    modelId: "model-ready",
    progress: status === "running" ? { completedBytes: 512, totalBytes: 1024 } : null,
    error: status === "failed" ? "Download failed safely." : null,
    createdAtUnixMs: 1,
    updatedAtUnixMs: 2,
  };
}

function setup() {
  let callbacks: ControlStreamCallbacks | undefined;
  const handle: ControlStreamHandle = {
    cancel: vi.fn(),
    dispose: vi.fn(),
    finished: new Promise(() => undefined),
  };
  const api: ModelsScreenServices = {
    readControlToken: vi.fn().mockResolvedValue(token),
    getControlNode: vi.fn().mockResolvedValue({ status: "unloaded", activeModelId: null, operationId: null, error: null }),
    getInventory: vi.fn().mockResolvedValue([
      model("model-ready", { kind: "not_downloaded" }),
      model("model-partial", { kind: "partial", bytes: 256 }),
      model("model-downloaded", { kind: "downloaded" }),
      model("model-verifying", { kind: "invalid", reason: "verification_required" }),
      model("model-incompatible", { kind: "not_downloaded" }, false),
    ]),
    downloadModel: vi.fn().mockResolvedValue({ operationId: "op-1" }),
    loadModel: vi.fn().mockResolvedValue({ operationId: "op-load" }),
    unloadModel: vi.fn().mockResolvedValue({ operationId: "op-unload" }),
    getOperation: vi.fn().mockResolvedValue(operation("queued")),
    cancelOperation: vi.fn().mockResolvedValue(operation("cancelled")),
    createControlEventStream: vi.fn((_endpoint, _token, _cursor, next) => {
      callbacks = next;
      return handle;
    }),
  };
  return { api, handle, callbacks: () => callbacks };
}

describe("ModelsScreen", () => {
  it("renders only legal node-authoritative lifecycle actions", async () => {
    const { api } = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={api} />);

    expect(await screen.findByRole("heading", { name: "Models" })).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "model-ready" })).toBeInTheDocument();
    expect(screen.getAllByText("Not downloaded")).toHaveLength(2);
    expect(screen.getByText(/Partial.*256 B.*1 KB/)).toBeInTheDocument();
    expect(screen.getByText("Downloaded and verified")).toBeInTheDocument();
    expect(screen.getByText("Verification required")).toBeInTheDocument();
    expect(screen.getByText(/Requires 12 GiB free memory\./)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Download model-ready" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Resume model-partial" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Download model-incompatible" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Load model-downloaded" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: /^load model-ready/i })).not.toBeInTheDocument();
  });

  it("starts load, uses Switch wording when another model is active, and refreshes authoritative node truth", async () => {
    const user = userEvent.setup();
    const setupState = setup();
    vi.mocked(setupState.api.getControlNode)
      .mockResolvedValueOnce({ status: "ready", activeModelId: "model-old", operationId: null, error: null })
      .mockResolvedValueOnce({ status: "loading", activeModelId: "model-old", operationId: "op-load", error: null });
    vi.mocked(setupState.api.getOperation).mockResolvedValueOnce({ ...operation("queued"), id: "op-load", kind: "load", modelId: "model-downloaded" });
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await user.click(await screen.findByRole("button", { name: "Switch to model-downloaded" }));
    expect(setupState.api.loadModel).toHaveBeenCalledWith("http://127.0.0.1:8080", token, "model-downloaded", expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(screen.getByLabelText("Model control summary")).toHaveTextContent("Node Loading");
    expect(screen.queryByRole("button", { name: "Cancel load model-downloaded" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Download model-ready" })).toBeDisabled();
  });

  it("offers unload only for the active model and leaves it active after a failed operation", async () => {
    const user = userEvent.setup();
    const setupState = setup();
    vi.mocked(setupState.api.getControlNode).mockResolvedValue({ status: "ready", activeModelId: "model-downloaded", operationId: null, error: null });
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await user.click(await screen.findByRole("button", { name: "Unload model-downloaded" }));
    expect(setupState.api.unloadModel).toHaveBeenCalledWith("http://127.0.0.1:8080", token, expect.objectContaining({ signal: expect.any(AbortSignal) }));
    act(() => setupState.callbacks()?.onEvent({ sequence: 8, operation: { ...operation("failed"), id: "op-unload", kind: "unload", modelId: null, error: "teardown failed" } }));
    expect(await screen.findByText("Active")).toBeInTheDocument();
  });

  it("blocks lifecycle controls during recovery and ignores stale operation events", async () => {
    const setupState = setup();
    vi.mocked(setupState.api.getControlNode).mockResolvedValue({ status: "recovery_required", activeModelId: "model-downloaded", operationId: null, error: "private detail" });
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("Recovery required");
    expect(screen.queryByText("private detail")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Unload model-downloaded/ })).not.toBeInTheDocument();
    act(() => setupState.callbacks()?.onSnapshot({ cursor: 10, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onEvent({ sequence: 9, operation: operation("running") }));
    expect(screen.queryByRole("progressbar", { name: "Download progress for model-ready" })).not.toBeInTheDocument();
  });

  it("keeps the newest terminal node refresh when an older request resolves last", async () => {
    const setupState = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await screen.findByRole("heading", { name: "model-downloaded" });
    let resolveOlder!: (value: Awaited<ReturnType<ModelsScreenServices["getControlNode"]>>) => void;
    let resolveOlderInventory!: (value: ModelInventoryEntry[]) => void;
    vi.mocked(setupState.api.getControlNode)
      .mockImplementationOnce(() => new Promise((resolve) => { resolveOlder = resolve; }))
      .mockResolvedValueOnce({ status: "ready", activeModelId: "model-downloaded", operationId: null, error: null });
    vi.mocked(setupState.api.getInventory)
      .mockImplementationOnce(() => new Promise((resolve) => { resolveOlderInventory = resolve; }))
      .mockResolvedValueOnce([model("model-downloaded", { kind: "downloaded" })]);
    act(() => setupState.callbacks()?.onEvent({ sequence: 2, operation: { ...operation("succeeded"), id: "old" } }));
    act(() => setupState.callbacks()?.onEvent({ sequence: 3, operation: { ...operation("succeeded"), id: "new", updatedAtUnixMs: 3 } }));
    expect(await screen.findByText("Active")).toBeInTheDocument();
    resolveOlder({ status: "unloaded", activeModelId: null, operationId: null, error: null });
    resolveOlderInventory([model("model-downloaded", { kind: "not_downloaded" })]);
    await Promise.resolve();
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(screen.getByText("Downloaded and verified")).toBeInTheDocument();
  });

  it("starts a download, then renders only the authoritative operation response", async () => {
    const user = userEvent.setup();
    const { api } = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={api} />);
    await user.click(await screen.findByRole("button", { name: "Download model-ready" }));
    expect(screen.getByRole("button", { name: "Resume model-partial" })).toBeDisabled();

    expect(api.downloadModel).toHaveBeenCalledWith("http://127.0.0.1:8080", token, "model-ready", expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(api.getOperation).toHaveBeenCalledWith("http://127.0.0.1:8080", token, "op-1", expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(await screen.findByText("Download queued")).toBeInTheDocument();
  });

  it("applies snapshot and progress events, announces byte progress, and cancels by operation ID", async () => {
    const user = userEvent.setup();
    const setupState = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await screen.findByRole("heading", { name: "model-ready" });

    act(() => setupState.callbacks()?.onSnapshot({ cursor: 3, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onEvent({ sequence: 4, operation: operation("running") }));

    expect(screen.getByRole("progressbar", { name: "Download progress for model-ready" })).toHaveAttribute("value", "512");
    expect(screen.getByText("512 B of 1 KB")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel download model-ready" }));
    expect(setupState.api.cancelOperation).toHaveBeenCalledWith("http://127.0.0.1:8080", token, "op-1", expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(await screen.findByText(/Last operation: Download cancelled/)).toBeInTheDocument();
  });

  it("renders unknown-total progress as indeterminate instead of a false percentage", async () => {
    const setupState = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await screen.findByRole("heading", { name: "model-ready" });
    const unknownTotal = {
      ...operation("running"),
      progress: { completedBytes: 512, totalBytes: null },
    };
    act(() => setupState.callbacks()?.onSnapshot({ cursor: 1, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onEvent({ sequence: 2, operation: unknownTotal }));

    const progress = screen.getByRole("progressbar", { name: "Download progress for model-ready" });
    expect(progress).not.toHaveAttribute("value");
    expect(progress).not.toHaveAttribute("max");
    expect(screen.getByText("512 B downloaded")).toBeInTheDocument();
  });

  it("polls authoritative inventory only while startup verification is pending", async () => {
    const setupState = setup();
    vi.mocked(setupState.api.getInventory)
      .mockResolvedValueOnce([model("model-verifying", { kind: "invalid", reason: "verification_required" })])
      .mockResolvedValueOnce([model("model-verifying", { kind: "downloaded" })]);
    render(
      <ModelsScreen
        endpoint="http://127.0.0.1:8080"
        services={setupState.api}
        verificationPollMs={0}
      />,
    );

    expect(await screen.findByText("Verification required")).toBeInTheDocument();
    expect(await screen.findByText("Downloaded and verified")).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(setupState.api.getInventory).toHaveBeenCalledTimes(2);
  });

  it("backs off and stops verification polling after its finite retry budget", async () => {
    const setupState = setup();
    vi.mocked(setupState.api.getInventory).mockResolvedValue([
      model("model-verifying", { kind: "invalid", reason: "verification_required" }),
    ]);
    render(
      <ModelsScreen
        endpoint="http://127.0.0.1:8080"
        services={setupState.api}
        verificationPollMs={0}
        verificationPollLimit={2}
      />,
    );

    expect(await screen.findByText(/Refresh Models or wait for a new node update/i)).toBeInTheDocument();
    expect(setupState.api.getInventory).toHaveBeenCalledTimes(3);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(setupState.api.getInventory).toHaveBeenCalledTimes(3);
  });

  it("aborts an in-flight verification refresh on unmount and suppresses its late result", async () => {
    const setupState = setup();
    let resolvePoll!: (value: ModelInventoryEntry[]) => void;
    vi.mocked(setupState.api.getInventory)
      .mockResolvedValueOnce([model("model-verifying", { kind: "invalid", reason: "verification_required" })])
      .mockImplementationOnce(() => new Promise((resolve) => { resolvePoll = resolve; }));
    const view = render(
      <ModelsScreen
        endpoint="http://127.0.0.1:8080"
        services={setupState.api}
        verificationPollMs={0}
      />,
    );
    await vi.waitFor(() => expect(setupState.api.getInventory).toHaveBeenCalledTimes(2));
    const pollSignal = vi.mocked(setupState.api.getInventory).mock.calls[1][2]?.signal;
    view.unmount();
    expect(pollSignal?.aborted).toBe(true);
    resolvePoll([model("model-verifying", { kind: "downloaded" })]);
    await Promise.resolve();
    expect(setupState.api.getInventory).toHaveBeenCalledTimes(2);
  });

  it("reconnects from the last cursor after an unexpected stream failure", async () => {
    const setupState = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} reconnectDelayMs={0} />);
    await screen.findByRole("heading", { name: "model-ready" });
    act(() => setupState.callbacks()?.onSnapshot({ cursor: 7, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onTerminal({ kind: "error", cursor: 7, message: "Live model updates disconnected." }));

    expect(screen.getByRole("status")).toHaveTextContent("Reconnecting");
    await vi.waitFor(() => expect(setupState.api.createControlEventStream).toHaveBeenCalledTimes(2));
    expect(setupState.api.createControlEventStream).toHaveBeenLastCalledWith(
      "http://127.0.0.1:8080", token, 7, expect.any(Object), expect.any(AbortSignal),
    );
  });

  it("keeps retrying a temporarily unavailable reconnect snapshot", async () => {
    const setupState = setup();
    vi.mocked(setupState.api.getControlNode)
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockRejectedValueOnce(new Error("node restarting"))
      .mockResolvedValue({ status: "unloaded", activeModelId: null, operationId: null, error: null });
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} reconnectDelayMs={0} />);
    await screen.findByRole("heading", { name: "model-ready" });
    act(() => setupState.callbacks()?.onSnapshot({ cursor: 9, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onTerminal({ kind: "error", cursor: 9, message: "disconnected" }));

    await vi.waitFor(() => expect(setupState.api.getControlNode).toHaveBeenCalledTimes(3));
    await vi.waitFor(() => expect(setupState.api.createControlEventStream).toHaveBeenCalledTimes(2));
    expect(screen.getByRole("status")).toHaveTextContent(/connecting|connected/i);
  });

  it("caps reconnect attempts and offers an explicit fresh retry", async () => {
    const user = userEvent.setup();
    const setupState = setup();
    vi.mocked(setupState.api.getControlNode)
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockRejectedValue(new Error("node replaced"));
    render(
      <ModelsScreen
        endpoint="http://127.0.0.1:8080"
        services={setupState.api}
        reconnectDelayMs={0}
        reconnectLimit={2}
      />,
    );
    await screen.findByRole("heading", { name: "model-ready" });
    act(() => setupState.callbacks()?.onSnapshot({ cursor: 3, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onTerminal({ kind: "error", cursor: 3, message: "replaced" }));

    const retry = await screen.findByRole("button", { name: "Retry live updates" });
    expect(setupState.api.getControlNode).toHaveBeenCalledTimes(3);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(setupState.api.getControlNode).toHaveBeenCalledTimes(3);

    await user.click(retry);
    await vi.waitFor(() => expect(setupState.api.getControlNode).toHaveBeenCalledTimes(4));
  });

  it("does not reset the reconnect budget for a flapping snapshot then disconnect loop", async () => {
    const setupState = setup();
    render(
      <ModelsScreen
        endpoint="http://127.0.0.1:8080"
        services={setupState.api}
        reconnectDelayMs={0}
        reconnectLimit={2}
      />,
    );
    await screen.findByRole("heading", { name: "model-ready" });

    act(() => setupState.callbacks()?.onSnapshot({ cursor: 1, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onTerminal({ kind: "error", cursor: 1, message: "flap one" }));
    await vi.waitFor(() => expect(setupState.api.createControlEventStream).toHaveBeenCalledTimes(2));

    act(() => setupState.callbacks()?.onSnapshot({ cursor: 2, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onTerminal({ kind: "error", cursor: 2, message: "flap two" }));
    await vi.waitFor(() => expect(setupState.api.createControlEventStream).toHaveBeenCalledTimes(3));

    act(() => setupState.callbacks()?.onSnapshot({ cursor: 3, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onTerminal({ kind: "error", cursor: 3, message: "flap three" }));

    expect(await screen.findByRole("button", { name: "Retry live updates" })).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(setupState.api.createControlEventStream).toHaveBeenCalledTimes(3);
  });

  it("disposes events, aborts requests, and clears work on unmount", async () => {
    const setupState = setup();
    const view = render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await screen.findByRole("heading", { name: "model-ready" });
    view.unmount();
    expect(setupState.handle.dispose).toHaveBeenCalledOnce();
  });

  it("aborts an in-flight mutation and suppresses its follow-up on window close", async () => {
    const user = userEvent.setup();
    const setupState = setup();
    let accept!: (value: { operationId: string }) => void;
    vi.mocked(setupState.api.downloadModel).mockImplementation(() => new Promise((resolve) => {
      accept = resolve;
    }));
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await user.click(await screen.findByRole("button", { name: "Download model-ready" }));
    const options = vi.mocked(setupState.api.downloadModel).mock.calls[0][3];
    expect(options?.signal?.aborted).toBe(false);

    window.dispatchEvent(new Event("beforeunload"));
    expect(options?.signal?.aborted).toBe(true);
    expect(setupState.handle.dispose).toHaveBeenCalledOnce();
    accept({ operationId: "op-late" });
    await Promise.resolve();
    expect(setupState.api.getOperation).not.toHaveBeenCalled();
  });

  it("reports an unavailable safe credential and sends no control request", async () => {
    const setupState = setup();
    vi.mocked(setupState.api.readControlToken).mockRejectedValue(new Error("credential missing"));
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("credential missing");
    expect(setupState.api.getInventory).not.toHaveBeenCalled();
    expect(setupState.api.createControlEventStream).not.toHaveBeenCalled();
  });

  it("binds the native credential request to the exact displayed endpoint", async () => {
    const setupState = setup();
    render(<ModelsScreen endpoint="http://127.0.0.1:18080" services={setupState.api} />);
    await screen.findByRole("heading", { name: "model-ready" });
    expect(setupState.api.readControlToken).toHaveBeenCalledWith("http://127.0.0.1:18080");
  });

  it("sends no authenticated mutation when a same-port replacement fails fresh proof", async () => {
    const user = userEvent.setup();
    const setupState = setup();
    vi.mocked(setupState.api.readControlToken)
      .mockResolvedValueOnce(token)
      .mockResolvedValueOnce(token)
      .mockResolvedValueOnce(token)
      .mockRejectedValueOnce(new Error("replacement identity rejected"));
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await user.click(await screen.findByRole("button", { name: "Download model-ready" }));

    expect(setupState.api.downloadModel).not.toHaveBeenCalled();
    expect(await screen.findByRole("alert")).toHaveTextContent("replacement identity rejected");
  });

  it("keeps refreshed artifact truth primary over contradictory terminal history", async () => {
    const setupState = setup();
    vi.mocked(setupState.api.getInventory)
      .mockResolvedValueOnce([model("model-ready", { kind: "not_downloaded" })])
      .mockResolvedValueOnce([model("model-ready", { kind: "invalid", reason: "checksum_mismatch" })]);
    render(<ModelsScreen endpoint="http://127.0.0.1:8080" services={setupState.api} />);
    await screen.findByRole("heading", { name: "model-ready" });
    act(() => setupState.callbacks()?.onSnapshot({ cursor: 1, cursorGap: false, operations: [], events: [] }));
    act(() => setupState.callbacks()?.onEvent({ sequence: 2, operation: operation("succeeded") }));

    expect(await screen.findByText("Checksum invalid")).toBeInTheDocument();
    expect(screen.getByText(/Last operation: Download completed/)).toBeInTheDocument();
  });
});
