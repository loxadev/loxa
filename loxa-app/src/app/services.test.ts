import { invoke } from "@tauri-apps/api/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  appServices,
  confirmGlobalDownloadCancel,
  DESKTOP_RUNTIME_UNAVAILABLE_MESSAGE,
  desktopRuntimeUnavailableMessage,
} from "./services";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

describe("desktop app services in browser preview", () => {
  beforeEach(() => {
    vi.mocked(invoke).mockReset();
  });

  afterEach(() => {
    Reflect.deleteProperty(window, "__TAURI_INTERNALS__");
    vi.restoreAllMocks();
  });

  it("rejects Tauri-backed bootstrap calls with a truthful preview error", async () => {
    Reflect.deleteProperty(window, "__TAURI_INTERNALS__");
    const expectedMessage = "Desktop runtime is unavailable in browser preview.";

    expect(DESKTOP_RUNTIME_UNAVAILABLE_MESSAGE).toBe(expectedMessage);

    const calls = [
      () => appServices.bootstrap.snapshot(),
      () => appServices.bootstrap.start({ endpoint: "http://127.0.0.1:8080" }),
      () => appServices.bootstrap.attach("http://127.0.0.1:8080"),
      () => appServices.bootstrap.stop(),
      () => appServices.readControlToken("http://127.0.0.1:8080"),
    ];

    for (const call of calls) {
      await expect(call()).rejects.toEqual(new Error(expectedMessage));
    }
  });

  it("selects truthful missing-runtime copy for development and production", () => {
    expect(desktopRuntimeUnavailableMessage(true)).toBe("Desktop runtime is unavailable in browser preview.");
    expect(desktopRuntimeUnavailableMessage(false)).toBe("Desktop runtime is unavailable.");
  });

  it("asks before globally cancelling a shared download", () => {
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);

    expect(confirmGlobalDownloadCancel()).toBe(true);
    expect(confirm).toHaveBeenCalledWith("Cancel this shared download for every observer connected to this Loxa node?");
  });

  it("forwards exact commands and arguments when the Tauri runtime is available", async () => {
    Object.defineProperty(window, "__TAURI_INTERNALS__", { configurable: true, value: {} });
    vi.mocked(invoke).mockResolvedValue({});

    await appServices.bootstrap.snapshot();
    await appServices.bootstrap.start({ endpoint: "http://127.0.0.1:8080" });
    await appServices.bootstrap.attach("http://127.0.0.1:8181");
    await appServices.bootstrap.stop();
    await appServices.readControlToken("http://127.0.0.1:8282");

    expect(invoke).toHaveBeenNthCalledWith(1, "bootstrap_snapshot", undefined);
    expect(invoke).toHaveBeenNthCalledWith(2, "start_node", {
      request: { endpoint: "http://127.0.0.1:8080" },
    });
    expect(invoke).toHaveBeenNthCalledWith(3, "attach_node", { endpoint: "http://127.0.0.1:8181" });
    expect(invoke).toHaveBeenNthCalledWith(4, "stop_owned_node", undefined);
    expect(invoke).toHaveBeenNthCalledWith(5, "read_control_token", { endpoint: "http://127.0.0.1:8282" });
    expect(invoke).toHaveBeenCalledTimes(5);
  });
});
