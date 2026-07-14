import { afterEach, describe, expect, it } from "vitest";

import { appServices, DESKTOP_RUNTIME_UNAVAILABLE_MESSAGE } from "./services";

describe("desktop app services in browser preview", () => {
  afterEach(() => {
    Reflect.deleteProperty(window, "__TAURI_INTERNALS__");
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
});
