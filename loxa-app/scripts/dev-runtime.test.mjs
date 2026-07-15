import net from "node:net";

import { afterEach, describe, expect, it } from "vitest";

import { buildDevConfig, buildTauriArgs, startFrontend } from "./dev-runtime.mjs";

const closeables = [];

afterEach(async () => {
  await Promise.all(closeables.splice(0).map((close) => close()));
});

describe("desktop development runtime", () => {
  it("starts and owns Vite on the next available loopback port", async () => {
    const occupied = net.createServer();
    await new Promise((resolve, reject) => {
      occupied.once("error", reject);
      occupied.listen(24_210, "127.0.0.1", resolve);
    });
    closeables.push(() => new Promise((resolve) => occupied.close(resolve)));

    const frontend = await startFrontend({ preferredPort: 24_210, configFile: false });
    closeables.push(() => frontend.server.close());

    expect(frontend.origin).toBe("http://127.0.0.1:24211");
  });

  it("gives Tauri the already-owned Vite origin without starting a second server", () => {
    expect(buildDevConfig("http://127.0.0.1:14221")).toEqual({
      build: {
        devUrl: "http://127.0.0.1:14221",
        beforeDevCommand: "",
      },
    });
  });

  it("keeps non-development Tauri commands unchanged", () => {
    expect(buildTauriArgs(["build", "--debug"], "http://127.0.0.1:1420")).toEqual(["build", "--debug"]);
    expect(buildTauriArgs(["info"], "http://127.0.0.1:1420")).toEqual(["info"]);
  });

  it("merges the selected origin into Tauri development arguments", () => {
    const args = buildTauriArgs(["dev", "--release"], "http://127.0.0.1:1421");
    expect(args[0]).toBe("dev");
    expect(args[1]).toBe("--config");
    expect(JSON.parse(args[2])).toEqual(buildDevConfig("http://127.0.0.1:1421"));
    expect(args.slice(3)).toEqual(["--release"]);
  });
});
