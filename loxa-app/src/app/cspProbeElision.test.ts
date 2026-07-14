// @vitest-environment node
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { afterEach, describe, expect, it } from "vitest";
import { build } from "vite";

const sentinels = [
  "Sanitized probe JSON",
  "consoleCounts",
  "installConsoleCountProbe",
  "securitypolicyviolation",
  "early-blocked-image",
];
const outputs: string[] = [];

async function emittedJavaScript(directory: string) {
  const assets = await readdir(resolve(directory, "assets"));
  const scripts = assets.filter((name) => name.endsWith(".js"));
  return (await Promise.all(scripts.map((name) => readFile(resolve(directory, "assets", name), "utf8")))).join("\n");
}

async function probeBuild(enabled: boolean) {
  const directory = await mkdtemp(join(tmpdir(), "loxa-csp-elision-"));
  outputs.push(directory);
  const previousProbe = process.env.VITE_LOXA_CSP_PROBE;
  const previousCase = process.env.VITE_LOXA_CSP_PROBE_CASE;
  if (enabled) {
    process.env.VITE_LOXA_CSP_PROBE = "1";
    process.env.VITE_LOXA_CSP_PROBE_CASE = "early-blocked-image";
  } else {
    delete process.env.VITE_LOXA_CSP_PROBE;
    delete process.env.VITE_LOXA_CSP_PROBE_CASE;
  }
  try {
    await build({
      configFile: resolve("vite.config.ts"),
      logLevel: "silent",
      build: { outDir: directory, emptyOutDir: true, minify: false },
    });
  } finally {
    if (previousProbe === undefined) delete process.env.VITE_LOXA_CSP_PROBE;
    else process.env.VITE_LOXA_CSP_PROBE = previousProbe;
    if (previousCase === undefined) delete process.env.VITE_LOXA_CSP_PROBE_CASE;
    else process.env.VITE_LOXA_CSP_PROBE_CASE = previousCase;
  }
  return emittedJavaScript(directory);
}

afterEach(async () => {
  await Promise.all(outputs.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

describe("CSP probe build isolation", () => {
  it("elides every evidence sentinel from a normal production build", async () => {
    const output = await probeBuild(false);
    for (const sentinel of sentinels) {
      expect(output.includes(sentinel), `normal build retained ${sentinel}`).toBe(false);
    }
  }, 30_000);

  it("retains the evidence sentinels in an explicit probe build", async () => {
    const output = await probeBuild(true);
    for (const sentinel of sentinels) {
      expect(output.includes(sentinel), `probe build omitted ${sentinel}`).toBe(true);
    }
  }, 30_000);
});
