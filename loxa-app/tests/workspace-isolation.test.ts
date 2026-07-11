import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const appRoot = resolve(import.meta.dirname, "..");
const repositoryRoot = resolve(appRoot, "..");

describe("Cargo workspace isolation", () => {
  it("starts with no frontend capability permissions", () => {
    const capability = JSON.parse(
      readFileSync(resolve(appRoot, "src-tauri/capabilities/default.json"), "utf8"),
    ) as { permissions: string[] };

    expect(capability.permissions).toEqual([]);
  });

  it("declares the desktop crate as its own workspace", () => {
    const manifest = readFileSync(resolve(appRoot, "src-tauri/Cargo.toml"), "utf8");
    expect(manifest).toMatch(/^\[workspace\]$/m);
  });

  it("keeps Tauri and WebKit outside root cargo metadata", () => {
    const metadata = JSON.parse(
      execFileSync("cargo", ["metadata", "--format-version", "1"], {
        cwd: repositoryRoot,
        encoding: "utf8",
        maxBuffer: 100 * 1024 * 1024,
      }),
    ) as { packages: Array<{ name: string }> };

    const packageNames = metadata.packages.map(({ name }) => name);
    expect(packageNames).not.toContain("loxa-app");
    expect(packageNames.some((name) => /tauri|webkit/i.test(name))).toBe(false);
  });
});
