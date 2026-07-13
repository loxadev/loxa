import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
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

  it("bundles the private node without granting frontend process or filesystem access", () => {
    const config = JSON.parse(
      readFileSync(resolve(appRoot, "src-tauri/tauri.conf.json"), "utf8"),
    ) as { bundle: { externalBin?: string[] }; app: { security: { csp: string } } };

    expect(config.bundle.externalBin).toEqual(["binaries/loxa-node"]);
    expect(config.app.security.csp).not.toMatch(/shell|filesystem|fs:/i);
    const bootstrap = readFileSync(resolve(appRoot, "src-tauri/src/bootstrap.rs"), "utf8");
    expect(bootstrap).not.toContain("LOXA_NODE_EXECUTABLE");
    expect(bootstrap).not.toMatch(/PathBuf::from\("loxa(?:-node)?"\)/);
    const packageJson = JSON.parse(readFileSync(resolve(appRoot, "package.json"), "utf8")) as {
      scripts: Record<string, string>;
    };
    expect(packageJson.scripts["build:desktop"]).toContain("prepare:sidecar");
    expect(packageJson.scripts["verify:sidecar"]).toContain("verify-sidecar.mjs");
    expect(packageJson.scripts["package:app"]).toContain("package-app.mjs");
  });

  it("uses the one exact development origin allowed by the control service", () => {
    const config = JSON.parse(
      readFileSync(resolve(appRoot, "src-tauri/tauri.conf.json"), "utf8"),
    ) as { build: { devUrl: string } };
    const vite = readFileSync(resolve(appRoot, "vite.config.ts"), "utf8");

    expect(config.build.devUrl).toBe("http://127.0.0.1:1420");
    expect(vite).toContain('host: "127.0.0.1"');
    expect(vite).not.toContain("TAURI_DEV_HOST");
  });

  it("prepares the fixed private sidecar before starting Vite", () => {
    const config = JSON.parse(
      readFileSync(resolve(appRoot, "src-tauri/tauri.conf.json"), "utf8"),
    ) as { build: { beforeDevCommand: string } };

    expect(config.build.beforeDevCommand).toBe("pnpm prepare:sidecar && pnpm dev");
  });

  it("keeps desktop diagnostics debug-only, structured, and non-persistent", () => {
    const manifest = readFileSync(resolve(appRoot, "src-tauri/Cargo.toml"), "utf8");
    const bootstrap = readFileSync(resolve(appRoot, "src-tauri/src/bootstrap.rs"), "utf8");
    const nativeShell = readFileSync(resolve(appRoot, "src-tauri/src/lib.rs"), "utf8");

    expect(manifest).toContain(
      'tracing = { version = "=0.1.44", default-features = false, features = ["std", "max_level_debug", "release_max_level_off"] }',
    );
    expect(manifest).toContain(
      'tracing-subscriber = { version = "=0.3.23", default-features = false, features = ["fmt", "std"] }',
    );
    const tracingDependencies = manifest
      .split("\n")
      .filter((line) => line.startsWith("tracing"))
      .join("\n");
    expect(tracingDependencies).not.toMatch(/env-filter|regex|json|ansi/i);
    expect(nativeShell).toContain("#[cfg(debug_assertions)]");
    expect(nativeShell).toContain("try_init");
    expect(bootstrap).toContain("Stdio::inherit()");
    expect(bootstrap).toContain("Stdio::null()");
    expect(bootstrap).not.toMatch(/tracing::(?:debug|info|warn|error)!\([^)]*(?:token|nonce|proof|authorization|prompt|response|credential_path)/s);
  });

  it("selects an explicit packaging target instead of silently using the host", () => {
    const selected = execFileSync(process.execPath, [resolve(appRoot, "scripts/prepare-sidecar.mjs"), "--print-target"], {
      encoding: "utf8",
      env: { ...process.env, LOXA_SIDECAR_TARGET: "x86_64-apple-darwin" },
    });
    expect(selected.trim()).toBe("x86_64-apple-darwin");
  });

  it("fails closed when the prepared sidecar hash differs from its manifest", () => {
    const root = mkdtempSync(resolve(tmpdir(), "loxa-sidecar-proof-"));
    const binaries = resolve(root, "src-tauri/binaries");
    mkdirSync(binaries, { recursive: true });
    const binary = resolve(binaries, "loxa-node-aarch64-apple-darwin");
    writeFileSync(binary, "not-the-reviewed-binary");
    chmodSync(binary, 0o755);
    writeFileSync(resolve(binaries, "loxa-node-manifest.json"), JSON.stringify({
      triple: "aarch64-apple-darwin",
      sourceHash: "0".repeat(64),
      destinationHash: "0".repeat(64),
      destination: "loxa-node-aarch64-apple-darwin",
    }));
    expect(() => execFileSync(process.execPath, [resolve(appRoot, "scripts/verify-sidecar.mjs")], {
      env: { ...process.env, LOXA_SIDECAR_APP_ROOT: root },
      stdio: "pipe",
    })).toThrow(/sidecar hash verification failed/);
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
