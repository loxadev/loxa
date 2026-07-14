import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const appRoot = resolve(import.meta.dirname, "..");
const repositoryRoot = resolve(appRoot, "..");

function yamlBlock(source: string, key: string, indent: number) {
  const lines = source.split("\n");
  const padding = " ".repeat(indent);
  const start = lines.findIndex((line) => line === `${padding}${key}:`);
  if (start < 0) throw new Error(`missing ${key} block`);
  let end = start + 1;
  while (end < lines.length) {
    const line = lines[end];
    if (line?.trim() && (line.match(/^ */)?.[0].length ?? 0) <= indent) break;
    end += 1;
  }
  return lines.slice(start, end).join("\n");
}

function workflowStep(job: string, name: string) {
  const lines = job.split("\n");
  const start = lines.findIndex((line) => line === `      - name: ${name}`);
  if (start < 0) throw new Error(`missing ${name} step`);
  let end = start + 1;
  while (end < lines.length && !lines[end]?.startsWith("      - name: ")) end += 1;
  return lines.slice(start, end).join("\n");
}

describe("Cargo workspace isolation", () => {
  it("keeps generated Tauri schemas outside the formatting gate", () => {
    const prettierIgnore = readFileSync(resolve(appRoot, ".prettierignore"), "utf8").split("\n");
    expect(prettierIgnore).toContain("src-tauri/gen/schemas");
  });

  it("enforces the complete desktop frontend gate in CI", () => {
    const manifest = JSON.parse(readFileSync(resolve(appRoot, "package.json"), "utf8"));
    expect(manifest.scripts).toMatchObject({
      "format:check": "prettier --check .",
      "test:unit": "vitest run",
      "test:browser": "vitest run --config vitest.browser.config.ts",
      check: "pnpm format:check && pnpm lint && pnpm typecheck && pnpm test:unit && pnpm test:browser && pnpm build",
    });

    const ci = readFileSync(resolve(repositoryRoot, ".github/workflows/ci.yml"), "utf8");
    const frontend = yamlBlock(ci, "frontend", 2);
    expect(frontend).toMatch(/^    name: desktop frontend$/m);
    expect(frontend).toMatch(/^    runs-on: ubuntu-latest$/m);
    expect(frontend).toMatch(/^    timeout-minutes: 20$/m);
    expect(frontend).toMatch(/^        working-directory: loxa-app$/m);

    expect(workflowStep(frontend, "Checkout")).toMatch(
      /^        uses: actions\/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0(?: # v7\.0\.0)?\n        with:\n          persist-credentials: false$/m,
    );
    expect(workflowStep(frontend, "Install pnpm")).toMatch(
      /^        uses: pnpm\/action-setup@9fd676a19091d4595eefd76e4bd31c97133911f1(?: # v4\.2\.0)?\n        with:\n          version: 11\.7\.0$/m,
    );
    expect(workflowStep(frontend, "Install Node")).toMatch(
      /^        uses: actions\/setup-node@2028fbc5c25fe9cf00d9f06a71cc4710d4507903(?: # v6\.0\.0)?\n        with:\n          node-version: 24\.18\.0\n          cache: pnpm\n          cache-dependency-path: loxa-app\/pnpm-lock\.yaml$/m,
    );

    const commands = new Map([
      ["Install dependencies", "pnpm install --frozen-lockfile"],
      ["Install Chromium", "pnpm exec playwright install --with-deps chromium"],
      ["Check formatting", "pnpm format:check"],
      ["Lint", "pnpm lint"],
      ["Typecheck", "pnpm typecheck"],
      ["Unit tests", "pnpm test:unit"],
      ["Browser tests", "pnpm test:browser"],
      ["Build", "pnpm build"],
    ]);
    for (const [name, command] of commands) {
      expect(workflowStep(frontend, name)).toMatch(new RegExp(`^        run: ${command.replaceAll(".", "\\.")}$`, "m"));
    }
  });

  it("enforces a deterministic shared-platform browser baseline", () => {
    const browserConfig = readFileSync(resolve(appRoot, "vitest.browser.config.ts"), "utf8");
    const baselineTest = readFileSync(resolve(appRoot, "src/test/BaselineApp.browser.test.tsx"), "utf8");
    const fontCss = readFileSync(resolve(appRoot, "src/styles/fonts.css"), "utf8");

    expect(browserConfig).toContain("platform");
    expect(browserConfig).toMatch(/"__screenshots__",\s*"shared",\s*browserName/);
    expect(baselineTest).toContain('document.fonts.load(`600 48px "Instrument Sans"`, "Node")');
    expect(baselineTest).toContain('document.fonts.load(`500 12px "IBM Plex Mono"`, "LOCAL RUNTIME")');
    expect(baselineTest).toContain('document.fonts.check(`600 48px "Instrument Sans"`, "Node")');
    expect(baselineTest).toContain('document.fonts.check(`500 12px "IBM Plex Mono"`, "LOCAL RUNTIME")');
    expect(baselineTest).toContain('animations: "disabled"');
    expect(baselineTest).toContain('caret: "hide"');
    expect(baselineTest).toContain('scale: "css"');
    expect(baselineTest).toContain("allowedMismatchedPixelRatio: 0.005");
    expect(baselineTest).toContain("await expectNoAxeViolations(document)");
    expect(fontCss).toContain('url("../assets/fonts/InstrumentSans-Variable.woff2")');
    expect(fontCss).toContain('url("../assets/fonts/IBMPlexMono-Regular.woff2")');
    expect(fontCss).toContain('url("../assets/fonts/IBMPlexMono-Medium.woff2")');
  });

  it("starts with no frontend capability permissions", () => {
    const capability = JSON.parse(readFileSync(resolve(appRoot, "src-tauri/capabilities/default.json"), "utf8")) as {
      permissions: string[];
    };

    expect(capability.permissions).toEqual([]);
  });

  it("bundles the private node without granting frontend process or filesystem access", () => {
    const config = JSON.parse(readFileSync(resolve(appRoot, "src-tauri/tauri.conf.json"), "utf8")) as {
      bundle: { externalBin?: string[] };
      app: { security: { csp: string } };
    };

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
    const config = JSON.parse(readFileSync(resolve(appRoot, "src-tauri/tauri.conf.json"), "utf8")) as {
      build: { devUrl: string };
    };
    const vite = readFileSync(resolve(appRoot, "vite.config.ts"), "utf8");

    expect(config.build.devUrl).toBe("http://127.0.0.1:1420");
    expect(vite).toContain('host: "127.0.0.1"');
    expect(vite).not.toContain("TAURI_DEV_HOST");
  });

  it("prepares the fixed private sidecar before starting Vite", () => {
    const config = JSON.parse(readFileSync(resolve(appRoot, "src-tauri/tauri.conf.json"), "utf8")) as {
      build: { beforeDevCommand: string };
    };

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
    expect(bootstrap).not.toMatch(
      /tracing::(?:debug|info|warn|error)!\([^)]*(?:token|nonce|proof|authorization|prompt|response|credential_path)/s,
    );
  });

  it("selects an explicit packaging target instead of silently using the host", () => {
    const selected = execFileSync(
      process.execPath,
      [resolve(appRoot, "scripts/prepare-sidecar.mjs"), "--print-target"],
      {
        encoding: "utf8",
        env: { ...process.env, LOXA_SIDECAR_TARGET: "x86_64-apple-darwin" },
      },
    );
    expect(selected.trim()).toBe("x86_64-apple-darwin");
  });

  it("verifies and digests the bundle before printing the exact package record", () => {
    const packageScript = readFileSync(resolve(appRoot, "scripts/package-app.mjs"), "utf8");
    expect(packageScript).toContain(
      'import { calculateBundleDigest, createPackageRecord, formatPackageRecord } from "./bundle-digest.mjs"',
    );
    const verifyIndex = packageScript.indexOf('spawnSync("node", ["scripts/verify-sidecar.mjs", bundle]');
    const digestIndex = packageScript.indexOf("calculateBundleDigest(bundle)");
    const recordIndex = packageScript.indexOf("createPackageRecord(bundle, target, profile, bundleDigest)");
    const outputIndex = packageScript.indexOf("console.log(formatPackageRecord(packageRecord))");
    expect(verifyIndex).toBeGreaterThan(-1);
    expect(digestIndex).toBeGreaterThan(verifyIndex);
    expect(recordIndex).toBeGreaterThan(digestIndex);
    expect(outputIndex).toBeGreaterThan(recordIndex);
  });

  it("fails closed when the prepared sidecar hash differs from its manifest", () => {
    const root = mkdtempSync(resolve(tmpdir(), "loxa-sidecar-proof-"));
    const binaries = resolve(root, "src-tauri/binaries");
    mkdirSync(binaries, { recursive: true });
    const binary = resolve(binaries, "loxa-node-aarch64-apple-darwin");
    writeFileSync(binary, "not-the-reviewed-binary");
    chmodSync(binary, 0o755);
    writeFileSync(
      resolve(binaries, "loxa-node-manifest.json"),
      JSON.stringify({
        triple: "aarch64-apple-darwin",
        sourceHash: "0".repeat(64),
        destinationHash: "0".repeat(64),
        destination: "loxa-node-aarch64-apple-darwin",
      }),
    );
    expect(() =>
      execFileSync(process.execPath, [resolve(appRoot, "scripts/verify-sidecar.mjs")], {
        env: { ...process.env, LOXA_SIDECAR_APP_ROOT: root },
        stdio: "pipe",
      }),
    ).toThrow(/sidecar hash verification failed/);
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
