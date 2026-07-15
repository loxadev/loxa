import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const appRoot = resolve(import.meta.dirname, "..");
const repositoryRoot = resolve(appRoot, "..");

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return /\.[cm]?[jt]sx?$/.test(entry.name) ? [path] : [];
  });
}

const disallowedIconPackages = [
  "@fortawesome/react-fontawesome",
  "@heroicons/react",
  "@mui/icons-material",
  "@phosphor-icons/react",
  "@radix-ui/react-icons",
  "@tabler/icons-react",
  "iconoir-react",
  "phosphor-react",
  "react-feather",
  "react-icons",
];

function isForeignIconPackage(name: string) {
  return disallowedIconPackages.some((packageName) => name === packageName || name.startsWith(`${packageName}/`));
}

function hasForeignIconImport(source: string) {
  const moduleSpecifiers = [...source.matchAll(/(?:\bfrom\s*|\bimport\s*(?:\(\s*)?)["']([^"']+)["']/g)].map(
    (match) => match[1] ?? "",
  );
  return moduleSpecifiers.some(isForeignIconPackage);
}

function foreignIconDependencies(manifest: {
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}) {
  return [
    ...new Set([...Object.keys(manifest.dependencies ?? {}), ...Object.keys(manifest.devDependencies ?? {})]),
  ].filter(isForeignIconPackage);
}

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
  it("keeps product icons on Lucide and direct Radix imports inside owned UI wrappers", () => {
    const manifest = JSON.parse(readFileSync(resolve(appRoot, "package.json"), "utf8")) as {
      dependencies: Record<string, string>;
      devDependencies: Record<string, string>;
    };
    expect(foreignIconDependencies(manifest)).toEqual([]);

    const files = sourceFiles(resolve(appRoot, "src"));
    const featureViolations = files
      .filter((path) => !path.includes("/components/ui/") && !path.includes(".test."))
      .filter((path) => /from\s+["'](?:radix-ui|@radix-ui\/)/.test(readFileSync(path, "utf8")));
    expect(featureViolations).toEqual([]);

    const iconViolations = files
      .filter((path) => !path.includes(".test."))
      .filter((path) => hasForeignIconImport(readFileSync(path, "utf8")));
    expect(iconViolations).toEqual([]);
  });

  it("rejects every known foreign icon dependency and import shape while allowing Lucide", () => {
    const foreignPackages = [
      "@fortawesome/react-fontawesome",
      "@heroicons/react",
      "@mui/icons-material",
      "@phosphor-icons/react",
      "@radix-ui/react-icons",
      "@tabler/icons-react",
      "iconoir-react",
      "phosphor-react",
      "react-feather",
      "react-icons",
    ];

    for (const packageName of foreignPackages) {
      expect(foreignIconDependencies({ dependencies: { [packageName]: "1.0.0" } }), packageName).toEqual([packageName]);
      expect(foreignIconDependencies({ devDependencies: { [packageName]: "1.0.0" } }), packageName).toEqual([
        packageName,
      ]);
      expect(hasForeignIconImport(`import { Icon } from "${packageName}";`), packageName).toBe(true);
      expect(hasForeignIconImport(`import Icon from "${packageName}/Icon";`), packageName).toBe(true);
      expect(hasForeignIconImport(`import "${packageName}";`), packageName).toBe(true);
      expect(hasForeignIconImport(`const icons = import("${packageName}");`), packageName).toBe(true);
    }

    expect(foreignIconDependencies({ dependencies: { "lucide-react": "1.24.0" } })).toEqual([]);
    expect(hasForeignIconImport('import { Copy } from "lucide-react";')).toBe(false);
    expect(hasForeignIconImport('import mark from "./assets/mark.svg";')).toBe(false);
    expect(hasForeignIconImport('import font from "./assets/font.woff2";')).toBe(false);
  });

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
    expect(frontend).toMatch(/^    runs-on: ubuntu-24\.04$/m);
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

    const packaging = yamlBlock(ci, "frontend-packaging", 2);
    expect(packaging).toMatch(/^    name: desktop package \(macOS\)$/m);
    expect(packaging).toMatch(/^    runs-on: macos-15$/m);
    expect(packaging).toMatch(/^    timeout-minutes: 30$/m);
    expect(packaging).toMatch(/^        working-directory: loxa-app$/m);
    expect(workflowStep(packaging, "Install dependencies")).toMatch(/^        run: pnpm install --frozen-lockfile$/m);
    expect(workflowStep(packaging, "Packaging contract tests")).toMatch(
      /^        run: pnpm exec vitest run tests\/workspace-isolation\.test\.ts$/m,
    );
    expect(workflowStep(packaging, "Build packaged app")).toMatch(/^        run: pnpm package:app -- --debug$/m);
  });

  it("runs latest GitHub runner images as a scheduled compatibility canary", () => {
    const nightly = readFileSync(resolve(repositoryRoot, ".github/workflows/nightly.yml"), "utf8");

    expect(nightly).toMatch(/^name: Nightly Runner Canary$/m);
    expect(nightly).toMatch(/^  schedule:$/m);
    expect(nightly).toMatch(/^    - cron: "0 3 \* \* \*"$/m);
    expect(nightly).toMatch(/^  workflow_dispatch:$/m);
    expect(nightly).toMatch(/^        os: \[ubuntu-latest, macos-latest\]$/m);
    expect(nightly).toMatch(/^    runs-on: ubuntu-latest$/m);
    expect(nightly).toMatch(/^    runs-on: macos-latest$/m);
  });

  it("enforces a deterministic semantic light and dark browser contract", () => {
    const browserConfig = readFileSync(resolve(appRoot, "vitest.browser.config.ts"), "utf8");
    const baselineTest = readFileSync(resolve(appRoot, "src/test/BaselineApp.browser.test.tsx"), "utf8");
    const fontCss = readFileSync(resolve(appRoot, "src/styles/fonts.css"), "utf8");
    const themeCss = readFileSync(resolve(appRoot, "src/styles/theme.css"), "utf8");

    expect(browserConfig).not.toContain("toMatchScreenshot");
    expect(baselineTest).toContain('document.fonts.load(`400 12px "IBM Plex Mono"`, "No active model")');
    expect(baselineTest).toContain('document.fonts.check(`400 12px "IBM Plex Mono"`, "No active model")');
    expect(baselineTest).toContain('getComputedStyle(chatHeading.element()).fontFamily).toContain("ui-sans-serif")');
    expect(baselineTest).toContain('getComputedStyle(document.body).fontFamily).toContain("ui-sans-serif")');
    expect(baselineTest).toContain("document.documentElement.scrollWidth");
    expect(baselineTest).toContain("composerWithinViewport()");
    expect(baselineTest).toContain("await expectNoAxeViolations(document)");
    expect(themeCss).toContain(
      '--loxa-font-sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    );
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

  it("selects one available loopback port for both Tauri and Vite development", () => {
    const packageJson = JSON.parse(readFileSync(resolve(appRoot, "package.json"), "utf8")) as {
      scripts: Record<string, string>;
    };
    const launcher = readFileSync(resolve(appRoot, "scripts/tauri.mjs"), "utf8");
    const runtime = readFileSync(resolve(appRoot, "scripts/dev-runtime.mjs"), "utf8");
    const sidecar = readFileSync(resolve(appRoot, "scripts/prepare-sidecar.mjs"), "utf8");

    expect(packageJson.scripts.tauri).toBe("node scripts/tauri.mjs");
    expect(launcher).toContain("LOXA_DEV_ORIGIN");
    expect(launcher).toContain("startFrontend");
    expect(runtime).toContain("http://127.0.0.1:");
    expect(runtime).toContain("strictPort: false");
    expect(runtime).not.toContain("0.0.0.0");
    expect(sidecar).toContain('process.argv.includes("--dev")');
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
    const prepareScript = readFileSync(resolve(appRoot, "scripts/prepare-sidecar.mjs"), "utf8");
    expect(prepareScript.indexOf("if (!triple)")).toBeLessThan(prepareScript.indexOf('spawnSync("rustc"'));
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
    const expectedFailure =
      process.platform === "darwin"
        ? /sidecar hash verification failed/
        : /macOS-only and requires an Apple Darwin target/;
    expect(() =>
      execFileSync(process.execPath, [resolve(appRoot, "scripts/verify-sidecar.mjs")], {
        env: { ...process.env, LOXA_SIDECAR_APP_ROOT: root },
        stdio: "pipe",
      }),
    ).toThrow(expectedFailure);
  });

  it("declares the desktop crate as its own workspace", () => {
    const manifest = readFileSync(resolve(appRoot, "src-tauri/Cargo.toml"), "utf8");
    expect(manifest).toMatch(/^\[workspace\]$/m);
  });

  it("keeps Tauri and WebKit outside root cargo metadata", () => {
    const rootManifest = readFileSync(resolve(repositoryRoot, "Cargo.toml"), "utf8");
    expect(rootManifest).not.toMatch(/loxa-app|tauri|webkit/i);
  });
});
