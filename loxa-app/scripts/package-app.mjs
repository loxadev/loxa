import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const rustc = spawnSync("rustc", ["-vV"], { encoding: "utf8" });
const host = rustc.stdout.match(/^host: (.+)$/m)?.[1];
const requestedIndex = process.argv.indexOf("--target");
const target = requestedIndex >= 0 ? process.argv[requestedIndex + 1] : host;
if (!target) throw new Error("package target is required");
const debug = process.argv.includes("--debug");
const build = spawnSync(
  "pnpm",
  ["tauri", "build", "--target", target, "--bundles", "app", ...(debug ? ["--debug"] : [])],
  {
    cwd: appRoot,
    env: { ...process.env, LOXA_SIDECAR_TARGET: target },
    stdio: "inherit",
  },
);
if (build.status !== 0) process.exit(build.status ?? 1);
const profile = debug ? "debug" : "release";
const bundle = resolve(appRoot, "src-tauri/target", target, profile, "bundle/macos/Loxa.app");
const verify = spawnSync("node", ["scripts/verify-sidecar.mjs", bundle], { cwd: appRoot, stdio: "inherit" });
if (verify.status !== 0) process.exit(verify.status ?? 1);
