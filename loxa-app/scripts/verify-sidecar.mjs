import { createHash } from "node:crypto";
import { accessSync, constants, readFileSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const appRoot = process.env.LOXA_SIDECAR_APP_ROOT || resolve(dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(readFileSync(resolve(appRoot, "src-tauri/binaries/loxa-node-manifest.json"), "utf8"));
if (process.platform !== "darwin" || !manifest.triple.endsWith("-apple-darwin")) {
  throw new Error("the Loxa .app sidecar verifier is macOS-only and requires an Apple Darwin target");
}
const binary = resolve(appRoot, "src-tauri/binaries", manifest.destination);
accessSync(binary, constants.R_OK | (process.platform === "win32" ? 0 : constants.X_OK));
if (!statSync(binary).isFile()) throw new Error("sidecar is not a regular file");
const hash = createHash("sha256").update(readFileSync(binary)).digest("hex");
if (hash !== manifest.sourceHash || hash !== manifest.destinationHash) throw new Error("sidecar hash verification failed");
const identity = spawnSync("file", [binary], { encoding: "utf8" });
const expectedArchitecture = manifest.triple.startsWith("aarch64-") ? "arm64" : manifest.triple.startsWith("x86_64-") ? "x86_64" : null;
if (!expectedArchitecture || identity.status !== 0 || !identity.stdout.toLowerCase().includes(expectedArchitecture)) {
  throw new Error(`sidecar architecture mismatch: ${identity.stdout || identity.stderr}`);
}
const bundlePath = process.argv.slice(2).find((argument) => argument !== "--");
if (bundlePath) {
  const packaged = resolve(bundlePath, "Contents/MacOS/loxa-node");
  accessSync(packaged, constants.R_OK | constants.X_OK);
  const packagedHash = createHash("sha256").update(readFileSync(packaged)).digest("hex");
  if (packagedHash !== hash) throw new Error("packaged sidecar hash mismatch");
}
