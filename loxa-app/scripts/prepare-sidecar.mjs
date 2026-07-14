import { createHash } from "node:crypto";
import { chmodSync, copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = resolve(appRoot, "..");
const explicitTarget = process.env.LOXA_SIDECAR_TARGET;
let triple = explicitTarget;
if (!triple) {
  const rustc = spawnSync("rustc", ["-vV"], { encoding: "utf8" });
  if (rustc.status !== 0) throw new Error(rustc.stderr || "rustc -vV failed");
  triple = rustc.stdout.match(/^host: (.+)$/m)?.[1];
}
if (!triple || !/^[a-z0-9_]+(?:-[a-z0-9_.]+)+$/.test(triple))
  throw new Error("a valid sidecar target triple is required");
if (process.argv.includes("--print-target")) {
  process.stdout.write(`${triple}\n`);
  process.exit(0);
}

const build = spawnSync("cargo", ["build", "--locked", "--release", "--target", triple, "-p", "loxa-node"], {
  cwd: repositoryRoot,
  encoding: "utf8",
  stdio: "inherit",
});
if (build.status !== 0) process.exit(build.status ?? 1);

const extension = process.platform === "win32" ? ".exe" : "";
const source = resolve(repositoryRoot, "target", triple, "release", `loxa-node${extension}`);
const destination = resolve(appRoot, "src-tauri", "binaries", `loxa-node-${triple}${extension}`);
mkdirSync(dirname(destination), { recursive: true });
copyFileSync(source, destination);
if (process.platform !== "win32") chmodSync(destination, 0o755);

const sha256 = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");
const sourceHash = sha256(source);
const destinationHash = sha256(destination);
if (sourceHash !== destinationHash) throw new Error("sidecar copy hash mismatch");
writeFileSync(
  resolve(appRoot, "src-tauri", "binaries", "loxa-node-manifest.json"),
  `${JSON.stringify({ triple, sourceHash, destinationHash, destination: `loxa-node-${triple}${extension}` }, null, 2)}\n`,
);
