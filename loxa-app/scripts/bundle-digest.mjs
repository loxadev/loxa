import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

async function regularFiles(root, directory = root) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const absolute = resolve(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await regularFiles(root, absolute)));
    if (entry.isFile()) files.push(absolute);
  }
  return files;
}

export async function calculateBundleDigest(bundlePath) {
  const root = resolve(bundlePath);
  const paths = (await regularFiles(root)).map((absolute) => ({
    absolute,
    path: relative(root, absolute).split(sep).join("/"),
  }));
  paths.sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));

  const files = [];
  for (const file of paths) {
    if (/[\n\0]/.test(file.path)) throw new Error("bundle paths may not contain newline or NUL characters");
    files.push({ path: file.path, sha256: sha256(await readFile(file.absolute)) });
  }
  const manifest = files.map(({ path, sha256: fileHash }) => `${path}\0${fileHash}\n`).join("");
  return { bundleDigest: sha256(Buffer.from(manifest, "utf8")), files };
}

export function createPackageRecord(bundlePath, target, profile, bundleDigest) {
  if (profile !== "debug" && profile !== "release") throw new Error("package profile must be debug or release");
  if (!/^[0-9a-f]{64}$/.test(bundleDigest)) throw new Error("bundle digest must be lowercase SHA-256");
  return { bundlePath: resolve(bundlePath), target, profile, bundleDigest };
}

export function formatPackageRecord(record) {
  return JSON.stringify(record);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const bundlePath = process.argv[2];
  if (!bundlePath) throw new Error("bundle path is required");
  const { bundleDigest } = await calculateBundleDigest(bundlePath);
  process.stdout.write(`${bundleDigest}\n`);
}
