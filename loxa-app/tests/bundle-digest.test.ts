import { mkdirSync, mkdtempSync, renameSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { calculateBundleDigest, createPackageRecord, formatPackageRecord } from "../scripts/bundle-digest.mjs";

function fixture(order: "forward" | "reverse" = "forward") {
  const root = mkdtempSync(resolve(tmpdir(), "loxa-bundle-digest-"));
  mkdirSync(resolve(root, "Contents"));
  const entries = [
    ["Contents/A.txt", "alpha"],
    ["Contents/Z.txt", "zeta"],
  ] as const;
  for (const [path, contents] of order === "forward" ? entries : [...entries].reverse()) {
    writeFileSync(resolve(root, path), contents);
  }
  symlinkSync(resolve(root, "Contents/Z.txt"), resolve(root, "ignored-link"));
  return root;
}

describe("bundle digest", () => {
  it("hashes sorted POSIX paths and bytes while excluding symlinks", async () => {
    const root = fixture();
    const first = await calculateBundleDigest(root);
    expect(first.files.map(({ path }) => path)).toEqual(["Contents/A.txt", "Contents/Z.txt"]);
    expect(first.bundleDigest).toBe("6cd0e6ff909d18b586d4ef08f4b6970ef2e3afa82ffe21204a27bf7d51f25b89");
    expect((await calculateBundleDigest(fixture("reverse"))).bundleDigest).toBe(first.bundleDigest);

    writeFileSync(resolve(root, "Contents/Z.txt"), "changed");
    const changedBytes = await calculateBundleDigest(root);
    expect(changedBytes.bundleDigest).not.toBe(first.bundleDigest);

    writeFileSync(resolve(root, "Contents/Z.txt"), "zeta");
    renameSync(resolve(root, "Contents/A.txt"), resolve(root, "Contents/B.txt"));
    expect((await calculateBundleDigest(root)).bundleDigest).not.toBe(first.bundleDigest);
  });

  it("creates the exact machine-readable package record", () => {
    const record = createPackageRecord("./relative/Loxa.app", "aarch64-apple-darwin", "debug", "a".repeat(64));
    expect(Object.keys(record)).toEqual(["bundlePath", "target", "profile", "bundleDigest"]);
    expect(record).toEqual({
      bundlePath: resolve("./relative/Loxa.app"),
      target: "aarch64-apple-darwin",
      profile: "debug",
      bundleDigest: "a".repeat(64),
    });
    expect(formatPackageRecord(record)).toBe(JSON.stringify(record));
    expect(() => createPackageRecord("Loxa.app", "target", "profile", "bad-hash")).toThrow(/profile|digest/);
  });

  it("rejects record-delimiter characters in paths", async () => {
    const root = mkdtempSync(resolve(tmpdir(), "loxa-bundle-path-"));
    writeFileSync(resolve(root, "bad\nname"), "content");
    await expect(calculateBundleDigest(root)).rejects.toThrow(/newline|NUL/);
  });
});
