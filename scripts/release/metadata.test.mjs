import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

test("all publishable Rust crates share the release version and rust floor", () => {
  assert.equal(readFileSync("release/VERSION", "utf8").trim(), "0.1.0");
  for (const manifest of [
    "Cargo.toml",
    "loxa/Cargo.toml",
    "loxa-core/Cargo.toml",
    "loxa-node/Cargo.toml",
    "loxa-protocol/Cargo.toml",
  ]) {
    const source = readFileSync(manifest, "utf8");
    assert.match(source, /version(?:\.workspace)?\s*=\s*"?0\.1\.0"?/);
    assert.ok(source.includes('rust-version = "1.96"'));
  }
});

test("release output and package adapter staging are untracked", () => {
  const ignore = readFileSync(".gitignore", "utf8");
  assert.ok(ignore.includes("/release/out/"));
  assert.ok(ignore.includes("/packages/npm/cli-darwin-arm64/bin/"));
  assert.ok(ignore.includes("/packages/python/loxa/src/loxa/_bin/"));
});
