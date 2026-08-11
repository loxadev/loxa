import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile, copyFile, chmod, lstat, readlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

import {
  atMost13_3,
  finalizeAppBundle,
  inspectFinalBundle,
  verifyUpstreamClosure,
} from "./runtime-bundle.mjs";

const appRoot = resolve(import.meta.dirname, "..");
const vendor = join(appRoot, "src-tauri/runtime/b10344/upstream");

test("the artifact floor comparison rejects a patch release above 13.3.0", () => {
  assert.equal(atMost13_3([12, 6, 9]), true);
  assert.equal(atMost13_3([13, 2, 99]), true);
  assert.equal(atMost13_3([13, 3, 0]), true);
  assert.equal(atMost13_3([13, 3, 1]), false);
  assert.equal(atMost13_3([13, 4, 0]), false);
  assert.equal(atMost13_3([14, 0, 0]), false);
});

function run(program, args) {
  const result = spawnSync(program, args, {
    encoding: "utf8",
    env: { HOME: "/nonexistent", PATH: "/usr/bin:/bin", LC_ALL: "C" },
  });
  assert.equal(result.status, 0, `${program} ${args.join(" ")}\n${result.stdout}\n${result.stderr}`);
  return result;
}

test("one finalizer produces the exact flat signed app runtime from frozen upstream bytes", async () => {
  const root = await mkdtemp(join(tmpdir(), "loxa-runtime-finalizer-"));
  try {
    const app = join(root, "Loxa.app");
    const contents = join(app, "Contents");
    const main = join(contents, "MacOS/loxa-app");
    await mkdir(dirname(main), { recursive: true, mode: 0o755 });
    await mkdir(join(contents, "Resources"), { recursive: true, mode: 0o755 });
    await copyFile(join(vendor, "llama-server"), main);
    await chmod(main, 0o755);
    await writeFile(
      join(contents, "Info.plist"),
      `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>loxa-app</string>
<key>CFBundleIdentifier</key><string>dev.loxa.runtime-test</string>
<key>LSMinimumSystemVersion</key><string>13.3</string>
</dict></plist>
`,
    );

    await verifyUpstreamClosure(vendor);
    await finalizeAppBundle(app, vendor);
    const inspected = await inspectFinalBundle(app);

    assert.equal(inspected.runtime.build, "b10344");
    assert.equal(inspected.runtime.version_line, "version: 10344 (7a20b417f)");
    assert.equal(inspected.regular_files.length, 14);
    assert.equal(inspected.symlinks.length, 9);
    assert.equal(
      await readlink(join(contents, "Frameworks/libggml.0.dylib")),
      "libggml.0.19.0.dylib",
    );
    assert.equal((await lstat(join(contents, "MacOS/llama-server"))).isFile(), true);
    assert.equal(
      (await readFile(join(contents, "Resources/loxa-runtime/b10344/LICENSE"))).length,
      1078,
    );

    const version = run(join(contents, "MacOS/llama-server"), ["--version"]);
    const lines = `${version.stdout}\n${version.stderr}`
      .split(/\r?\n/)
      .filter(Boolean);
    assert.deepEqual(lines.filter((line) => line.startsWith("version:")), [
      "version: 10344 (7a20b417f)",
    ]);
    run("/usr/bin/codesign", ["--verify", "--deep", "--strict", "--verbose=4", app]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed staged finalization leaves the original exact and an immediate retry succeeds", async () => {
  const root = await mkdtemp(join(tmpdir(), "loxa-runtime-finalizer-retry-"));
  try {
    const app = join(root, "Loxa.app");
    const contents = join(app, "Contents");
    const main = join(contents, "MacOS/loxa-app");
    const plist = join(contents, "Info.plist");
    await mkdir(dirname(main), { recursive: true, mode: 0o755 });
    await mkdir(join(contents, "Resources"), { recursive: true, mode: 0o755 });
    await copyFile(join(vendor, "llama-server"), main);
    await chmod(main, 0o755);
    await writeFile(
      plist,
      `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>loxa-app</string>
<key>CFBundleIdentifier</key><string>dev.loxa.runtime-retry-test</string>
<key>LSMinimumSystemVersion</key><string>13.3</string>
</dict></plist>
`,
    );
    const originalMain = await readFile(main);
    const originalPlist = await readFile(plist);
    const originalEntries = (await readdir(contents)).sort();

    await assert.rejects(
      finalizeAppBundle(app, vendor, { injectFailureAt: "after-relocation" }),
      /injected failure after relocation/,
    );

    assert.deepEqual(await readFile(main), originalMain);
    assert.deepEqual(await readFile(plist), originalPlist);
    assert.deepEqual((await readdir(contents)).sort(), originalEntries);
    assert.equal(await lstat(join(contents, "MacOS/llama-server")).catch(() => null), null);

    await finalizeAppBundle(app, vendor);
    const finalizedInventory = await readFile(
      join(contents, "Resources/loxa-runtime/b10344/inventory.json"),
    );
    await finalizeAppBundle(app, vendor);
    assert.deepEqual(
      await readFile(join(contents, "Resources/loxa-runtime/b10344/inventory.json")),
      finalizedInventory,
      "an already-finalized invocation must be a verified no-op",
    );
    assert.equal((await inspectFinalBundle(app)).runtime.build, "b10344");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("an interruption after moving the original app is recovered before an immediate retry", async () => {
  const root = await mkdtemp(join(tmpdir(), "loxa-runtime-finalizer-interrupted-"));
  try {
    const app = join(root, "Loxa.app");
    const contents = join(app, "Contents");
    const main = join(contents, "MacOS/loxa-app");
    const plist = join(contents, "Info.plist");
    await mkdir(dirname(main), { recursive: true, mode: 0o755 });
    await mkdir(join(contents, "Resources"), { recursive: true, mode: 0o755 });
    await copyFile(join(vendor, "llama-server"), main);
    await chmod(main, 0o755);
    await writeFile(
      plist,
      `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>loxa-app</string>
<key>CFBundleIdentifier</key><string>dev.loxa.runtime-interrupted-test</string>
<key>LSMinimumSystemVersion</key><string>13.3</string>
</dict></plist>
`,
    );
    const originalMain = await readFile(main);
    const originalPlist = await readFile(plist);

    await assert.rejects(
      finalizeAppBundle(app, vendor, { injectFailureAt: "after-original-move" }),
      /injected interruption after original move/,
    );

    assert.equal(await lstat(app).catch(() => null), null, "the fixture did not reach the missing-app crash state");
    const transactions = (await readdir(root)).filter((name) =>
      name.startsWith(".Loxa.app.runtime-bundle-"),
    );
    assert.equal(transactions.length, 1, "the interrupted transaction was not retained exactly once");
    const transaction = join(root, transactions[0]);
    assert.deepEqual(await readFile(join(transaction, "original.app/Contents/MacOS/loxa-app")), originalMain);
    assert.deepEqual(await readFile(join(transaction, "original.app/Contents/Info.plist")), originalPlist);

    await finalizeAppBundle(app, vendor);

    assert.equal((await inspectFinalBundle(app)).runtime.build, "b10344");
    assert.deepEqual(
      (await readdir(root)).filter((name) => name.startsWith(".Loxa.app.runtime-bundle-")),
      [],
      "the recovered transaction survived the retry",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("an interruption after promotion is cleaned by a verified no-op retry", async () => {
  const root = await mkdtemp(join(tmpdir(), "loxa-runtime-finalizer-promoted-"));
  try {
    const app = join(root, "Loxa.app");
    const contents = join(app, "Contents");
    const main = join(contents, "MacOS/loxa-app");
    await mkdir(dirname(main), { recursive: true, mode: 0o755 });
    await mkdir(join(contents, "Resources"), { recursive: true, mode: 0o755 });
    await copyFile(join(vendor, "llama-server"), main);
    await chmod(main, 0o755);
    await writeFile(
      join(contents, "Info.plist"),
      `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>loxa-app</string>
<key>CFBundleIdentifier</key><string>dev.loxa.runtime-promoted-test</string>
<key>LSMinimumSystemVersion</key><string>13.3</string>
</dict></plist>
`,
    );

    await assert.rejects(
      finalizeAppBundle(app, vendor, { injectFailureAt: "after-promotion" }),
      /injected interruption after promotion/,
    );
    const beforeRetry = await readFile(
      join(contents, "Resources/loxa-runtime/b10344/inventory.json"),
    );
    assert.equal((await inspectFinalBundle(app)).runtime.build, "b10344");

    await finalizeAppBundle(app, vendor);

    assert.deepEqual(
      await readFile(join(contents, "Resources/loxa-runtime/b10344/inventory.json")),
      beforeRetry,
      "the recovery retry changed the already-promoted app",
    );
    assert.deepEqual(
      (await readdir(root)).filter((name) => name.startsWith(".Loxa.app.runtime-bundle-")),
      [],
      "the promoted transaction survived the verified no-op retry",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
