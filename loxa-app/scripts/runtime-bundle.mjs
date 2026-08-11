import { createHash } from "node:crypto";
import {
  chmod,
  cp,
  copyFile,
  lstat,
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  readlink,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const BUILD = "b10344";
const COMMIT = "7a20b417f4526cae073bd997af5020cea3e7ccbe";
const VERSION_LINE = "version: 10344 (7a20b417f)";
const MINIMUM_MACOS = "13.3";
const FRAMEWORK_RPATH = "@executable_path/../Frameworks";
const RELOCATION = `install_name_tool -add_rpath ${FRAMEWORK_RPATH} llama-server`;
const PROVENANCE_SHA256 = "402029fbca52d7835acea49f515e83c3213e234bfbc78a474f7a0435afc95721";
const TRANSACTION_MARKER = ".loxa-runtime-bundle-transaction.json";

const upstreamRegular = new Map([
  ["LICENSE", [1078, "94f29bbed6a22c35b992c5c6ebf0e7c92f13b836b90f36f461c9cf2f0f1d010d", false]],
  ["llama-server", [33472, "d3bce60d45758268a90e0fca82ce5a22d5c35ecb92a06d1c544ed68ec2efa769", true]],
  ["libllama-server-impl.dylib", [9609768, "8fe5fbfb9cf291ece5091425a07429e164e860e875d9be19f0aded4b48612967", true]],
  ["libllama-common.0.0.10344.dylib", [7880808, "f372ff2e8379d981971e6559e76371c8d06500b37934fb5562d20059a41727b4", true]],
  ["libmtmd.0.0.10344.dylib", [1238112, "a3ca5155cddc7a7f1e82873a95aeb864c87f614a200589e25ef715474e707b97", true]],
  ["libllama.0.0.10344.dylib", [2830416, "79562cedcbf44083c2402e931ddeb25239de229dc6f26a86c63c71d20b62d825", true]],
  ["libggml.0.19.0.dylib", [59872, "d86c98babd7e63d9d0ead8cdd240bd2011a6d1a7996a52f87f21ae634853c20e", true]],
  ["libggml-cpu.0.19.0.dylib", [918064, "ba6d4d66eefae0da8d5927a9c4e379011c1472ad090939b356f9b4240f04c115", true]],
  ["libggml-blas.0.19.0.dylib", [58776, "b469f062cc2800e27b20e45e7653ab8af545838e16a03368f059d370dc3ab3a2", true]],
  ["libggml-metal.0.19.0.dylib", [884456, "5cac42dbd02198b55bd03365132b757f87553462bf07b2273116b2226b5e4670", true]],
  ["libggml-rpc.0.19.0.dylib", [133776, "494cc6414b6d5015d8fc67d641f04b2eff7b4aad8d6d99b028e74fa138098240", true]],
  ["libggml-base.0.19.0.dylib", [729960, "858b9b16c2bc4fe82eba70c4a2ee6159a2af2ab93ac0da7525eb3ee54d3c69ec", true]],
  ["upstream-provenance.json", [3846, PROVENANCE_SHA256, false]],
]);

const dylibs = [
  "libllama-server-impl.dylib",
  "libllama-common.0.0.10344.dylib",
  "libmtmd.0.0.10344.dylib",
  "libllama.0.0.10344.dylib",
  "libggml.0.19.0.dylib",
  "libggml-cpu.0.19.0.dylib",
  "libggml-blas.0.19.0.dylib",
  "libggml-metal.0.19.0.dylib",
  "libggml-rpc.0.19.0.dylib",
  "libggml-base.0.19.0.dylib",
];

const links = new Map([
  ["libllama-common.0.dylib", "libllama-common.0.0.10344.dylib"],
  ["libmtmd.0.dylib", "libmtmd.0.0.10344.dylib"],
  ["libllama.0.dylib", "libllama.0.0.10344.dylib"],
  ["libggml.0.dylib", "libggml.0.19.0.dylib"],
  ["libggml-cpu.0.dylib", "libggml-cpu.0.19.0.dylib"],
  ["libggml-blas.0.dylib", "libggml-blas.0.19.0.dylib"],
  ["libggml-metal.0.dylib", "libggml-metal.0.19.0.dylib"],
  ["libggml-rpc.0.dylib", "libggml-rpc.0.19.0.dylib"],
  ["libggml-base.0.dylib", "libggml-base.0.19.0.dylib"],
]);

const codeRelative = ["MacOS/llama-server", ...dylibs.map((name) => `Frameworks/${name}`)];
const dataRelative = [
  `Resources/loxa-runtime/${BUILD}/LICENSE`,
  `Resources/loxa-runtime/${BUILD}/upstream-provenance.json`,
  `Resources/loxa-runtime/${BUILD}/normalized-inventory.json`,
];

function invariant(condition, message) {
  if (!condition) throw new Error(message);
}

function run(program, args, options = {}) {
  const result = spawnSync(program, args, {
    encoding: "utf8",
    env: { HOME: "/nonexistent", PATH: "/usr/bin:/bin", LC_ALL: "C", ...options.env },
    ...options,
  });
  invariant(result.status === 0, `${program} ${args.join(" ")} failed: ${result.stderr || result.stdout}`);
  return result;
}

async function sha256(path) {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

async function safeRegular(path, label, executable) {
  const metadata = await lstat(path).catch(() => null);
  invariant(metadata?.isFile() && metadata.nlink === 1, `${label} must be a single-link regular file`);
  invariant(((metadata.mode & 0o111) !== 0) === executable, `${label} has the wrong executable mode`);
  return metadata;
}

function exactSet(actual, expected, label) {
  assertNoDuplicates(actual, label);
  invariant(
    actual.length === expected.length && actual.every((value) => expected.includes(value)),
    `${label} has a missing or unexpected entry`,
  );
}

function assertNoDuplicates(values, label) {
  invariant(new Set(values).size === values.length, `${label} contains a duplicate entry`);
}

function parseMinimum(output, label) {
  invariant((output.match(/\bcmd LC_BUILD_VERSION\b/g) ?? []).length === 1, `${label} must have one LC_BUILD_VERSION`);
  const match = output.match(/\bminos (\d+)\.(\d+)(?:\.(\d+))?/);
  invariant(match, `${label} has no minimum macOS version`);
  return [Number(match[1]), Number(match[2]), Number(match[3] ?? 0)];
}

export function atMost13_3(version) {
  const [major, minor, patch] = version;
  return major < 13 || (major === 13 && (minor < 3 || (minor === 3 && patch === 0)));
}

function dependencies(path) {
  return run("/usr/bin/otool", ["-L", path]).stdout
    .split(/\r?\n/)
    .slice(1)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.split(" (compatibility version", 1)[0]);
}

function installNames(path) {
  return run("/usr/bin/otool", ["-D", path]).stdout
    .split(/\r?\n/)
    .slice(1)
    .map((line) => line.trim())
    .filter(Boolean);
}

function inspectMachO(path, relativePath, frameworkNames, normalized) {
  const architectures = run("/usr/bin/lipo", ["-archs", path]).stdout.trim().split(/\s+/);
  invariant(architectures.length === 1 && architectures[0] === "arm64", `${relativePath} must be thin arm64`);
  const loadCommands = run("/usr/bin/otool", ["-l", path]).stdout;
  invariant(atMost13_3(parseMinimum(loadCommands, relativePath)), `${relativePath} minimum macOS exceeds ${MINIMUM_MACOS}`);
  if (basename(relativePath) === "llama-server") {
    if (normalized) {
      invariant(loadCommands.includes(`path ${FRAMEWORK_RPATH} (offset`), "llama-server is missing the Frameworks rpath");
    }
    invariant(installNames(path).length === 0, "llama-server has an unexpected install name");
  } else {
    invariant(loadCommands.includes("path @loader_path (offset"), `${relativePath} is missing @loader_path`);
    const names = installNames(path);
    invariant(names.length === 1 && names[0].startsWith("@rpath/"), `${relativePath} has an unsafe install name`);
    invariant(frameworkNames.has(names[0].slice("@rpath/".length)), `${relativePath} has an unresolved install name`);
  }
  for (const dependency of dependencies(path)) {
    if (dependency.startsWith("/System/Library/") || dependency.startsWith("/usr/lib/")) continue;
    invariant(dependency.startsWith("@rpath/"), `${relativePath} has an unsafe dependency ${dependency}`);
    invariant(frameworkNames.has(dependency.slice("@rpath/".length)), `${relativePath} has an unresolved dependency ${dependency}`);
  }
}

async function jsonFile(path, label) {
  const text = await readFile(path, "utf8");
  invariant(Buffer.byteLength(text) <= 128 * 1024, `${label} is too large`);
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${label} is invalid JSON`);
  }
}

function inventoryIdentity(inventory) {
  invariant(inventory.schema_version === 1, "runtime inventory schema is invalid");
  invariant(inventory.runtime?.build === BUILD, "runtime inventory build is invalid");
  invariant(inventory.runtime?.commit === COMMIT, "runtime inventory commit is invalid");
  invariant(inventory.runtime?.version_line === VERSION_LINE, "runtime inventory version is invalid");
  invariant(inventory.runtime?.architecture === "arm64", "runtime inventory architecture is invalid");
  invariant(inventory.runtime?.minimum_macos === MINIMUM_MACOS, "runtime inventory minimum macOS is invalid");
}

async function inventoryEntry(contents, path, mach_o) {
  const fullPath = join(contents, path);
  const metadata = await safeRegular(fullPath, path, mach_o);
  return { path, size: metadata.size, sha256: await sha256(fullPath), mach_o };
}

async function writeJson(path, value) {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8", mode: 0o644, flag: "wx" });
}

export async function verifyUpstreamClosure(vendorPath) {
  const vendor = resolve(vendorPath);
  const directory = await lstat(vendor).catch(() => null);
  invariant(directory?.isDirectory(), "upstream runtime directory is missing");
  const expectedNames = [...upstreamRegular.keys(), ...links.keys()];
  exactSet(await readdir(vendor), expectedNames, "upstream runtime directory");
  const frameworkNames = new Set([...dylibs, ...links.keys()]);
  for (const [name, [size, expectedSha, executable]] of upstreamRegular) {
    const path = join(vendor, name);
    const metadata = await safeRegular(path, `upstream ${name}`, executable);
    invariant(metadata.size === size, `upstream ${name} has the wrong size`);
    invariant((await sha256(path)) === expectedSha, `upstream ${name} has the wrong SHA-256`);
    if (executable) {
      inspectMachO(path, name, frameworkNames, false);
      run("/usr/bin/codesign", ["--verify", "--strict", "--verbose=4", path]);
    }
  }
  for (const [name, target] of links) {
    const path = join(vendor, name);
    const metadata = await lstat(path).catch(() => null);
    invariant(metadata?.isSymbolicLink(), `upstream ${name} must be a symlink`);
    invariant((await readlink(path)) === target, `upstream ${name} has an unsafe symlink target`);
  }
  const provenance = await jsonFile(join(vendor, "upstream-provenance.json"), "upstream provenance");
  invariant(provenance.release?.tag === BUILD && provenance.release?.commit === COMMIT, "upstream provenance release is invalid");
  invariant(provenance.asset?.size === 11024576, "upstream provenance asset size is invalid");
  invariant(provenance.asset?.sha256 === "24bf4348ddc6d1d9b465105ed8ae371e326c576623ac612fbff73532181c8f13", "upstream provenance asset digest is invalid");
  invariant((await sha256(join(vendor, "upstream-provenance.json"))) === PROVENANCE_SHA256, "upstream provenance bytes are invalid");
}

function appLayout(appPath) {
  const app = resolve(appPath);
  invariant(app.endsWith(".app"), "application path must end in .app");
  const contents = join(app, "Contents");
  return {
    app,
    contents,
    macos: join(contents, "MacOS"),
    frameworks: join(contents, "Frameworks"),
    resources: join(contents, `Resources/loxa-runtime/${BUILD}`),
  };
}

async function verifyInfoPlist(contents) {
  const plist = join(contents, "Info.plist");
  await safeRegular(plist, "Info.plist", false);
  const value = run("/usr/libexec/PlistBuddy", ["-c", "Print :LSMinimumSystemVersion", plist]).stdout.trim();
  invariant(value === MINIMUM_MACOS, `Info.plist LSMinimumSystemVersion must be ${MINIMUM_MACOS}`);
  const executable = run("/usr/libexec/PlistBuddy", ["-c", "Print :CFBundleExecutable", plist]).stdout.trim();
  invariant(executable === "loxa-app", "Info.plist CFBundleExecutable must be loxa-app");
  return executable;
}

async function inspectMainExecutable(contents, executable) {
  const relativePath = `MacOS/${executable}`;
  const path = join(contents, relativePath);
  await safeRegular(path, "main application executable", true);
  const architectures = run("/usr/bin/lipo", ["-archs", path]).stdout.trim().split(/\s+/);
  invariant(architectures.length === 1 && architectures[0] === "arm64", "main application executable must be thin arm64");
  const minimum = parseMinimum(run("/usr/bin/otool", ["-l", path]).stdout, relativePath);
  invariant(minimum[0] === 13 && minimum[1] === 3 && minimum[2] === 0, "main application executable minimum macOS must be 13.3");
}

async function inspectCodeClosure(contents, normalized) {
  const frameworkNames = new Set([...dylibs, ...links.keys()]);
  for (const relativePath of codeRelative) {
    const path = join(contents, relativePath);
    await safeRegular(path, relativePath, true);
    inspectMachO(path, relativePath, frameworkNames, normalized);
  }
}

export async function inspectFinalBundle(appPath) {
  const { contents, frameworks, resources } = appLayout(appPath);
  const executable = await verifyInfoPlist(contents);
  await inspectMainExecutable(contents, executable);
  exactSet(await readdir(frameworks), [...dylibs, ...links.keys()], "packaged Frameworks");
  exactSet(
    await readdir(resources),
    ["LICENSE", "upstream-provenance.json", "normalized-inventory.json", "inventory.json"],
    "packaged runtime resources",
  );
  const inventory = await jsonFile(join(resources, "inventory.json"), "final runtime inventory");
  inventoryIdentity(inventory);
  exactSet(inventory.regular_files.map((entry) => entry.path), [...codeRelative, ...dataRelative], "final inventory regular files");
  exactSet(inventory.symlinks.map((entry) => entry.path), [...links.keys()].map((name) => `Frameworks/${name}`), "final inventory symlinks");
  for (const entry of inventory.regular_files) {
    invariant(typeof entry.size === "number" && entry.size > 0, `${entry.path} inventory size is invalid`);
    invariant(/^[0-9a-f]{64}$/.test(entry.sha256), `${entry.path} inventory SHA-256 is invalid`);
    const expectedMachO = codeRelative.includes(entry.path);
    invariant(entry.mach_o === expectedMachO, `${entry.path} inventory kind is invalid`);
    const current = await inventoryEntry(contents, entry.path, expectedMachO);
    invariant(current.size === entry.size, `${entry.path} packaged size is invalid`);
    invariant(current.sha256 === entry.sha256, `${entry.path} packaged SHA-256 is invalid`);
  }
  for (const entry of inventory.symlinks) {
    const name = basename(entry.path);
    invariant(entry.target === links.get(name), `${entry.path} inventory symlink target is invalid`);
    const path = join(contents, entry.path);
    const metadata = await lstat(path).catch(() => null);
    invariant(metadata?.isSymbolicLink(), `${entry.path} must be a symlink`);
    invariant((await readlink(path)) === entry.target, `${entry.path} packaged symlink is unsafe`);
  }
  invariant(await sha256(join(resources, "LICENSE")) === upstreamRegular.get("LICENSE")[1], "packaged LICENSE is invalid");
  invariant(await sha256(join(resources, "upstream-provenance.json")) === PROVENANCE_SHA256, "packaged provenance is invalid");
  const normalized = await jsonFile(join(resources, "normalized-inventory.json"), "normalized inventory");
  invariant(normalized.schema_version === 1 && normalized.relocation === RELOCATION, "normalized relocation metadata is invalid");
  exactSet(normalized.regular_files.map((entry) => entry.path), codeRelative, "normalized inventory");
  await inspectCodeClosure(contents, true);
  return inventory;
}

async function finalizeStagedAppBundle(appPath, vendor, options) {
  const { app, contents, macos, frameworks, resources } = appLayout(appPath);
  const executable = await verifyInfoPlist(contents);
  await inspectMainExecutable(contents, executable);
  invariant(!(await lstat(join(macos, "llama-server")).catch(() => null)), "packaged llama-server already exists");
  invariant(!(await lstat(resources).catch(() => null)), "packaged runtime resources already exist");
  const existingFrameworks = await lstat(frameworks).catch(() => null);
  if (existingFrameworks) {
    invariant(existingFrameworks.isDirectory() && (await readdir(frameworks)).length === 0, "packaged Frameworks is not empty");
  } else {
    await mkdir(frameworks, { mode: 0o755 });
  }
  await mkdir(resources, { recursive: true, mode: 0o755 });

  await copyFile(join(vendor, "llama-server"), join(macos, "llama-server"));
  await chmod(join(macos, "llama-server"), 0o755);
  for (const name of dylibs) {
    await copyFile(join(vendor, name), join(frameworks, name));
    await chmod(join(frameworks, name), 0o755);
  }
  for (const [name, target] of links) await symlink(target, join(frameworks, name));
  await copyFile(join(vendor, "LICENSE"), join(resources, "LICENSE"));
  await chmod(join(resources, "LICENSE"), 0o644);
  await copyFile(join(vendor, "upstream-provenance.json"), join(resources, "upstream-provenance.json"));
  await chmod(join(resources, "upstream-provenance.json"), 0o644);

  run("/usr/bin/install_name_tool", ["-add_rpath", FRAMEWORK_RPATH, join(macos, "llama-server")]);
  if (options.injectFailureAt === "after-relocation") {
    throw new Error("injected failure after relocation");
  }
  await inspectCodeClosure(contents, true);
  const normalized = {
    schema_version: 1,
    relocation: RELOCATION,
    regular_files: await Promise.all(codeRelative.map((path) => inventoryEntry(contents, path, true))),
  };
  await writeJson(join(resources, "normalized-inventory.json"), normalized);

  for (const relativePath of [...codeRelative].reverse()) {
    const path = join(contents, relativePath);
    run("/usr/bin/codesign", ["--force", "--sign", "-", "--timestamp=none", path]);
    run("/usr/bin/codesign", ["--verify", "--strict", "--verbose=4", path]);
  }
  const inventory = {
    schema_version: 1,
    runtime: {
      build: BUILD,
      commit: COMMIT,
      version_line: VERSION_LINE,
      architecture: "arm64",
      minimum_macos: MINIMUM_MACOS,
    },
    regular_files: await Promise.all([
      ...codeRelative.map((path) => inventoryEntry(contents, path, true)),
      ...dataRelative.map((path) => inventoryEntry(contents, path, false)),
    ]),
    symlinks: [...links].map(([name, target]) => ({ path: `Frameworks/${name}`, target })),
  };
  await writeJson(join(resources, "inventory.json"), inventory);
  await inspectFinalBundle(app);
  run("/usr/bin/codesign", ["--force", "--sign", "-", "--timestamp=none", app]);
  run("/usr/bin/codesign", ["--verify", "--deep", "--strict", "--verbose=4", app]);
  await inspectFinalBundle(app);

  const version = run(join(macos, "llama-server"), ["--version"]);
  const versionLines = `${version.stdout}\n${version.stderr}`
    .split(/\r?\n/)
    .filter((line) => line.startsWith("version:"));
  invariant(versionLines.length === 1 && versionLines[0] === VERSION_LINE, "packaged llama-server version is invalid");
  return inventory;
}

async function inspectAlreadyFinalized(appPath) {
  const { app, macos, resources } = appLayout(appPath);
  const helper = await lstat(join(macos, "llama-server")).catch(() => null);
  const inventoryFile = await lstat(join(resources, "inventory.json")).catch(() => null);
  if (!helper && !inventoryFile) return null;

  const inventory = await inspectFinalBundle(app);
  run("/usr/bin/codesign", ["--verify", "--deep", "--strict", "--verbose=4", app]);
  const version = run(join(macos, "llama-server"), ["--version"]);
  const versionLines = `${version.stdout}\n${version.stderr}`
    .split(/\r?\n/)
    .filter((line) => line.startsWith("version:"));
  invariant(versionLines.length === 1 && versionLines[0] === VERSION_LINE, "packaged llama-server version is invalid");
  return inventory;
}

async function verifyRecoverableApp(appPath) {
  const finalized = await inspectAlreadyFinalized(appPath);
  if (finalized) return;
  const { contents } = appLayout(appPath);
  const executable = await verifyInfoPlist(contents);
  await inspectMainExecutable(contents, executable);
}

async function transactionMarkerMatches(transaction, appName) {
  const markerPath = join(transaction, TRANSACTION_MARKER);
  const metadata = await lstat(markerPath).catch(() => null);
  if (!metadata?.isFile() || metadata.nlink !== 1 || (metadata.mode & 0o111) !== 0) return false;
  const marker = await jsonFile(markerPath, "runtime finalizer transaction marker").catch(() => null);
  return marker !== null
    && Object.keys(marker).sort().join(",") === "app,schema_version"
    && marker.schema_version === 1
    && marker.app === appName;
}

async function recoverInterruptedFinalization(appPath) {
  const app = resolve(appPath);
  const parent = dirname(app);
  const appName = basename(app);
  const prefix = `.${appName}.runtime-bundle-`;
  const transactions = [];
  for (const entry of await readdir(parent, { withFileTypes: true })) {
    if (!entry.isDirectory() || !entry.name.startsWith(prefix)) continue;
    const transaction = join(parent, entry.name);
    if (await transactionMarkerMatches(transaction, appName)) transactions.push(transaction);
  }
  invariant(transactions.length <= 1, "multiple interrupted runtime finalizer transactions exist");
  if (transactions.length === 0) return;

  const transaction = transactions[0];
  const stagedApp = join(transaction, appName);
  const originalApp = join(transaction, "original.app");
  const entries = await readdir(transaction);
  invariant(
    entries.every((entry) => [TRANSACTION_MARKER, appName, "original.app"].includes(entry)),
    "interrupted runtime finalizer transaction has an unexpected entry",
  );
  const publicMetadata = await lstat(app).catch(() => null);
  const stagedMetadata = await lstat(stagedApp).catch(() => null);
  const originalMetadata = await lstat(originalApp).catch(() => null);
  invariant(!stagedMetadata || stagedMetadata.isDirectory(), "interrupted staged app is invalid");
  invariant(!originalMetadata || originalMetadata.isDirectory(), "interrupted original app is invalid");

  if (!publicMetadata) {
    invariant(originalMetadata?.isDirectory(), "interrupted finalization has no recoverable original app");
    await verifyRecoverableApp(originalApp);
    await rename(originalApp, app);
    await rm(transaction, { recursive: true, force: true });
    return;
  }
  invariant(publicMetadata.isDirectory(), "application path is invalid during finalizer recovery");
  invariant(!(stagedMetadata && originalMetadata), "interrupted finalization state is ambiguous");
  if (originalMetadata) {
    invariant(!stagedMetadata, "promoted finalization retained an unexpected staged app");
    invariant(await inspectAlreadyFinalized(app), "promoted finalization is not complete");
  } else {
    await verifyRecoverableApp(app);
  }
  await rm(transaction, { recursive: true, force: true });
}

export async function finalizeAppBundle(appPath, vendorPath, options = {}) {
  invariant(
    options && typeof options === "object" && !Array.isArray(options),
    "runtime finalizer options are invalid",
  );
  invariant(
    options.injectFailureAt === undefined
      || ["after-relocation", "after-original-move", "after-promotion"].includes(options.injectFailureAt),
    "runtime finalizer injection point is invalid",
  );
  const vendor = resolve(vendorPath);
  await verifyUpstreamClosure(vendor);
  const app = resolve(appPath);
  await recoverInterruptedFinalization(app);
  const finalized = await inspectAlreadyFinalized(app);
  if (finalized) return finalized;

  const stagingRoot = await mkdtemp(join(dirname(app), `.${basename(app)}.runtime-bundle-`));
  const stagedApp = join(stagingRoot, basename(app));
  const originalApp = join(stagingRoot, "original.app");
  let originalMoved = false;
  try {
    await writeJson(join(stagingRoot, TRANSACTION_MARKER), {
      schema_version: 1,
      app: basename(app),
    });
    await cp(app, stagedApp, {
      recursive: true,
      errorOnExist: true,
      force: false,
      verbatimSymlinks: true,
    });
    const inventory = await finalizeStagedAppBundle(stagedApp, vendor, options);
    await rename(app, originalApp);
    originalMoved = true;
    if (options.injectFailureAt === "after-original-move") {
      throw new Error("injected interruption after original move");
    }
    try {
      await rename(stagedApp, app);
    } catch (error) {
      await rename(originalApp, app);
      originalMoved = false;
      throw error;
    }
    if (options.injectFailureAt === "after-promotion") {
      throw new Error("injected interruption after promotion");
    }
    originalMoved = false;
    await rm(originalApp, { recursive: true, force: true });
    return inventory;
  } finally {
    if (!originalMoved) {
      await rm(stagingRoot, { recursive: true, force: true });
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  invariant(args.length === 2 && args[0] === "--app", "usage: runtime-bundle.mjs --app /path/to/Loxa.app");
  const app = resolve(args[1]);
  const script = dirname(fileURLToPath(import.meta.url));
  const vendor = resolve(script, "../src-tauri/runtime/b10344/upstream");
  await finalizeAppBundle(app, vendor);
  process.stdout.write(`Finalized ${relative(process.cwd(), app).split(sep).join("/")} with ${BUILD}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`Runtime bundle finalization failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}
