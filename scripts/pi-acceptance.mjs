#!/usr/bin/env node

import { createHash } from "node:crypto";
import { lstat, readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PHASES = new Set(["mac-local", "windows-tailnet", "post-recovery"]);
const DIGEST_PATTERN = /^[a-f0-9]{64}$/;
const TAILNET_HOSTNAME_PATTERN =
  /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+ts\.net$/;
const MAX_ARGUMENT_LENGTH = 4096;
const MAX_BASE_URL_LENGTH = 2048;
const MAX_TRACE_RECORDS = 10_000;

export class QualificationRequiredError extends Error {
  constructor() {
    super(
      "Pi CLI qualification required before live execution; invocation flags and JSONL event names are not pinned.",
    );
    this.name = "QualificationRequiredError";
  }
}

function fail(message) {
  throw new Error(message);
}

function isPlainObject(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}

function validateBoundedString(value, label, maximum = MAX_ARGUMENT_LENGTH) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maximum ||
    value.includes("\0")
  ) {
    fail(`${label} is invalid`);
  }
  return value;
}

function validateDigest(digest, label = "provider config digest") {
  if (typeof digest !== "string" || !DIGEST_PATTERN.test(digest)) {
    fail(`${label} must be a lowercase SHA-256 digest`);
  }
  return digest;
}

function validatePhase(phase) {
  if (!PHASES.has(phase)) {
    fail("phase must be mac-local, windows-tailnet, or post-recovery");
  }
  return phase;
}

export function validateBaseUrl(value) {
  validateBoundedString(value, "base URL", MAX_BASE_URL_LENGTH);
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    fail("base URL must be a valid absolute URL");
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    fail("base URL must use http or https");
  }
  if (
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash ||
    parsed.pathname !== "/v1" ||
    !parsed.hostname ||
    parsed.hostname === "0.0.0.0"
  ) {
    fail("base URL must be credential-free and end at exact /v1");
  }
  return parsed;
}

export function validatePhaseEndpoint(
  phase,
  baseUrl,
  expectedConfigSha256,
) {
  validatePhase(phase);
  const parsed = validateBaseUrl(baseUrl);
  if (phase === "mac-local" && parsed.hostname !== "127.0.0.1") {
    fail("mac-local requires the IPv4 loopback endpoint");
  }
  if (phase === "windows-tailnet") {
    if (parsed.protocol !== "https:") {
      fail("windows-tailnet requires https");
    }
    if (
      parsed.hostname === "127.0.0.1" ||
      parsed.hostname === "localhost" ||
      parsed.hostname === "::1" ||
      parsed.hostname === "[::1]"
    ) {
      fail("windows-tailnet requires a non-loopback tailnet endpoint");
    }
    if (!TAILNET_HOSTNAME_PATTERN.test(parsed.hostname)) {
      fail("windows-tailnet requires a syntactically valid tailnet hostname");
    }
  }
  if (phase === "post-recovery" && expectedConfigSha256 === undefined) {
    fail("post-recovery requires the expected provider config digest");
  }
  if (expectedConfigSha256 !== undefined) {
    validateDigest(expectedConfigSha256, "expected provider config digest");
  }
  return parsed;
}

export function validateModelsConfig(config) {
  if (!isPlainObject(config) || !isPlainObject(config.providers)) {
    fail("models config must contain a providers object");
  }
  const providerNames = Object.keys(config.providers);
  if (providerNames.length !== 1 || providerNames[0] !== "loxa") {
    fail("models config must define exactly the stable loxa provider");
  }
  const provider = config.providers.loxa;
  if (!isPlainObject(provider)) {
    fail("loxa provider config must be an object");
  }
  if ("compat" in provider) {
    fail("compat overrides require live qualification");
  }
  validateBaseUrl(provider.baseUrl);
  if (provider.api !== "openai-completions") {
    fail("loxa provider must use openai-completions");
  }
  if (provider.apiKey !== "loxa-dummy-key") {
    fail("loxa provider must use the documented dummy key");
  }
  if (!Array.isArray(provider.models) || provider.models.length !== 1) {
    fail("loxa provider must define exactly one model");
  }
  const model = provider.models[0];
  if (!isPlainObject(model)) {
    fail("loxa model config must be an object");
  }
  if ("compat" in model) {
    fail("model compat overrides require live qualification");
  }
  if ("maxTokens" in model) {
    fail("maxTokens requires live qualification");
  }
  if (
    model.id !== "loxa" ||
    model.reasoning !== false ||
    !Array.isArray(model.input) ||
    model.input.length !== 1 ||
    model.input[0] !== "text" ||
    model.contextWindow !== 8192
  ) {
    fail("loxa model must be the fixed non-reasoning text-only 8192 profile");
  }
  return {
    providerName: "loxa",
    provider,
    api: provider.api,
    apiKey: provider.apiKey,
    model,
  };
}

export function validateModelsResponse(response) {
  if (
    !isPlainObject(response) ||
    !Array.isArray(response.data) ||
    response.data.length > 10_000 ||
    !response.data.some(
      (model) => isPlainObject(model) && model.id === "loxa",
    )
  ) {
    fail("models response does not contain the stable loxa model");
  }
  return true;
}

export function validateReadyStatus(response) {
  if (
    !isPlainObject(response) ||
    response.health !== "ready" ||
    response.model !== "loxa"
  ) {
    fail("status response is not ready for the stable loxa model");
  }
  return true;
}

function sourceValue(source, key, caseInsensitive) {
  if (Object.hasOwn(source, key)) {
    return source[key];
  }
  if (!caseInsensitive) {
    return undefined;
  }
  const found = Object.keys(source).find(
    (candidate) => candidate.toLowerCase() === key.toLowerCase(),
  );
  return found === undefined ? undefined : source[found];
}

function copyAllowedEnvironment(source, keys, caseInsensitive) {
  const environment = {};
  for (const key of keys) {
    const value = sourceValue(source, key, caseInsensitive);
    if (
      typeof value === "string" &&
      value.length <= MAX_ARGUMENT_LENGTH &&
      !value.includes("\0")
    ) {
      environment[key] = value;
    }
  }
  return environment;
}

export function buildIsolatedChildEnvironment(
  platform,
  { home, temp, source = {} },
) {
  if (!isPlainObject(source)) {
    fail("child environment source must be an object");
  }
  validateBoundedString(home, "isolated home");
  validateBoundedString(temp, "isolated temp");
  if (platform === "darwin") {
    if (!path.posix.isAbsolute(home) || !path.posix.isAbsolute(temp)) {
      fail("isolated Mac home and temp must be absolute");
    }
    return {
      ...copyAllowedEnvironment(
        source,
        ["PATH", "LANG", "LC_ALL", "LC_CTYPE"],
        false,
      ),
      HOME: home,
      XDG_CONFIG_HOME: path.posix.join(home, ".config"),
      XDG_CACHE_HOME: path.posix.join(home, ".cache"),
      XDG_DATA_HOME: path.posix.join(home, ".local", "share"),
      TMPDIR: temp,
    };
  }
  if (platform === "win32") {
    if (
      !path.win32.isAbsolute(home) ||
      !path.win32.isAbsolute(temp) ||
      !/^[A-Za-z]:\\/.test(home)
    ) {
      fail("isolated Windows home and temp must be absolute drive paths");
    }
    const drive = home.slice(0, 2);
    const homePath = home.slice(2);
    return {
      ...copyAllowedEnvironment(
        source,
        [
          "PATH",
          "PATHEXT",
          "SYSTEMROOT",
          "WINDIR",
          "COMSPEC",
          "LANG",
          "LC_ALL",
          "LC_CTYPE",
        ],
        true,
      ),
      HOME: home,
      USERPROFILE: home,
      HOMEDRIVE: drive,
      HOMEPATH: homePath,
      APPDATA: path.win32.join(home, "AppData", "Roaming"),
      LOCALAPPDATA: path.win32.join(home, "AppData", "Local"),
      XDG_CONFIG_HOME: path.win32.join(home, ".config"),
      XDG_CACHE_HOME: path.win32.join(home, ".cache"),
      XDG_DATA_HOME: path.win32.join(home, ".local", "share"),
      TEMP: temp,
      TMP: temp,
    };
  }
  fail("child environment platform must be darwin or win32");
}

export function validateSemanticToolTrace(records) {
  if (
    !Array.isArray(records) ||
    records.length === 0 ||
    records.length > MAX_TRACE_RECORDS
  ) {
    fail("semantic tool trace is invalid");
  }
  for (const record of records) {
    if (
      !isPlainObject(record) ||
      typeof record.tool !== "string" ||
      record.tool.length > 128 ||
      typeof record.status !== "string" ||
      record.status.length > 32 ||
      (record.stage !== undefined &&
        (typeof record.stage !== "string" || record.stage.length > 64))
    ) {
      fail("semantic tool trace contains an invalid record");
    }
  }
  const required = [
    (record) => record.tool === "read" && record.status === "success",
    (record) =>
      record.tool === "bash" &&
      record.stage === "precheck" &&
      record.status === "success",
    (record) =>
      (record.tool === "edit" || record.tool === "write") &&
      record.status === "success",
    (record) =>
      record.tool === "bash" &&
      record.stage === "verification" &&
      record.status === "success",
  ];
  let cursor = 0;
  for (const matches of required) {
    const found = records.findIndex(
      (record, index) => index >= cursor && matches(record),
    );
    if (found === -1) {
      fail("semantic tool trace is missing the ordered successful tool loop");
    }
    cursor = found + 1;
  }
  return true;
}

export function providerConfigDigest(bytes) {
  if (
    !(typeof bytes === "string") &&
    !Buffer.isBuffer(bytes) &&
    !(bytes instanceof Uint8Array)
  ) {
    fail("provider config digest input must be exact bytes");
  }
  return createHash("sha256").update(bytes).digest("hex");
}

export function assertProviderDigest(actual, expected) {
  validateDigest(actual);
  validateDigest(expected, "expected provider config digest");
  if (actual !== expected) {
    fail("provider config digest changed");
  }
}

function normalizedRelativePath(relativePath) {
  return relativePath.split(path.sep).join("/");
}

async function snapshotTree(root) {
  const rootStat = await lstat(root);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) {
    fail("workspace root must be a real directory");
  }
  const entries = new Map();
  const foldedPaths = new Map();

  async function walk(directory, parentRelative) {
    const names = (await readdir(directory)).sort((left, right) =>
      left < right ? -1 : left > right ? 1 : 0,
    );
    for (const name of names) {
      const relative = parentRelative ? path.join(parentRelative, name) : name;
      const normalized = normalizedRelativePath(relative);
      const folded = normalized.toLocaleLowerCase("en-US");
      if (foldedPaths.has(folded)) {
        fail("workspace contains a case-only path collision");
      }
      foldedPaths.set(folded, normalized);

      const absolute = path.join(directory, name);
      const stat = await lstat(absolute);
      const record = {
        type: stat.isDirectory()
          ? "directory"
          : stat.isFile()
            ? "file"
            : stat.isSymbolicLink()
              ? "symlink"
              : "other",
        mode: stat.mode & 0o777,
        nlink: stat.nlink,
      };
      if (record.type === "file") {
        if (record.nlink !== 1) {
          fail("workspace contains a hard-linked file");
        }
        record.sha256 = providerConfigDigest(await readFile(absolute));
      }
      entries.set(normalized, record);
      if (record.type === "directory") {
        await walk(absolute, relative);
      }
    }
  }

  await walk(root, "");
  return entries;
}

function validateChangedPath(value) {
  validateBoundedString(value, "changed workspace path");
  const normalized = value.replaceAll("\\", "/");
  if (
    normalized.startsWith("/") ||
    normalized.includes("../") ||
    normalized === ".." ||
    path.posix.normalize(normalized) !== normalized
  ) {
    fail("changed workspace path must be a normalized relative path");
  }
  return normalized;
}

export async function validateExactWorkspace({
  seedRoot,
  workspaceRoot,
  expectedResult,
  changedPath,
}) {
  const expectedChangedPath = validateChangedPath(changedPath);
  const [seed, workspace, expectedBytes] = await Promise.all([
    snapshotTree(seedRoot),
    snapshotTree(workspaceRoot),
    readFile(expectedResult),
  ]);
  const seedPaths = [...seed.keys()];
  const workspacePaths = [...workspace.keys()];
  const seedFolded = seedPaths
    .map((entry) => entry.toLocaleLowerCase("en-US"))
    .sort();
  const workspaceFolded = workspacePaths
    .map((entry) => entry.toLocaleLowerCase("en-US"))
    .sort();
  if (
    seedPaths.some((entry, index) => entry !== workspacePaths[index]) &&
    seedFolded.length === workspaceFolded.length &&
    seedFolded.every((entry, index) => entry === workspaceFolded[index])
  ) {
    fail("workspace contains a case-only path change");
  }
  if (
    seedPaths.length !== workspacePaths.length ||
    seedPaths.some((entry, index) => entry !== workspacePaths[index])
  ) {
    fail("workspace has extra, deleted, or renamed paths");
  }

  const expectedDigest = providerConfigDigest(expectedBytes);
  let changedFiles = 0;
  for (const relative of seedPaths) {
    const before = seed.get(relative);
    const after = workspace.get(relative);
    if (
      before.type === "symlink" ||
      after.type === "symlink" ||
      before.type === "other" ||
      after.type === "other" ||
      before.type !== after.type
    ) {
      fail("workspace contains a symlink or type change");
    }
    if (process.platform !== "win32" && before.mode !== after.mode) {
      fail("workspace contains a mode change");
    }
    if (before.type !== "file") {
      continue;
    }
    if (before.sha256 !== after.sha256) {
      changedFiles += 1;
      if (relative !== expectedChangedPath) {
        fail("workspace contains an unrelated byte change");
      }
    }
    if (
      relative === expectedChangedPath &&
      (after.sha256 !== expectedDigest || before.sha256 === expectedDigest)
    ) {
      fail("workspace result does not match the expected byte change");
    }
  }
  if (
    changedFiles !== 1 ||
    !workspace.has(expectedChangedPath) ||
    workspace.get(expectedChangedPath).type !== "file"
  ) {
    fail("workspace must contain exactly one expected byte change");
  }
  return { changedPath: expectedChangedPath, changedFiles };
}

export function buildSanitizedEvidence(input) {
  if (!isPlainObject(input)) {
    fail("sanitized evidence input must be an object");
  }
  const phase = validatePhase(input.phase);
  const providerConfigSha256 = validateDigest(input.providerConfigSha256);
  const booleanFields = [
    "modelsBefore",
    "readyBefore",
    "toolOrder",
    "exactWorkspace",
    "verification",
    "modelsAfter",
    "readyAfter",
  ];
  for (const field of booleanFields) {
    if (typeof input[field] !== "boolean") {
      fail("sanitized evidence checks must be boolean");
    }
  }
  return {
    schemaVersion: 1,
    phase,
    providerConfigSha256,
    modelsBefore: input.modelsBefore,
    readyBefore: input.readyBefore,
    toolOrder: input.toolOrder,
    exactWorkspace: input.exactWorkspace,
    verification: input.verification,
    modelsAfter: input.modelsAfter,
    readyAfter: input.readyAfter,
  };
}

function validateEvidenceDirectory(value) {
  validateBoundedString(value, "evidence directory");
  const portable = value.replaceAll("\\", "/");
  if (path.posix.isAbsolute(portable) || path.win32.isAbsolute(value)) {
    fail("evidence directory must be under target/pi-acceptance");
  }
  const normalized = path.posix.normalize(portable);
  if (
    normalized !== "target/pi-acceptance" &&
    !normalized.startsWith("target/pi-acceptance/")
  ) {
    fail("evidence directory must be under target/pi-acceptance");
  }
  return normalized;
}

export function parseArguments(argv) {
  if (!Array.isArray(argv) || argv.length > 12) {
    fail("CLI arguments are invalid");
  }
  const supported = new Map([
    ["--phase", "phase"],
    ["--base-url", "baseUrl"],
    ["--pi-bin", "piBin"],
    ["--expected-config-sha256", "expectedConfigSha256"],
    ["--evidence-dir", "evidenceDir"],
  ]);
  const parsed = {};
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const key = supported.get(flag);
    if (key === undefined) {
      fail("unknown Pi acceptance argument");
    }
    if (Object.hasOwn(parsed, key)) {
      fail("duplicate Pi acceptance argument");
    }
    const value = argv[index + 1];
    validateBoundedString(value, "Pi acceptance argument");
    parsed[key] = value;
  }
  if (parsed.phase === undefined || parsed.baseUrl === undefined) {
    fail("phase and base URL are required");
  }
  validatePhaseEndpoint(
    parsed.phase,
    parsed.baseUrl,
    parsed.expectedConfigSha256,
  );
  if (parsed.piBin !== undefined) {
    validateBoundedString(parsed.piBin, "Pi binary");
  }
  if (parsed.evidenceDir !== undefined) {
    parsed.evidenceDir = validateEvidenceDirectory(parsed.evidenceDir);
  }
  return parsed;
}

export async function runQualifiedPiAdapter() {
  throw new QualificationRequiredError();
}

async function main() {
  try {
    const options = parseArguments(process.argv.slice(2));
    await runQualifiedPiAdapter(options);
  } catch (error) {
    if (error instanceof QualificationRequiredError) {
      console.error(error.message);
    } else {
      console.error("Pi acceptance input validation failed.");
    }
    process.exitCode = 2;
  }
}

if (
  process.argv[1] !== undefined &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  await main();
}
