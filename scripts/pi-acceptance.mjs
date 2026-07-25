#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import {
  cp,
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";

const PHASES = new Set(["mac-local", "windows-tailnet", "post-recovery"]);
const DIGEST_PATTERN = /^[a-f0-9]{64}$/;
const TAILNET_HOSTNAME_PATTERN =
  /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+ts\.net$/;
const MAX_ARGUMENT_LENGTH = 4096;
const MAX_BASE_URL_LENGTH = 2048;
const MAX_TRACE_RECORDS = 10_000;
const MAX_PI_STDOUT_BYTES = 1024 * 1024;
const MAX_PI_STDERR_BYTES = 64 * 1024;
const MAX_PI_LINE_BYTES = 64 * 1024;
const MAX_GATEWAY_RESPONSE_BYTES = 64 * 1024;
const MAX_PROCESS_TIMEOUT_MS = 5 * 60 * 1000;
const GATEWAY_TIMEOUT_MS = 5000;
const FORCE_KILL_DELAY_MS = 250;
const TERMINAL_DEADLINE_MS = 250;
const KNOWN_LIFECYCLE_EVENTS = new Set([
  "agent_start",
  "turn_start",
  "message_start",
  "message_update",
  "message_end",
  "turn_end",
]);
const ALLOWED_TOOL_NAMES = new Set(["read", "bash", "write"]);
const repositoryRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

export class QualificationRequiredError extends Error {
  constructor() {
    super(
      "Safe output-token limit qualification required before live Pi execution.",
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

function isEnvironmentRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.prototype.toString.call(value) === "[object Object]"
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
  if (!isEnvironmentRecord(source)) {
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
    records.length !== 4
  ) {
    fail("semantic tool trace must contain exactly four records");
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
      record.tool === "write" &&
      record.status === "success",
    (record) =>
      record.tool === "bash" &&
      record.stage === "verification" &&
      record.status === "success",
  ];
  if (!required.every((matches, index) => matches(records[index]))) {
    fail("semantic tool trace is missing the exact successful tool loop");
  }
  return true;
}

function validateEventIdentifier(value, label) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 256 ||
    value.includes("\0")
  ) {
    fail(`Pi JSONL ${label} is invalid`);
  }
  return value;
}

function validatePrivateEventObject(record, field) {
  if (!Object.hasOwn(record, field) || !isPlainObject(record[field])) {
    fail("Pi JSONL event shape is invalid");
  }
}

class QualifiedPiEventAdapter {
  constructor() {
    this.pending = new Map();
    this.completed = new Set();
    this.semanticTrace = [];
    this.requiredStep = 0;
    this.sessionSeen = false;
    this.agentEndSeen = false;
    this.settledSeen = false;
    this.recordCount = 0;
  }

  acceptLine(line) {
    if (
      typeof line !== "string" ||
      line.length === 0 ||
      Buffer.byteLength(line) > MAX_PI_LINE_BYTES
    ) {
      fail("Pi process output limit exceeded");
    }
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      fail("invalid Pi JSONL record");
    }
    if (!isPlainObject(record) || typeof record.type !== "string") {
      fail("invalid Pi JSONL record");
    }
    const isFirstRecord = this.recordCount === 0;
    this.recordCount += 1;
    if (this.settledSeen) {
      if (record.type === "agent_settled") {
        fail("duplicate terminal Pi lifecycle record");
      }
      fail("Pi JSONL record appeared after agent_settled");
    }
    if (record.type === "session") {
      if (!isFirstRecord || this.sessionSeen || record.version !== 3) {
        fail("duplicate or invalid Pi session record");
      }
      this.sessionSeen = true;
      return;
    }
    if (this.agentEndSeen && record.type === "agent_end") {
      fail("duplicate terminal Pi lifecycle record");
    }
    if (this.agentEndSeen && record.type !== "agent_settled") {
      fail("Pi JSONL record appeared after agent_end");
    }
    if (KNOWN_LIFECYCLE_EVENTS.has(record.type)) {
      return;
    }
    if (record.type === "agent_end") {
      this.agentEndSeen = true;
      return;
    }
    if (record.type === "agent_settled") {
      if (this.pending.size !== 0) {
        fail("agent_settled arrived with a pending tool call");
      }
      this.settledSeen = true;
      return;
    }
    if (record.type === "tool_execution_start") {
      validatePrivateEventObject(record, "args");
      const toolCallId = validateEventIdentifier(
        record.toolCallId,
        "toolCallId",
      );
      const toolName = validateEventIdentifier(record.toolName, "toolName");
      if (
        !ALLOWED_TOOL_NAMES.has(toolName) ||
        this.pending.has(toolCallId) ||
        this.completed.has(toolCallId)
      ) {
        fail("Pi tool correlation is invalid");
      }
      this.pending.set(toolCallId, toolName);
      return;
    }
    if (record.type === "tool_execution_update") {
      validatePrivateEventObject(record, "args");
      validatePrivateEventObject(record, "partialResult");
      this.correlate(record);
      return;
    }
    if (record.type === "tool_execution_end") {
      validatePrivateEventObject(record, "result");
      const { toolCallId, toolName } = this.correlate(record);
      this.pending.delete(toolCallId);
      this.completed.add(toolCallId);
      if (record.isError !== false) {
        fail("Pi tool execution failed");
      }
      this.advanceSemanticTrace(toolName);
      return;
    }
    fail("unknown Pi JSONL record type");
  }

  correlate(record) {
    const toolCallId = validateEventIdentifier(
      record.toolCallId,
      "toolCallId",
    );
    const toolName = validateEventIdentifier(record.toolName, "toolName");
    if (
      !this.pending.has(toolCallId) ||
      this.pending.get(toolCallId) !== toolName
    ) {
      fail("Pi tool correlation is invalid");
    }
    return { toolCallId, toolName };
  }

  advanceSemanticTrace(toolName) {
    const expected = [
      (name) => name === "read",
      (name) => name === "bash",
      (name) => name === "write",
      (name) => name === "bash",
    ];
    if (!expected[this.requiredStep]?.(toolName)) {
      fail("Pi JSONL must contain exactly four successful tool completions");
    }
    const record = { tool: toolName, status: "success" };
    if (this.requiredStep === 1) {
      record.stage = "precheck";
    } else if (this.requiredStep === 3) {
      record.stage = "verification";
    }
    this.semanticTrace.push(record);
    this.requiredStep += 1;
  }

  finish() {
    if (!this.sessionSeen) {
      fail("Pi JSONL is missing version-3 session header");
    }
    if (!this.agentEndSeen) {
      fail("Pi JSONL is missing agent_end");
    }
    if (!this.settledSeen) {
      fail("Pi JSONL is missing agent_settled");
    }
    if (this.pending.size !== 0) {
      fail("Pi JSONL ended with a pending tool call");
    }
    validateSemanticToolTrace(this.semanticTrace);
    return this.semanticTrace.map((record) => ({ ...record }));
  }
}

export function adaptQualifiedPiJsonl(lines) {
  if (
    !Array.isArray(lines) ||
    lines.length === 0 ||
    lines.length > MAX_TRACE_RECORDS
  ) {
    fail("Pi JSONL line count is invalid");
  }
  const adapter = new QualifiedPiEventAdapter();
  for (const line of lines) {
    adapter.acceptLine(line);
  }
  return adapter.finish();
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
    ["--max-tokens", "maxTokens"],
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
  if (
    parsed.phase === undefined ||
    parsed.baseUrl === undefined ||
    parsed.piBin === undefined ||
    parsed.maxTokens === undefined
  ) {
    fail("phase, base URL, Pi binary, and max tokens are required");
  }
  validatePhaseEndpoint(
    parsed.phase,
    parsed.baseUrl,
    parsed.expectedConfigSha256,
  );
  validateBoundedString(parsed.piBin, "Pi binary");
  if (!/^[1-9][0-9]*$/.test(parsed.maxTokens)) {
    fail("max tokens must be an integer between 1 and 8191");
  }
  parsed.maxTokens = Number(parsed.maxTokens);
  if (!Number.isSafeInteger(parsed.maxTokens) || parsed.maxTokens >= 8192) {
    fail("max tokens must be an integer between 1 and 8191");
  }
  if (parsed.evidenceDir !== undefined) {
    parsed.evidenceDir = validateEvidenceDirectory(parsed.evidenceDir);
  }
  return parsed;
}

export function buildRuntimeModelsConfig(baseUrl, qualifiedMaxTokens) {
  return {
    providers: {
      loxa: {
        baseUrl,
        api: "openai-completions",
        apiKey: "loxa-dummy-key",
        models: [
          {
            id: "loxa",
            name: "Loxa",
            reasoning: false,
            input: ["text"],
            contextWindow: 8192,
            maxTokens: qualifiedMaxTokens,
            compat: { maxTokensField: "max_tokens" },
          },
        ],
      },
    },
  };
}

export function buildQualifiedPiArgv(extensionPath, prompt) {
  if (!path.isAbsolute(extensionPath)) {
    fail("trusted Pi extension path must be absolute");
  }
  return [
    "--provider",
    "loxa",
    "--model",
    "loxa",
    "--mode",
    "json",
    "--no-session",
    "--tools",
    "read,bash,write",
    "--no-extensions",
    "--extension",
    extensionPath,
    "--no-skills",
    "--no-prompt-templates",
    "--no-context-files",
    "--no-themes",
    "--no-approve",
    "--offline",
    prompt,
  ];
}

async function boundedGatewayJson(url, cancellation) {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  if (cancellation?.aborted) {
    fail("Pi acceptance was cancelled");
  }
  cancellation?.addEventListener("abort", cancel, { once: true });
  const timeout = setTimeout(() => controller.abort(), GATEWAY_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      method: "GET",
      redirect: "error",
      signal: controller.signal,
    });
    if (!response.ok) {
      fail("gateway acceptance endpoint was unavailable");
    }
    if (response.body === null) {
      fail("gateway acceptance response was invalid");
    }
    const chunks = [];
    let byteCount = 0;
    const reader = response.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      byteCount += value.byteLength;
      if (byteCount > MAX_GATEWAY_RESPONSE_BYTES) {
        try {
          await reader.cancel();
        } catch {
          // The fixed size-limit failure remains authoritative.
        }
        fail("gateway acceptance response exceeded its size limit");
      }
      chunks.push(Buffer.from(value));
    }
    const bytes = Buffer.concat(chunks, byteCount);
    try {
      return JSON.parse(bytes.toString("utf8"));
    } catch {
      fail("gateway acceptance response was invalid");
    }
  } catch (error) {
    if (cancellation?.aborted) {
      fail("Pi acceptance was cancelled");
    }
    if (
      error instanceof Error &&
      error.message.startsWith("gateway acceptance")
    ) {
      throw error;
    }
    fail("gateway acceptance request failed");
  } finally {
    clearTimeout(timeout);
    cancellation?.removeEventListener("abort", cancel);
  }
}

async function validateGatewayAcceptance(baseUrl, cancellation) {
  const parsed = validateBaseUrl(baseUrl);
  const origin = `${parsed.protocol}//${parsed.host}`;
  validateModelsResponse(
    await boundedGatewayJson(`${origin}/v1/models`, cancellation),
  );
  validateReadyStatus(
    await boundedGatewayJson(`${origin}/loxa/status`, cancellation),
  );
}

function isAbsoluteExecutable(value) {
  return typeof value === "string" && (path.isAbsolute(value) || path.win32.isAbsolute(value));
}

function readPiVersion(program) {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(program, ["--version"], {
        shell: false,
        stdio: ["ignore", "pipe", "ignore"],
        windowsHide: true,
      });
    } catch {
      reject(new Error("qualified Pi executable verification failed"));
      return;
    }
    if (!child?.stdout || typeof child.once !== "function") {
      reject(new Error("qualified Pi executable verification failed"));
      return;
    }
    let output = "";
    child.stdout.on("data", (chunk) => {
      output += Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk);
    });
    child.once("error", () =>
      reject(new Error("qualified Pi executable verification failed")),
    );
    child.once("close", (code) => {
      if (code !== 0) {
        reject(new Error("qualified Pi executable verification failed"));
        return;
      }
      resolve(output);
    });
  });
}

export async function qualifyPiExecutable(piBin, {
  resolveExecutable = realpath,
  readVersion = readPiVersion,
} = {}) {
  if (!isAbsoluteExecutable(piBin)) {
    fail("qualified Pi executable must be an absolute path");
  }
  let resolved;
  let version;
  try {
    resolved = await resolveExecutable(piBin);
    if (!isAbsoluteExecutable(resolved)) {
      throw new Error("non-absolute executable");
    }
    version = await readVersion(resolved);
  } catch {
    fail("qualified Pi executable verification failed");
  }
  if (version.trim() !== "0.82.1") {
    fail("qualified Pi executable version must be 0.82.1");
  }
  return resolved;
}

export function terminateOwnedProcessTree({
  child,
  platform,
  signal,
  signalProcess,
  spawnTreeKiller,
  taskkillExecutable,
}) {
  const signalExactChild = () => {
    try {
      child.kill(signal);
    } catch {
      // The fixed terminal deadline remains authoritative.
    }
  };
  const pid = child.pid;
  if (Number.isSafeInteger(pid) && pid > 0) {
    if (platform === "win32") {
      const argv = ["/PID", String(pid), "/T"];
      if (signal === "SIGKILL") {
        argv.push("/F");
      }
      try {
        if (!isAbsoluteExecutable(taskkillExecutable)) {
          throw new Error("taskkill path is invalid");
        }
        const killer = spawnTreeKiller(taskkillExecutable, argv, {
          shell: false,
          stdio: "ignore",
          windowsHide: true,
        });
        if (typeof killer?.once !== "function") {
          throw new Error("taskkill launcher is invalid");
        }
        killer?.unref?.();
        return new Promise((resolve, reject) => {
          let complete = false;
          const failCleanup = () => {
            if (!complete) {
              complete = true;
              reject(new Error("Pi process cleanup failed"));
            }
          };
          killer.once("error", failCleanup);
          killer.once("close", (code) => {
            if (complete) {
              return;
            }
            complete = true;
            if (code === 0) {
              resolve();
            } else {
              reject(new Error("Pi process cleanup failed"));
            }
          });
        });
      } catch {
        signalExactChild();
        return Promise.reject(new Error("Pi process cleanup failed"));
      }
    } else {
      try {
        signalProcess(-pid, signal);
        return Promise.resolve();
      } catch {
        // Fall back to the exact child if the owned group already disappeared.
      }
    }
  }
  signalExactChild();
  return Promise.resolve();
}

function runPiProcess({
  program,
  argv,
  cwd,
  environment,
  processTimeoutMs,
  cancellation,
  spawnProcess,
  platform,
  signalProcess,
  spawnTreeKiller,
  taskkillExecutable,
}) {
  if (cancellation?.aborted) {
    return Promise.reject(new Error("Pi acceptance was cancelled"));
  }
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawnProcess(program, argv, {
        cwd,
        detached: platform !== "win32",
        env: environment,
        shell: false,
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      });
    } catch {
      reject(new Error("Pi process failed to start"));
      return;
    }
    if (!child?.stdout || !child?.stderr || typeof child.kill !== "function") {
      reject(new Error("Pi process failed to start"));
      return;
    }

    const adapter = new QualifiedPiEventAdapter();
    const decoder = new StringDecoder("utf8");
    let stdoutBuffer = "";
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let lineCount = 0;
    let failure;
    let settled = false;
    let terminationRequested = false;
    let forceKillTimer;
    let terminalDeadlineTimer;
    let timeout;
    let cleanup = Promise.resolve();

    const stopWatching = () => {
      clearTimeout(timeout);
      clearTimeout(forceKillTimer);
      clearTimeout(terminalDeadlineTimer);
      cancellation?.removeEventListener("abort", cancel);
      child.stdout.off("data", onStdout);
      child.stderr.off("data", onStderr);
      child.off("error", onError);
      child.off("close", onClose);
    };

    const settleRejected = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      stopWatching();
      child.stdout.destroy();
      child.stderr.destroy();
      reject(error);
    };

    const settleResolved = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      stopWatching();
      child.stdout.destroy();
      child.stderr.destroy();
      resolve(value);
    };

    const requestTermination = () => {
      if (terminationRequested || settled) {
        return;
      }
      terminationRequested = true;
      const requestTreeTermination = (signal) => {
        cleanup = cleanup
          .then(() =>
            terminateOwnedProcessTree({
              child,
              platform,
              signal,
              signalProcess,
              spawnTreeKiller,
              taskkillExecutable,
            }),
          )
          .catch(() => {
            failure = new Error("Pi process cleanup failed");
          });
      };
      requestTreeTermination("SIGTERM");
      forceKillTimer = setTimeout(() => {
        if (settled) {
          return;
        }
        requestTreeTermination("SIGKILL");
        terminalDeadlineTimer = setTimeout(() => {
          void cleanup.then(() => {
            if (!settled) {
              settleRejected(
                failure ?? new Error("Pi process did not terminate"),
              );
            }
          });
        }, TERMINAL_DEADLINE_MS);
      }, FORCE_KILL_DELAY_MS);
    };

    const recordFailure = (message) => {
      if (failure === undefined) {
        failure = new Error(message);
      }
      requestTermination();
    };

    const acceptLine = (line) => {
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      lineCount += 1;
      if (lineCount > MAX_TRACE_RECORDS) {
        fail("Pi process output limit exceeded");
      }
      adapter.acceptLine(line);
    };

    const consumeText = (text) => {
      stdoutBuffer += text;
      while (true) {
        const boundary = stdoutBuffer.indexOf("\n");
        if (boundary === -1) {
          break;
        }
        const line = stdoutBuffer.slice(0, boundary);
        stdoutBuffer = stdoutBuffer.slice(boundary + 1);
        acceptLine(line);
      }
      if (Buffer.byteLength(stdoutBuffer) > MAX_PI_LINE_BYTES) {
        fail("Pi process output limit exceeded");
      }
    };

    const onStdout = (chunk) => {
      if (failure !== undefined) {
        return;
      }
      try {
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        stdoutBytes += bytes.length;
        if (stdoutBytes > MAX_PI_STDOUT_BYTES) {
          fail("Pi process output limit exceeded");
        }
        consumeText(decoder.write(bytes));
      } catch {
        recordFailure("Pi process output limit or JSONL validation failed");
      }
    };
    const onStderr = (chunk) => {
      if (failure !== undefined) {
        return;
      }
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      stderrBytes += bytes.length;
      if (stderrBytes > MAX_PI_STDERR_BYTES) {
        recordFailure("Pi process output limit exceeded");
      }
    };
    const onError = () => {
      recordFailure("Pi process failed to start");
    };

    const cancel = () => {
      recordFailure("Pi acceptance was cancelled");
    };
    const onClose = (code) => {
      if (failure !== undefined) {
        return;
      }
      try {
        consumeText(decoder.end());
        if (stdoutBuffer.length > 0) {
          acceptLine(stdoutBuffer);
          stdoutBuffer = "";
        }
        if (code !== 0) {
          recordFailure("Pi process exited unsuccessfully");
          return;
        }
        settleResolved(adapter.finish());
      } catch (error) {
        recordFailure(
          error instanceof Error &&
          error.message.startsWith("Pi process output limit")
            ? error.message
            : error instanceof Error &&
                (error.message.startsWith("Pi JSONL") ||
                  error.message.startsWith("invalid Pi JSONL") ||
                  error.message.startsWith("unknown Pi JSONL") ||
                  error.message.startsWith("duplicate terminal") ||
                  error.message.startsWith("agent_settled"))
              ? error.message
              : "Pi process output was invalid",
        );
      }
    };

    child.stdout.on("data", onStdout);
    child.stderr.on("data", onStderr);
    child.once("error", onError);
    child.once("close", onClose);
    timeout = setTimeout(() => {
      recordFailure("Pi process timed out");
    }, processTimeoutMs);
    cancellation?.addEventListener("abort", cancel, { once: true });
    if (cancellation?.aborted) {
      cancel();
    }
  });
}

export async function runQualifiedPiAdapter(
  options = {},
  testSeam = {},
) {
  if (!isPlainObject(options) || !isPlainObject(testSeam)) {
    fail("qualified Pi adapter input is invalid");
  }
  const maxTokens = options.maxTokens ?? options.qualifiedMaxTokens;
  if (maxTokens === undefined) {
    throw new QualificationRequiredError();
  }
  if (
    !Number.isSafeInteger(maxTokens) ||
    maxTokens <= 0 ||
    maxTokens >= 8192
  ) {
    fail("qualified output-token limit must be between 1 and 8191");
  }
  const parsedEndpoint = validatePhaseEndpoint(
    options.phase,
    options.baseUrl,
    options.expectedConfigSha256,
  );
  const processTimeoutMs = options.processTimeoutMs ?? 120_000;
  if (
    !Number.isSafeInteger(processTimeoutMs) ||
    processTimeoutMs < 10 ||
    processTimeoutMs > MAX_PROCESS_TIMEOUT_MS
  ) {
    fail("Pi process timeout is invalid");
  }
  const piBin = options.piBin;
  validateBoundedString(piBin, "Pi binary");
  if (
    options.signal !== undefined &&
    (typeof options.signal !== "object" ||
      typeof options.signal.addEventListener !== "function" ||
      typeof options.signal.removeEventListener !== "function")
  ) {
    fail("Pi cancellation signal is invalid");
  }
  if (options.signal?.aborted) {
    fail("Pi acceptance was cancelled");
  }

  const platform = testSeam.platform ?? process.platform;
  const processPlatform = testSeam.processPlatform ?? platform;
  if (processPlatform !== "darwin" && processPlatform !== "win32") {
    fail("Pi process platform is invalid");
  }
  const sourceEnvironment = testSeam.sourceEnvironment ?? process.env;
  const spawnProcess = testSeam.spawnProcess ?? spawn;
  const signalProcess = testSeam.signalProcess ?? process.kill;
  const spawnTreeKiller = testSeam.spawnTreeKiller ?? spawn;
  const resolveExecutable =
    testSeam.resolveExecutable ??
    (testSeam.spawnProcess === undefined ? realpath : async (value) => value);
  const readQualifiedPiVersion =
    testSeam.readPiVersion ??
    (testSeam.spawnProcess === undefined
      ? readPiVersion
      : async () => "0.82.1\n");
  if (
    typeof spawnProcess !== "function" ||
    typeof signalProcess !== "function" ||
    typeof spawnTreeKiller !== "function"
  ) {
    fail("Pi process launcher is invalid");
  }
  const qualifiedPiBin = await qualifyPiExecutable(piBin, {
    resolveExecutable,
    readVersion: readQualifiedPiVersion,
  });

  const temporaryRoot = await mkdtemp(
    path.join(os.tmpdir(), "loxa-pi-acceptance-"),
  );
  const home = path.join(temporaryRoot, "home");
  const configDirectory = path.join(temporaryRoot, "pi-config");
  const workspace = path.join(temporaryRoot, "workspace");
  const childTemp = path.join(temporaryRoot, "tmp");
  try {
    await Promise.all([
      mkdir(home, { recursive: true }),
      mkdir(configDirectory, { recursive: true }),
      mkdir(childTemp, { recursive: true }),
      cp(
        path.join(repositoryRoot, "examples/pi/tool-loop/seed"),
        workspace,
        { recursive: true },
      ),
    ]);
    const prompt = await readFile(
      path.join(repositoryRoot, "examples/pi/tool-loop/prompt.txt"),
      "utf8",
    );
    validateBoundedString(prompt, "Pi acceptance prompt", 32 * 1024);
    const configBytes = Buffer.from(
      `${JSON.stringify(
        buildRuntimeModelsConfig(
          parsedEndpoint.toString(),
          maxTokens,
        ),
        null,
        2,
      )}\n`,
    );
    const providerConfigSha256 = providerConfigDigest(configBytes);
    if (options.expectedConfigSha256 !== undefined) {
      assertProviderDigest(
        providerConfigSha256,
        options.expectedConfigSha256,
      );
    }
    await writeFile(
      path.join(configDirectory, "models.json"),
      configBytes,
      { mode: 0o600 },
    );
    const environment = {
      ...buildIsolatedChildEnvironment(platform, {
        home,
        temp: childTemp,
        source: sourceEnvironment,
      }),
      PI_CODING_AGENT_DIR: configDirectory,
      PI_OFFLINE: "1",
      PI_TELEMETRY: "0",
      PI_SKIP_VERSION_CHECK: "1",
    };
    const argv = buildQualifiedPiArgv(
      path.join(repositoryRoot, "examples/pi/tool-loop/acceptance-gate.mjs"),
      prompt,
    );

    await validateGatewayAcceptance(parsedEndpoint.toString(), options.signal);
    const semanticTrace = await runPiProcess({
      program: qualifiedPiBin,
      argv,
      cwd: workspace,
      environment,
      processTimeoutMs,
      cancellation: options.signal,
      spawnProcess,
      platform: processPlatform,
      signalProcess,
      spawnTreeKiller,
      taskkillExecutable:
        testSeam.taskkillExecutable ??
        path.win32.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "taskkill.exe"),
    });
    await validateExactWorkspace({
      seedRoot: path.join(repositoryRoot, "examples/pi/tool-loop/seed"),
      workspaceRoot: workspace,
      expectedResult: path.join(
        repositoryRoot,
        "examples/pi/tool-loop/expected/result.txt",
      ),
      changedPath: "result.txt",
    });
    await validateGatewayAcceptance(parsedEndpoint.toString(), options.signal);
    return {
      providerConfigSha256,
      semanticTrace,
    };
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
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
