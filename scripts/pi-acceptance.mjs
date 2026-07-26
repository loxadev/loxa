#!/usr/bin/env node

import { spawn } from "node:child_process";
import { realpath } from "node:fs/promises";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";

const MAX_ARGUMENT_LENGTH = 4096;
const MAX_TRACE_RECORDS = 10_000;
const MAX_PI_STDOUT_BYTES = 1024 * 1024;
const MAX_PI_STDERR_BYTES = 64 * 1024;
const MAX_PI_LINE_BYTES = 64 * 1024;
const MAX_PI_VERSION_STDOUT_BYTES = 4096;
const MAX_PROCESS_TIMEOUT_MS = 5 * 60 * 1000;
const PI_VERSION_TIMEOUT_MS = 5000;
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
        this.pending.size !== 0 ||
        this.pending.has(toolCallId) ||
        this.completed.has(toolCallId)
      ) {
        fail("Pi tool correlation is invalid or concurrent tool execution started");
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

export function buildQualifiedPiArgv(extensionPath, prompt) {
  if (!path.isAbsolute(extensionPath) && !path.win32.isAbsolute(extensionPath)) {
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
    "--append-system-prompt",
    prompt,
    prompt,
  ];
}

function isAbsoluteExecutable(value) {
  return typeof value === "string" && (path.isAbsolute(value) || path.win32.isAbsolute(value));
}

export function readPiVersion(
  program,
  argv,
  {
    spawnProcess = spawn,
    cancellation,
    timeoutMs = PI_VERSION_TIMEOUT_MS,
    forceKillDelayMs = FORCE_KILL_DELAY_MS,
    terminalTimeoutMs = TERMINAL_DEADLINE_MS,
  } = {},
) {
  return new Promise((resolve, reject) => {
    let child;
    let settled = false;
    let terminationRequested = false;
    let output = "";
    let outputBytes = 0;
    let timeout;
    let forceKillTimer;
    let terminalTimer;
    const cleanup = () => {
      clearTimeout(timeout);
      clearTimeout(forceKillTimer);
      clearTimeout(terminalTimer);
      child?.stdout?.off("data", onStdout);
      child?.off("error", onError);
      child?.off("close", onClose);
      cancellation?.removeEventListener("abort", rejectAfterTermination);
      child?.stdout?.destroy();
    };
    const failProbe = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(new Error("qualified Pi executable verification failed"));
    };
    const succeed = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve(output);
    };
    const terminate = () => {
      if (terminationRequested || settled) {
        return;
      }
      terminationRequested = true;
      try {
        child?.kill?.("SIGTERM");
      } catch {
        // The terminal deadline below is authoritative.
      }
      forceKillTimer = setTimeout(() => {
        try {
          child?.kill?.("SIGKILL");
        } catch {
          // The terminal deadline below is authoritative.
        }
        terminalTimer = setTimeout(failProbe, terminalTimeoutMs);
      }, forceKillDelayMs);
    };
    const rejectAfterTermination = () => {
      terminate();
    };
    const onStdout = (chunk) => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      outputBytes += bytes.length;
      if (outputBytes > MAX_PI_VERSION_STDOUT_BYTES) {
        rejectAfterTermination();
        return;
      }
      output += bytes.toString("utf8");
    };
    const onError = () => rejectAfterTermination();
    const onClose = (code) => {
      if (terminationRequested || code !== 0) {
        failProbe();
        return;
      }
      succeed();
    };
    try {
      child = spawnProcess(program, argv, {
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
    child.stdout.on("data", onStdout);
    child.once("error", onError);
    child.once("close", onClose);
    cancellation?.addEventListener("abort", rejectAfterTermination, {
      once: true,
    });
    if (cancellation?.aborted) {
      rejectAfterTermination();
    }
    timeout = setTimeout(rejectAfterTermination, timeoutMs);
  });
}

export async function qualifyPiEntrypoint(piEntrypoint, {
  nodeExecutable = process.execPath,
  resolveEntrypoint = realpath,
  readVersion = readPiVersion,
  cancellation,
} = {}) {
  if (
    !isAbsoluteExecutable(piEntrypoint) ||
    !isAbsoluteExecutable(nodeExecutable)
  ) {
    fail("qualified Pi CLI entrypoint must use absolute paths");
  }
  let resolved;
  let version;
  try {
    if (cancellation?.aborted) {
      throw new Error("Pi acceptance was cancelled");
    }
    resolved = await resolveEntrypoint(piEntrypoint);
    if (cancellation?.aborted) {
      throw new Error("Pi acceptance was cancelled");
    }
    if (
      !isAbsoluteExecutable(resolved) ||
      path.win32.basename(resolved).toLowerCase() !== "cli.js" ||
      path.win32.basename(path.win32.dirname(resolved)).toLowerCase() !== "dist"
    ) {
      throw new Error("invalid Pi CLI entrypoint");
    }
    version = await readVersion(
      nodeExecutable,
      [resolved, "--version"],
      { cancellation },
    );
  } catch {
    fail("qualified Pi CLI entrypoint verification failed");
  }
  if (version.trim() !== "0.82.1") {
    fail("qualified Pi CLI entrypoint version must be 0.82.1");
  }
  return { program: nodeExecutable, prefixArgs: [resolved] };
}

export function terminateOwnedProcessTree({
  child,
  platform,
  signal,
  spawnTreeKiller,
  taskkillExecutable,
  terminalTimeoutMs = TERMINAL_DEADLINE_MS,
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
          const terminalTimer = setTimeout(() => {
            try {
              killer.kill?.("SIGKILL");
            } catch {
              // The terminal cleanup failure remains authoritative.
            }
            failCleanup();
          }, terminalTimeoutMs);
          const finish = (callback) => {
            clearTimeout(terminalTimer);
            callback();
          };
          const failCleanup = () => {
            if (!complete) {
              complete = true;
              signalExactChild();
              reject(new Error("Pi process cleanup failed"));
            }
          };
          killer.once("error", () => finish(failCleanup));
          killer.once("close", (code) => {
            if (complete) {
              return;
            }
            finish(() => {
              if (code === 0) {
                complete = true;
                resolve();
              } else {
                failCleanup();
              }
            });
          });
        });
      } catch {
        signalExactChild();
        return Promise.reject(new Error("Pi process cleanup failed"));
      }
    } else {
      signalExactChild();
      return Promise.resolve();
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
  spawnTreeKiller,
  taskkillExecutable,
  taskkillTerminalTimeoutMs,
}) {
  if (cancellation?.aborted) {
    return Promise.reject(new Error("Pi acceptance was cancelled"));
  }
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawnProcess(program, argv, {
        cwd,
        detached: false,
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
              spawnTreeKiller,
              taskkillExecutable,
              terminalTimeoutMs: taskkillTerminalTimeoutMs,
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

export function parseBridgeArguments(argv) {
  if (!Array.isArray(argv) || argv.length !== 8) {
    fail("Pi bridge arguments are invalid");
  }
  const supported = new Map([
    ["--pi-entrypoint", "piEntrypoint"],
    ["--extension", "extension"],
    ["--prompt", "prompt"],
    ["--timeout-ms", "processTimeoutMs"],
  ]);
  const parsed = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = supported.get(argv[index]);
    if (key === undefined || Object.hasOwn(parsed, key)) {
      fail("Pi bridge arguments are invalid");
    }
    parsed[key] = validateBoundedString(
      argv[index + 1],
      "Pi bridge argument",
      key === "prompt" ? 32 * 1024 : MAX_ARGUMENT_LENGTH,
    );
  }
  if (!/^[1-9][0-9]*$/.test(parsed.processTimeoutMs)) {
    fail("Pi process timeout is invalid");
  }
  parsed.processTimeoutMs = Number(parsed.processTimeoutMs);
  if (
    !Number.isSafeInteger(parsed.processTimeoutMs) ||
    parsed.processTimeoutMs < 10 ||
    parsed.processTimeoutMs > MAX_PROCESS_TIMEOUT_MS
  ) {
    fail("Pi process timeout is invalid");
  }
  if (!isAbsoluteExecutable(parsed.piEntrypoint)) {
    fail("Pi CLI entrypoint must be absolute");
  }
  if (!path.isAbsolute(parsed.extension)) {
    fail("trusted Pi extension path must be absolute");
  }
  return parsed;
}

export function validateIsolatedEnvironment(
  environment,
  cwd,
  platform = process.platform,
) {
  const platformPath = platform === "win32" ? path.win32 : path.posix;
  if (!isEnvironmentRecord(environment) || !platformPath.isAbsolute(cwd)) {
    fail("Pi bridge environment is invalid");
  }
  const allowed =
    platform === "win32"
      ? new Set([
          "APPDATA",
          "COMSPEC",
          "HOME",
          "HOMEDRIVE",
          "HOMEPATH",
          "LANG",
          "LC_ALL",
          "LC_CTYPE",
          "LOCALAPPDATA",
          "PATH",
          "PATHEXT",
          "PI_CODING_AGENT_DIR",
          "PI_OFFLINE",
          "PI_SKIP_VERSION_CHECK",
          "PI_TELEMETRY",
          "SYSTEMROOT",
          "TEMP",
          "TMP",
          "USERPROFILE",
          "WINDIR",
          "XDG_CACHE_HOME",
          "XDG_CONFIG_HOME",
          "XDG_DATA_HOME",
        ])
      : new Set([
          "__CF_USER_TEXT_ENCODING",
          "HOME",
          "LANG",
          "LC_ALL",
          "LC_CTYPE",
          "PATH",
          "PI_CODING_AGENT_DIR",
          "PI_OFFLINE",
          "PI_SKIP_VERSION_CHECK",
          "PI_TELEMETRY",
          "TMPDIR",
          "XDG_CACHE_HOME",
          "XDG_CONFIG_HOME",
          "XDG_DATA_HOME",
        ]);
  if (
    !["darwin", "win32"].includes(platform) ||
    Object.keys(environment).some((key) => !allowed.has(key))
  ) {
    fail("Pi bridge environment contains an unapproved key");
  }
  if (
    Object.values(environment).some(
      (value) =>
        typeof value !== "string" ||
        value.length > MAX_ARGUMENT_LENGTH ||
        value.includes("\0"),
    )
  ) {
    fail("Pi bridge environment contains an invalid value");
  }
  const requiredPaths = [
    ["HOME", environment.HOME],
    ["XDG_CONFIG_HOME", environment.XDG_CONFIG_HOME],
    ["XDG_CACHE_HOME", environment.XDG_CACHE_HOME],
    ["XDG_DATA_HOME", environment.XDG_DATA_HOME],
    ["PI_CODING_AGENT_DIR", environment.PI_CODING_AGENT_DIR],
  ];
  if (platform === "win32") {
    requiredPaths.push(
      ["USERPROFILE", environment.USERPROFILE],
      ["APPDATA", environment.APPDATA],
      ["LOCALAPPDATA", environment.LOCALAPPDATA],
    );
  }
  for (const [key, value] of requiredPaths) {
    validateBoundedString(value, key);
    if (!platformPath.isAbsolute(value)) {
      fail("Pi bridge environment must use absolute isolated paths");
    }
  }
  const temp =
    platform === "win32" ? environment.TEMP : environment.TMPDIR;
  validateBoundedString(temp, "isolated temp");
  if (!platformPath.isAbsolute(temp)) {
    fail("Pi bridge environment must use absolute isolated paths");
  }
  if (
    environment.PI_OFFLINE !== "1" ||
    environment.PI_TELEMETRY !== "0" ||
    environment.PI_SKIP_VERSION_CHECK !== "1"
  ) {
    fail("Pi bridge environment is not safely isolated");
  }
  const root = platformPath.dirname(cwd);
  for (const value of [
    environment.HOME,
    environment.PI_CODING_AGENT_DIR,
    temp,
  ]) {
    if (platformPath.dirname(value) !== root) {
      fail("Pi bridge environment is not safely isolated");
    }
  }
  if (
    platform === "win32" &&
    (environment.USERPROFILE !== environment.HOME ||
      platformPath.dirname(environment.APPDATA) !==
        platformPath.join(environment.HOME, "AppData") ||
      platformPath.dirname(environment.LOCALAPPDATA) !==
        platformPath.join(environment.HOME, "AppData"))
  ) {
    fail("Pi bridge environment is not safely isolated");
  }
  return true;
}

export async function runQualifiedPiBridge(
  options = {},
  testSeam = {},
) {
  if (!isPlainObject(options) || !isPlainObject(testSeam)) {
    fail("qualified Pi bridge input is invalid");
  }
  const processTimeoutMs = options.processTimeoutMs;
  if (
    !Number.isSafeInteger(processTimeoutMs) ||
    processTimeoutMs < 10 ||
    processTimeoutMs > MAX_PROCESS_TIMEOUT_MS
  ) {
    fail("Pi process timeout is invalid");
  }
  const piEntrypoint = options.piEntrypoint;
  validateBoundedString(piEntrypoint, "Pi CLI entrypoint");
  validateBoundedString(options.extension, "trusted Pi extension");
  validateBoundedString(options.prompt, "Pi acceptance prompt", 32 * 1024);
  const cwd = options.cwd ?? process.cwd();
  validateIsolatedEnvironment(
    options.environment ?? process.env,
    cwd,
    testSeam.platform ?? process.platform,
  );
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

  const processPlatform = testSeam.platform ?? process.platform;
  if (processPlatform !== "darwin" && processPlatform !== "win32") {
    fail("Pi process platform is invalid");
  }
  const spawnProcess = testSeam.spawnProcess ?? spawn;
  const spawnTreeKiller = testSeam.spawnTreeKiller ?? spawn;
  const resolveEntrypoint =
    testSeam.resolveEntrypoint ??
    (testSeam.spawnProcess === undefined ? realpath : async (value) => value);
  const readQualifiedPiVersion =
    testSeam.readPiVersion ??
    (testSeam.spawnProcess === undefined
      ? readPiVersion
      : async () => "0.82.1\n");
  if (
    typeof spawnProcess !== "function" ||
    typeof spawnTreeKiller !== "function"
  ) {
    fail("Pi process launcher is invalid");
  }
  const qualifiedPi = await qualifyPiEntrypoint(piEntrypoint, {
    nodeExecutable: testSeam.nodeExecutable ?? process.execPath,
    resolveEntrypoint,
    readVersion: readQualifiedPiVersion,
    cancellation: options.signal,
  });
  const toolTrace = await runPiProcess({
    program: qualifiedPi.program,
    argv: [
      ...qualifiedPi.prefixArgs,
      ...buildQualifiedPiArgv(options.extension, options.prompt),
    ],
    cwd,
    environment: options.environment ?? process.env,
    processTimeoutMs,
    cancellation: options.signal,
    spawnProcess,
    platform: processPlatform,
    spawnTreeKiller,
    taskkillExecutable:
      testSeam.taskkillExecutable ??
      path.win32.join(
        process.env.SystemRoot ?? "C:\\Windows",
        "System32",
        "taskkill.exe",
      ),
    taskkillTerminalTimeoutMs: testSeam.taskkillTerminalTimeoutMs,
  });
  return { schemaVersion: 1, toolTrace };
}

export function createBridgeCancellation(controlInput) {
  if (
    controlInput === null ||
    typeof controlInput !== "object" ||
    typeof controlInput.once !== "function" ||
    typeof controlInput.off !== "function" ||
    typeof controlInput.resume !== "function"
  ) {
    fail("Pi bridge control channel is invalid");
  }
  const controller = new AbortController();
  const cancel = () => controller.abort();
  controlInput.once("data", cancel);
  controlInput.once("end", cancel);
  controlInput.once("error", cancel);
  controlInput.resume();
  return {
    signal: controller.signal,
    dispose() {
      controlInput.off("data", cancel);
      controlInput.off("end", cancel);
      controlInput.off("error", cancel);
      controlInput.pause?.();
    },
  };
}

async function main() {
  const cancellation = createBridgeCancellation(process.stdin);
  try {
    const options = parseBridgeArguments(process.argv.slice(2));
    const result = await runQualifiedPiBridge({
      ...options,
      signal: cancellation.signal,
    });
    const serialized = JSON.stringify(result);
    if (Buffer.byteLength(serialized) > MAX_PI_LINE_BYTES) {
      fail("Pi bridge result exceeded its size limit");
    }
    process.stdout.write(`${serialized}\n`);
  } catch {
    console.error("Pi bridge failed.");
    process.exitCode = 2;
  } finally {
    cancellation.dispose();
  }
}

if (
  process.argv[1] !== undefined &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  await main();
}
