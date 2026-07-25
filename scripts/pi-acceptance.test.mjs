import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import {
  access,
  chmod,
  cp,
  link,
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";

import {
  QualificationRequiredError,
  adaptQualifiedPiJsonl,
  assertProviderDigest,
  buildIsolatedChildEnvironment,
  buildSanitizedEvidence,
  parseArguments,
  providerConfigDigest,
  runQualifiedPiAdapter,
  validateBaseUrl,
  validateExactWorkspace,
  validateModelsConfig,
  validateModelsResponse,
  validatePhaseEndpoint,
  validateReadyStatus,
  validateSemanticToolTrace,
} from "./pi-acceptance.mjs";

const repositoryRoot = path.resolve(import.meta.dirname, "..");

async function withTempDirectory(name, run) {
  const directory = await mkdtemp(path.join(os.tmpdir(), `loxa-${name}-`));
  try {
    return await run(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

function runCli(argv) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["scripts/pi-acceptance.mjs", ...argv], {
      cwd: repositoryRoot,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => resolve({ code, stdout, stderr }));
  });
}

function successfulTrace(thirdTool = "write") {
  return [
    { tool: "read", status: "success" },
    { tool: "bash", stage: "precheck", status: "success" },
    { tool: thirdTool, status: "success" },
    { tool: "bash", stage: "verification", status: "success" },
  ];
}

function successfulQualifiedLines(thirdTool = "write") {
  const calls = [
    ["read-1", "read", { path: "source.txt" }],
    ["bash-1", "bash", { command: "node verify.mjs --precheck" }],
    ["write-1", thirdTool, { path: "result.txt", content: "sum=18\n" }],
    ["bash-2", "bash", { command: "node verify.mjs" }],
  ];
  return [
    JSON.stringify({ type: "session", version: 3 }),
    JSON.stringify({ type: "agent_start" }),
    ...calls.flatMap(([toolCallId, toolName, args]) => [
      JSON.stringify({
        type: "tool_execution_start",
        toolCallId,
        toolName,
        args,
      }),
      JSON.stringify({
        type: "tool_execution_end",
        toolCallId,
        toolName,
        result: { content: `private ${toolName} result` },
        isError: false,
      }),
    ]),
    JSON.stringify({ type: "agent_end" }),
    JSON.stringify({ type: "agent_settled" }),
  ];
}

async function withFakeGateway(run, responses = {}) {
  const requests = [];
  const server = createServer((request, response) => {
    requests.push(request.url);
    response.setHeader("content-type", "application/json");
    if (request.url === "/v1/models") {
      response.end(
        responses.models ??
          JSON.stringify({
            object: "list",
            data: [{ id: "loxa", object: "model", owned_by: "loxa" }],
          }),
      );
      return;
    }
    if (request.url === "/loxa/status") {
      response.end(
        responses.status ??
          JSON.stringify({
            health: "ready",
            model: "loxa",
            engine: {
              name: "llama-cpp",
              version: "version: 10107 (c0bc8591e)\nbuild: test",
            },
          }),
      );
      return;
    }
    response.statusCode = 404;
    response.end("{}");
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  try {
    return await run({
      baseUrl: `http://127.0.0.1:${address.port}/v1`,
      requests,
    });
  } finally {
    await new Promise((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}

function fakeSpawn(scenario, capture = {}) {
  return (program, argv, options) => {
    capture.program = program;
    capture.argv = [...argv];
    capture.options = {
      cwd: options.cwd,
      detached: options.detached,
      shell: options.shell,
      stdio: options.stdio,
      windowsHide: options.windowsHide,
      env: { ...options.env },
    };
    capture.kills = [];
    const child = new EventEmitter();
    child.stdout = new PassThrough();
    child.stderr = new PassThrough();
    if (scenario.pid !== undefined) {
      child.pid = scenario.pid;
    }
    let closed = false;
    const close = (code, signal = null) => {
      if (closed) {
        return;
      }
      closed = true;
      child.stdout.end();
      child.stderr.end();
      child.emit("close", code, signal);
    };
    capture.close = close;
    capture.child = child;
    child.kill = (signal = "SIGTERM") => {
      capture.kills.push(signal);
      if (scenario.ignoreAllKills) {
        return true;
      }
      if (scenario.descendantKeepsPipes) {
        queueMicrotask(() => child.emit("exit", null, signal));
        return true;
      }
      if (signal === "SIGTERM" && scenario.ignoreSigterm) {
        return true;
      }
      queueMicrotask(() => close(null, signal));
      return true;
    };

    queueMicrotask(async () => {
      if (scenario.onStart) {
        scenario.onStart();
      }
      if (scenario.hang || closed) {
        return;
      }
      try {
        capture.modelsConfig = JSON.parse(
          await readFile(
            path.join(options.env.PI_CODING_AGENT_DIR, "models.json"),
            "utf8",
          ),
        );
        if (scenario.writeExpectedResult) {
          await writeFile(path.join(options.cwd, "result.txt"), "sum=18\n");
        }
        if (scenario.stdoutBytes) {
          child.stdout.write(scenario.stdoutBytes);
        } else {
          for (const line of scenario.lines ?? successfulQualifiedLines()) {
            child.stdout.write(`${line}\n`);
          }
        }
        if (scenario.stderrBytes) {
          child.stderr.write(scenario.stderrBytes);
        }
        if (scenario.processError) {
          child.emit("error", new Error("private process error"));
          return;
        }
        close(scenario.exitCode ?? 0);
      } catch (error) {
        capture.fixtureError = error;
        close(97);
      }
    });
    return child;
  };
}

async function assertMissing(absolutePath) {
  await assert.rejects(access(absolutePath), { code: "ENOENT" });
}

async function observeSettlement(promise, timeoutMs) {
  let timer;
  try {
    return await Promise.race([
      promise.then(
        (value) => ({ status: "resolved", value }),
        (error) => ({ status: "rejected", error }),
      ),
      new Promise((resolve) => {
        timer = setTimeout(
          () => resolve({ status: "watchdog" }),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

test("pre-execution acceptance gate advances only after each exact successful tool result", async () => {
  const { createAcceptanceGate } = await import(
    "../examples/pi/tool-loop/acceptance-gate.mjs"
  );
  const gate = createAcceptanceGate();
  const calls = [
    { toolCallId: "read-1", toolName: "read", input: { path: "source.txt" } },
    { toolCallId: "bash-1", toolName: "bash", input: { command: "node verify.mjs --precheck" } },
    {
      toolCallId: "write-1",
      toolName: "write",
      input: { path: "result.txt", content: "sum=18\n" },
    },
    { toolCallId: "bash-2", toolName: "bash", input: { command: "node verify.mjs" } },
  ];

  for (const call of calls) {
    assert.equal(await gate(call), undefined);
    assert.deepEqual(await gate(calls.at(-1)), {
      block: true,
      reason: "Pi acceptance tool call is not the next exact step.",
    });
    assert.equal(
      await gate.toolResult({ ...call, isError: false }),
      undefined,
    );
  }
  assert.deepEqual(await gate(calls[3]), {
    block: true,
    reason: "Pi acceptance tool call is not the next exact step.",
  });
});

test("pre-execution acceptance gate blocks invalid sibling, retry, and result transitions at every step", async () => {
  const { createAcceptanceGate } = await import(
    "../examples/pi/tool-loop/acceptance-gate.mjs"
  );
  const valid = [
    { toolCallId: "read-1", toolName: "read", input: { path: "source.txt" } },
    { toolCallId: "bash-1", toolName: "bash", input: { command: "node verify.mjs --precheck" } },
    { toolCallId: "write-1", toolName: "write", input: { path: "result.txt", content: "sum=18\n" } },
    { toolCallId: "bash-2", toolName: "bash", input: { command: "node verify.mjs" } },
  ];
  const rejected = [
    { toolCallId: "bad-read", toolName: "read", input: { path: "/source.txt" } },
    { toolCallId: "bad-read", toolName: "read", input: { path: "nested/../source.txt" } },
    { toolCallId: "bad-bash", toolName: "bash", input: { command: "node verify.mjs" } },
    {
      toolCallId: "bad-write",
      toolName: "write",
      input: { path: "result.txt", content: "sum=18" },
    },
    { toolCallId: "bad-edit", toolName: "edit", input: { path: "result.txt" } },
  ];

  for (let step = 0; step < valid.length; step += 1) {
    const gate = createAcceptanceGate();
    for (const call of valid.slice(0, step)) {
      assert.equal(await gate(call), undefined);
      assert.equal(await gate.toolResult({ ...call, isError: false }), undefined);
    }
    for (const call of rejected) {
      if (
        call.toolName === valid[step].toolName &&
        JSON.stringify(call.input) === JSON.stringify(valid[step].input)
      ) {
        continue;
      }
      assert.deepEqual(await gate(call), {
        block: true,
        reason: "Pi acceptance tool call is not the next exact step.",
      });
    }
    assert.equal(await gate(valid[step]), undefined);
    assert.deepEqual(await gate({ ...valid[step], toolCallId: "sibling" }), {
      block: true,
      reason: "Pi acceptance tool call is not the next exact step.",
    });
    assert.deepEqual(
      await gate.toolResult({ ...valid[step], toolCallId: "wrong-result", isError: false }),
      { isError: true },
    );
    assert.deepEqual(await gate(valid[step]), {
      block: true,
      reason: "Pi acceptance tool call is not the next exact step.",
    });
  }

  const errorGate = createAcceptanceGate();
  assert.equal(await errorGate(valid[0]), undefined);
  assert.deepEqual(
    await errorGate.toolResult({ ...valid[0], isError: true }),
    { isError: true },
  );
  assert.deepEqual(await errorGate(valid[0]), {
    block: true,
    reason: "Pi acceptance tool call is not the next exact step.",
  });
});

test("pre-execution acceptance gate registers tool_call and tool_result handlers", async () => {
  const { default: acceptanceGate } = await import(
    "../examples/pi/tool-loop/acceptance-gate.mjs"
  );
  const handlers = new Map();
  acceptanceGate({ on: (event, handler) => handlers.set(event, handler) });
  assert.equal(typeof handlers.get("tool_call"), "function");
  assert.equal(typeof handlers.get("tool_result"), "function");
});

test("committed model examples use the fixed text-only provider contract", async () => {
  for (const relative of [
    "examples/pi/models.local.json",
    "examples/pi/models.tailnet.json",
  ]) {
    const bytes = await readFile(path.join(repositoryRoot, relative));
    const config = JSON.parse(bytes);
    const validated = validateModelsConfig(config);

    assert.deepEqual(Object.keys(config.providers), ["loxa"]);
    assert.equal(validated.providerName, "loxa");
    assert.equal(validated.api, "openai-completions");
    assert.equal(validated.apiKey, "loxa-dummy-key");
    assert.equal(validated.model.id, "loxa");
    assert.equal(validated.model.reasoning, false);
    assert.deepEqual(validated.model.input, ["text"]);
    assert.equal(validated.model.contextWindow, 8192);
    assert.equal("compat" in validated.provider, false);
    assert.equal("compat" in validated.model, false);
    assert.equal("maxTokens" in validated.model, false);
  }
});

test("tool-loop prompt requires the qualified four-call sequence and exact output", async () => {
  const prompt = await readFile(
    path.join(repositoryRoot, "examples/pi/tool-loop/prompt.txt"),
    "utf8",
  );

  assert.match(prompt, /exactly four tool calls and no others/i);
  assert.match(prompt, /read, bash, write, bash/i);
  assert.match(prompt, /^1\. Use read to read source\.txt\.$/m);
  assert.match(
    prompt,
    /^2\. Use bash to run `node verify\.mjs --precheck`\.$/m,
  );
  assert.match(
    prompt,
    /^3\. Use write to write result\.txt as exactly one `sum=<computed integer>` line followed by exactly one LF, with no spaces, Markdown, or extra line\.$/m,
  );
  assert.match(
    prompt,
    /^4\. Use bash to run `node verify\.mjs` as the final verification\.$/m,
  );
  assert.match(prompt, /never (?:use|run) bash before (?:the )?read/i);
  assert.match(prompt, /do not use edit or retry/i);
  assert.match(
    prompt,
    /exactly one `sum=<computed integer>` line followed by exactly one LF/i,
  );
  assert.match(prompt, /no spaces, Markdown, or extra line/i);
  assert.match(prompt, /stop after (?:the )?final verification/i);
});

test("model validation rejects unqualified compatibility and output-limit overrides", async () => {
  const config = JSON.parse(
    await readFile(path.join(repositoryRoot, "examples/pi/models.local.json")),
  );
  const withCompat = structuredClone(config);
  withCompat.providers.loxa.compat = { supportsDeveloperRole: false };
  assert.throws(() => validateModelsConfig(withCompat), /compat/i);

  const withMaxTokens = structuredClone(config);
  withMaxTokens.providers.loxa.models[0].maxTokens = 2048;
  assert.throws(() => validateModelsConfig(withMaxTokens), /maxTokens/i);
});

test("base URL accepts only credential-free http(s) endpoints at exact /v1", () => {
  assert.equal(validateBaseUrl("http://127.0.0.1:11435/v1").pathname, "/v1");
  assert.equal(validateBaseUrl("https://loxa-node.invalid/v1").pathname, "/v1");

  for (const invalid of [
    "http://127.0.0.1:11435",
    "http://127.0.0.1:11435/v1/",
    "ftp://127.0.0.1/v1",
    "http://user:secret@127.0.0.1/v1",
    "http://127.0.0.1/v1?token=secret",
    "http://127.0.0.1/v1#private",
  ]) {
    assert.throws(() => validateBaseUrl(invalid), /base URL/i);
  }
});

test("phase and endpoint relationship fails closed without pinning a tailnet hostname", () => {
  assert.equal(
    validatePhaseEndpoint("mac-local", "http://127.0.0.1:11435/v1").hostname,
    "127.0.0.1",
  );
  assert.equal(
    validatePhaseEndpoint(
      "windows-tailnet",
      "https://device.tailnet.ts.net/v1",
    ).hostname,
    "device.tailnet.ts.net",
  );
  assert.equal(
    validatePhaseEndpoint(
      "post-recovery",
      "http://127.0.0.1:11435/v1",
      "a".repeat(64),
    ).pathname,
    "/v1",
  );
  assert.equal(
    validatePhaseEndpoint(
      "post-recovery",
      "https://another-node.invalid/v1",
      "a".repeat(64),
    ).pathname,
    "/v1",
  );

  assert.throws(
    () => validatePhaseEndpoint("mac-local", "https://node.invalid/v1"),
    /loopback/i,
  );
  assert.throws(
    () =>
      validatePhaseEndpoint(
        "windows-tailnet",
        "http://127.0.0.1:11435/v1",
      ),
    /tailnet/i,
  );
  assert.throws(
    () => validatePhaseEndpoint("windows-tailnet", "http://node.invalid/v1"),
    /https/i,
  );
  assert.throws(
    () =>
      validatePhaseEndpoint(
        "windows-tailnet",
        "https://example.com/v1",
      ),
    /tailnet/i,
  );
  assert.throws(
    () =>
      validatePhaseEndpoint(
        "windows-tailnet",
        "https://node.invalid/v1",
      ),
    /tailnet/i,
  );
  assert.throws(
    () => validatePhaseEndpoint("windows-tailnet", "https://ts.net/v1"),
    /tailnet/i,
  );
  assert.throws(
    () =>
      validatePhaseEndpoint(
        "windows-tailnet",
        "https://[::1]/v1",
      ),
    /tailnet/i,
  );
  assert.throws(
    () =>
      validatePhaseEndpoint(
        "post-recovery",
        "http://127.0.0.1:11435/v1",
      ),
    /digest/i,
  );
});

test("Mac child environment is an allowlist with isolated home XDG and temp", () => {
  const environment = buildIsolatedChildEnvironment("darwin", {
    home: "/private/tmp/pi-home",
    temp: "/private/tmp/pi-temp",
    source: {
      PATH: "/usr/bin:/bin",
      LANG: "en_US.UTF-8",
      OPENAI_API_KEY: "must-not-leak",
      HOME: "/Users/real",
      XDG_CONFIG_HOME: "/Users/real/.config",
    },
  });

  assert.deepEqual(environment, {
    PATH: "/usr/bin:/bin",
    LANG: "en_US.UTF-8",
    HOME: "/private/tmp/pi-home",
    XDG_CONFIG_HOME: "/private/tmp/pi-home/.config",
    XDG_CACHE_HOME: "/private/tmp/pi-home/.cache",
    XDG_DATA_HOME: "/private/tmp/pi-home/.local/share",
    TMPDIR: "/private/tmp/pi-temp",
  });
});

test("Mac child environment accepts Node process.env through the same allowlist", () => {
  const environment = buildIsolatedChildEnvironment("darwin", {
    home: "/private/tmp/pi-live-home",
    temp: "/private/tmp/pi-live-temp",
    source: process.env,
  });
  const allowedKeys = new Set([
    "PATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
    "TMPDIR",
  ]);

  assert.equal(
    Object.keys(environment).every((key) => allowedKeys.has(key)),
    true,
  );
  assert.equal(environment.HOME, "/private/tmp/pi-live-home");
  assert.equal(environment.TMPDIR, "/private/tmp/pi-live-temp");
  assert.equal(environment.OPENAI_API_KEY, undefined);
});

test("child environment still rejects non-record object containers", () => {
  for (const source of [null, [], new Date(0), new Map()]) {
    assert.throws(
      () =>
        buildIsolatedChildEnvironment("darwin", {
          home: "/private/tmp/pi-home",
          temp: "/private/tmp/pi-temp",
          source,
        }),
      /child environment source/i,
    );
  }
});

test("gateway preflight and postflight validators pin the qualified llama-cpp build", () => {
  assert.doesNotThrow(() =>
    validateModelsResponse({
      object: "list",
      data: [{ id: "loxa", object: "model", owned_by: "loxa" }],
    }),
  );
  assert.doesNotThrow(() =>
    validateReadyStatus({
      health: "ready",
      model: "loxa",
      engine: {
        name: "llama-cpp",
        version: "version: 10107 (c0bc8591e)\nbuild: test",
      },
    }),
  );
  assert.throws(
    () => validateModelsResponse({ object: "list", data: [] }),
    /models response/i,
  );
  assert.throws(
    () => validateReadyStatus({ health: "unavailable", model: "loxa" }),
    /status response/i,
  );
  assert.throws(
    () => validateReadyStatus({ health: "ready", model: "other" }),
    /status response/i,
  );
  assert.throws(
    () => validateReadyStatus({ health: "ready", model: "loxa" }),
    /status response/i,
  );
  for (const engine of [
    { name: "llama.cpp", version: "version: 10107 (c0bc8591e)" },
    { name: "llama-cpp", version: "version: 10106 (c0bc8591e)" },
    { name: "llama-cpp", version: "launcher version: 10107 (c0bc8591e)" },
    { name: "llama-cpp", version: "version: 10107 (c0bc8591e) extra" },
    { name: "llama-cpp", version: "build: test\nversion: 10107 (c0bc8591e)" },
  ]) {
    assert.throws(
      () => validateReadyStatus({ health: "ready", model: "loxa", engine }),
      /status response/i,
    );
  }
});

test("Windows child environment isolates home profile AppData and temp", () => {
  const environment = buildIsolatedChildEnvironment("win32", {
    home: "R:\\pi-home",
    temp: "R:\\pi-temp",
    source: {
      Path: "C:\\Windows\\System32",
      PATHEXT: ".COM;.EXE",
      SYSTEMROOT: "C:\\Windows",
      WINDIR: "C:\\Windows",
      COMSPEC: "C:\\Windows\\System32\\cmd.exe",
      USERPROFILE: "C:\\Users\\real",
      APPDATA: "C:\\Users\\real\\AppData\\Roaming",
      SECRET_TOKEN: "must-not-leak",
    },
  });

  assert.deepEqual(environment, {
    PATH: "C:\\Windows\\System32",
    PATHEXT: ".COM;.EXE",
    SYSTEMROOT: "C:\\Windows",
    WINDIR: "C:\\Windows",
    COMSPEC: "C:\\Windows\\System32\\cmd.exe",
    HOME: "R:\\pi-home",
    USERPROFILE: "R:\\pi-home",
    HOMEDRIVE: "R:",
    HOMEPATH: "\\pi-home",
    APPDATA: "R:\\pi-home\\AppData\\Roaming",
    LOCALAPPDATA: "R:\\pi-home\\AppData\\Local",
    XDG_CONFIG_HOME: "R:\\pi-home\\.config",
    XDG_CACHE_HOME: "R:\\pi-home\\.cache",
    XDG_DATA_HOME: "R:\\pi-home\\.local\\share",
    TEMP: "R:\\pi-temp",
    TMP: "R:\\pi-temp",
  });
});

test("semantic post-adapter tool trace accepts ordered read bash write verify", () => {
  assert.doesNotThrow(() => validateSemanticToolTrace(successfulTrace("write")));
  assert.throws(
    () => validateSemanticToolTrace(successfulTrace("edit")),
    /tool trace/i,
  );
});

test("semantic post-adapter tool trace rejects failed missing and reordered tools", () => {
  const failed = successfulTrace();
  failed[1] = { ...failed[1], status: "failed" };
  assert.throws(() => validateSemanticToolTrace(failed), /tool trace/i);
  assert.throws(
    () => validateSemanticToolTrace(successfulTrace().toReversed()),
    /tool trace/i,
  );
  assert.throws(
    () => validateSemanticToolTrace(successfulTrace().slice(0, 3)),
    /tool trace/i,
  );
});

test("exact workspace validation accepts only the expected result bytes", async () => {
  await withTempDirectory("pi-exact-pass", async (directory) => {
    const seed = path.join(repositoryRoot, "examples/pi/tool-loop/seed");
    const workspace = path.join(directory, "workspace");
    await cp(seed, workspace, { recursive: true });
    await writeFile(path.join(workspace, "result.txt"), "sum=18\n");

    const result = await validateExactWorkspace({
      seedRoot: seed,
      workspaceRoot: workspace,
      expectedResult: path.join(
        repositoryRoot,
        "examples/pi/tool-loop/expected/result.txt",
      ),
      changedPath: "result.txt",
    });

    assert.deepEqual(result, { changedPath: "result.txt", changedFiles: 1 });
  });
});

test("exact workspace validation rejects extra deleted symlink mode and hardlink changes", async () => {
  const seed = path.join(repositoryRoot, "examples/pi/tool-loop/seed");
  const expectedResult = path.join(
    repositoryRoot,
    "examples/pi/tool-loop/expected/result.txt",
  );
  const cases = {
    extra: async (workspace) => writeFile(path.join(workspace, "extra.txt"), "extra\n"),
    deleted: async (workspace) => rm(path.join(workspace, "source.txt")),
    hardlink: async (workspace) => {
      await link(
        path.join(workspace, "source.txt"),
        path.join(workspace, "source-copy.txt"),
      );
    },
  };
  if (process.platform !== "win32") {
    cases.mode = async (workspace) =>
      chmod(path.join(workspace, "source.txt"), 0o600);
    cases.symlink = async (workspace) => {
      await rm(path.join(workspace, "result.txt"));
      await symlink("source.txt", path.join(workspace, "result.txt"));
    };
  }

  for (const [name, mutate] of Object.entries(cases)) {
    await withTempDirectory(`pi-exact-${name}`, async (directory) => {
      const workspace = path.join(directory, "workspace");
      await cp(seed, workspace, { recursive: true });
      await writeFile(path.join(workspace, "result.txt"), "sum=18\n");
      await mutate(workspace);
      await assert.rejects(
        validateExactWorkspace({
          seedRoot: seed,
          workspaceRoot: workspace,
          expectedResult,
          changedPath: "result.txt",
        }),
        /workspace/i,
        name,
      );
    });
  }
});

test("exact workspace validation rejects case-only path collisions", async () => {
  await withTempDirectory("pi-exact-case", async (directory) => {
    const seed = path.join(repositoryRoot, "examples/pi/tool-loop/seed");
    const workspace = path.join(directory, "workspace");
    await cp(seed, workspace, { recursive: true });
    await writeFile(path.join(workspace, "result.txt"), "sum=18\n");
    await rename(
      path.join(workspace, "source.txt"),
      path.join(workspace, "SOURCE.txt"),
    );

    await assert.rejects(
      validateExactWorkspace({
        seedRoot: seed,
        workspaceRoot: workspace,
        expectedResult: path.join(
          repositoryRoot,
          "examples/pi/tool-loop/expected/result.txt",
        ),
        changedPath: "result.txt",
      }),
      /case-only/i,
    );
  });
});

test("provider digest is exact bytes and post-recovery mismatches fail closed", () => {
  const bytes = Buffer.from('{"providers":{"loxa":{}}}\n');
  const digest = providerConfigDigest(bytes);
  assert.match(digest, /^[a-f0-9]{64}$/);
  assert.doesNotThrow(() => assertProviderDigest(digest, digest));
  assert.throws(
    () => assertProviderDigest(providerConfigDigest(Buffer.concat([bytes, Buffer.from(" ")])), digest),
    /provider config digest/i,
  );
});

test("sanitized evidence is constructed only from strict allowlisted fields", () => {
  const evidence = buildSanitizedEvidence({
    phase: "mac-local",
    providerConfigSha256: "a".repeat(64),
    modelsBefore: true,
    readyBefore: true,
    toolOrder: true,
    exactWorkspace: true,
    verification: true,
    modelsAfter: true,
    readyAfter: true,
    baseUrl: "https://private-node.invalid/v1",
    prompt: "private prompt",
    completion: "private completion",
    absolutePath: "/Users/private/source",
    environment: { SECRET: "credential" },
    rawLog: "tool arguments and output",
  });
  const serialized = JSON.stringify(evidence);

  assert.deepEqual(Object.keys(evidence).sort(), [
    "exactWorkspace",
    "modelsAfter",
    "modelsBefore",
    "phase",
    "providerConfigSha256",
    "readyAfter",
    "readyBefore",
    "schemaVersion",
    "toolOrder",
    "verification",
  ]);
  for (const leak of [
    "private-node",
    "private prompt",
    "private completion",
    "/Users/private",
    "credential",
    "tool arguments",
  ]) {
    assert.equal(serialized.includes(leak), false, leak);
  }
});

test("CLI parser exposes only the static acceptance arguments", () => {
  assert.deepEqual(
    parseArguments([
      "--phase",
      "post-recovery",
      "--base-url",
      "http://127.0.0.1:11435/v1",
      "--pi-bin",
      "/opt/pi",
      "--max-tokens",
      "1024",
      "--expected-config-sha256",
      "b".repeat(64),
      "--evidence-dir",
      "target/pi-acceptance/run",
    ]),
    {
      phase: "post-recovery",
      baseUrl: "http://127.0.0.1:11435/v1",
      piBin: "/opt/pi",
      maxTokens: 1024,
      expectedConfigSha256: "b".repeat(64),
      evidenceDir: "target/pi-acceptance/run",
    },
  );
  assert.throws(() => parseArguments(["--command-template", "pi {prompt}"]), /unknown/i);
  assert.throws(
    () =>
      parseArguments([
        "--phase",
        "mac-local",
        "--phase",
        "mac-local",
        "--base-url",
        "http://127.0.0.1:11435/v1",
      ]),
    /duplicate/i,
  );
  assert.throws(
    () =>
      parseArguments([
        "--phase",
        "mac-local",
        "--base-url",
        "http://127.0.0.1:11435/v1",
        "--pi-bin",
        "/opt/pi",
        "--max-tokens",
        "1024",
        "--evidence-dir",
        "target/pi-acceptance/../../private",
      ]),
    /evidence directory/i,
  );
  assert.throws(
    () =>
      parseArguments([
        "--phase",
        "post-recovery",
        "--base-url",
        "http://127.0.0.1:11435/v1",
        "--pi-bin",
        "/opt/pi",
        "--max-tokens",
        "1024",
        "--expected-config-sha256",
        "not-a-digest",
        "--evidence-dir",
        "target/pi-acceptance/run",
      ]),
    /digest/i,
  );
  assert.throws(
    () =>
      parseArguments([
        "--phase",
        "mac-local",
        "--base-url",
        "http://127.0.0.1:11435/v1",
        "--pi-bin",
        `pi${"\0"}private`,
        "--max-tokens",
        "1024",
        "--evidence-dir",
        "target/pi-acceptance/run",
      ]),
    /invalid/i,
  );
});

test("qualified Pi CLI requires bounded max tokens and an evidence directory", () => {
  const required = [
    "--phase",
    "mac-local",
    "--base-url",
    "http://127.0.0.1:11435/v1",
    "--pi-bin",
    "/opt/pi",
    "--max-tokens",
    "1024",
    "--evidence-dir",
    "target/pi-acceptance/test",
  ];
  assert.deepEqual(parseArguments(required), {
    phase: "mac-local",
    baseUrl: "http://127.0.0.1:11435/v1",
    piBin: "/opt/pi",
    maxTokens: 1024,
    evidenceDir: "target/pi-acceptance/test",
  });
  assert.throws(() => parseArguments(required.slice(0, -2)), /evidence directory/i);
  for (const value of ["0", "8192", "1.5", "words"]) {
    const invalidMaxTokens = [...required];
    invalidMaxTokens[7] = value;
    assert.throws(
      () => parseArguments(invalidMaxTokens),
      /max.tokens/i,
    );
  }
});

test("CLI atomically retains only sanitized successful evidence and exposes its reusable digest", async () => {
  const evidenceDir = `target/pi-acceptance/test-cli-evidence-${process.pid}`;
  const recoveryDir = `target/pi-acceptance/test-cli-recovery-${process.pid}`;
  const failureDir = `target/pi-acceptance/test-cli-failure-${process.pid}`;
  const unsafeDir = `target/pi-acceptance/test-cli-unsafe-${process.pid}`;
  const unsafeArtifactDir = `target/pi-acceptance/test-cli-artifact-${process.pid}`;
  const occupiedDir = `target/pi-acceptance/test-cli-occupied-${process.pid}`;
  const evidencePath = path.join(repositoryRoot, evidenceDir, "evidence.json");
  const recoveryPath = path.join(repositoryRoot, recoveryDir, "evidence.json");
  const failurePath = path.join(repositoryRoot, failureDir, "evidence.json");
  const occupiedPath = path.join(repositoryRoot, occupiedDir, "evidence.json");
  await rm(path.join(repositoryRoot, evidenceDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, recoveryDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, failureDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, unsafeDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, unsafeArtifactDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, occupiedDir), { recursive: true, force: true });
  await withTempDirectory("pi-cli", async (directory) => {
    const piBin = path.join(directory, "fake-pi");
    await writeFile(
      piBin,
      `#!/bin/sh
if [ "$1" = "--version" ]; then
  printf '0.82.1\\n'
  exit 0
fi
printf 'sum=18\\n' > result.txt
cat <<'EOF'
${successfulQualifiedLines().join("\n")}
EOF
`,
      { mode: 0o700 },
    );
    await chmod(piBin, 0o700);

    await withFakeGateway(async ({ baseUrl }) => {
      const first = await runCli([
        "--phase",
        "mac-local",
        "--base-url",
        baseUrl,
        "--pi-bin",
        piBin,
        "--max-tokens",
        "1024",
        "--evidence-dir",
        evidenceDir,
      ]);
      assert.equal(first.code, 0, first.stderr);
      assert.equal(first.stderr, "");
      const evidence = JSON.parse(first.stdout);
      assert.deepEqual(evidence, {
        schemaVersion: 1,
        phase: "mac-local",
        providerConfigSha256: evidence.providerConfigSha256,
        modelsBefore: true,
        readyBefore: true,
        toolOrder: true,
        exactWorkspace: true,
        verification: true,
        modelsAfter: true,
        readyAfter: true,
      });
      assert.match(evidence.providerConfigSha256, /^[a-f0-9]{64}$/);
      assert.deepEqual(JSON.parse(await readFile(evidencePath, "utf8")), evidence);
      assert.equal((await lstat(evidencePath)).mode & 0o777, 0o600);
      assert.deepEqual(await readdir(path.dirname(evidencePath)), ["evidence.json"]);
      const serialized = JSON.stringify(evidence);
      for (const privateSentinel of [piBin, baseUrl, "private", "source.txt"]) {
        assert.equal(serialized.includes(privateSentinel), false, privateSentinel);
      }

      const recovery = await runCli([
        "--phase",
        "post-recovery",
        "--base-url",
        baseUrl,
        "--pi-bin",
        piBin,
        "--max-tokens",
        "1024",
        "--expected-config-sha256",
        evidence.providerConfigSha256,
        "--evidence-dir",
        recoveryDir,
      ]);
      assert.equal(recovery.code, 0);
      assert.equal(JSON.parse(recovery.stdout).providerConfigSha256, evidence.providerConfigSha256);
      assert.equal(
        JSON.parse(await readFile(recoveryPath, "utf8")).providerConfigSha256,
        evidence.providerConfigSha256,
      );
    });

    await withFakeGateway(
      async ({ baseUrl }) => {
        const failed = await runCli([
          "--phase",
          "mac-local",
          "--base-url",
          baseUrl,
          "--pi-bin",
          piBin,
          "--max-tokens",
          "1024",
          "--evidence-dir",
          failureDir,
        ]);
        assert.equal(failed.code, 2);
        assert.equal(failed.stdout, "");
        await assertMissing(failurePath);
      },
      {
        status: JSON.stringify({
          health: "ready",
          model: "loxa",
          engine: { name: "llama-cpp", version: "version: 10106 (c0bc8591e)" },
        }),
      },
    );

    const escapedEvidence = path.join(directory, "escaped-evidence");
    await symlink(escapedEvidence, path.join(repositoryRoot, unsafeDir));
    await withFakeGateway(async ({ baseUrl }) => {
      const unsafe = await runCli([
        "--phase",
        "mac-local",
        "--base-url",
        baseUrl,
        "--pi-bin",
        piBin,
        "--max-tokens",
        "1024",
        "--evidence-dir",
        unsafeDir,
      ]);
      assert.equal(unsafe.code, 2);
      await assertMissing(path.join(escapedEvidence, "evidence.json"));
    });

    const escapedArtifact = path.join(directory, "escaped-artifact");
    const unsafeArtifactPath = path.join(repositoryRoot, unsafeArtifactDir);
    await mkdir(unsafeArtifactPath, { recursive: true });
    await symlink(escapedArtifact, path.join(unsafeArtifactPath, "evidence.json"));
    await withFakeGateway(async ({ baseUrl }) => {
      const unsafe = await runCli([
        "--phase",
        "mac-local",
        "--base-url",
        baseUrl,
        "--pi-bin",
        piBin,
        "--max-tokens",
        "1024",
        "--evidence-dir",
        unsafeArtifactDir,
      ]);
      assert.equal(unsafe.code, 2);
      await assertMissing(escapedArtifact);
    });

    const retainedEvidence = "existing evidence must remain byte-identical\n";
    await mkdir(path.dirname(occupiedPath), { recursive: true });
    await writeFile(occupiedPath, retainedEvidence, { mode: 0o600 });
    await withFakeGateway(async ({ baseUrl }) => {
      const occupied = await runCli([
        "--phase",
        "mac-local",
        "--base-url",
        baseUrl,
        "--pi-bin",
        piBin,
        "--max-tokens",
        "1024",
        "--evidence-dir",
        occupiedDir,
      ]);
      assert.equal(occupied.code, 2);
      assert.equal(occupied.stdout, "");
      assert.equal(occupied.stderr.includes(retainedEvidence.trim()), false);
      assert.equal(await readFile(occupiedPath, "utf8"), retainedEvidence);
      assert.deepEqual(await readdir(path.dirname(occupiedPath)), ["evidence.json"]);
    });
  });
  await rm(path.join(repositoryRoot, evidenceDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, recoveryDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, failureDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, unsafeDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, unsafeArtifactDir), { recursive: true, force: true });
  await rm(path.join(repositoryRoot, occupiedDir), { recursive: true, force: true });
});

test("qualified Pi adapter removes its private temp when evidence publication fails", async () => {
  const evidenceDir = `target/pi-acceptance/test-publish-failure-${process.pid}`;
  const absoluteEvidenceDir = path.join(repositoryRoot, evidenceDir);
  await rm(absoluteEvidenceDir, { recursive: true, force: true });
  try {
    await withFakeGateway(async ({ baseUrl }) => {
      await assert.rejects(
        runQualifiedPiAdapter(
          {
            phase: "mac-local",
            baseUrl,
            piBin: "/fake/pi",
            maxTokens: 1024,
            processTimeoutMs: 1000,
            evidenceDir,
          },
          {
            platform: "darwin",
            sourceEnvironment: { PATH: "/usr/bin:/bin" },
            spawnProcess: fakeSpawn({
              lines: successfulQualifiedLines(),
              writeExpectedResult: true,
            }),
            publishEvidence: async () => {
              throw new Error("simulated evidence publication failure");
            },
          },
        ),
        /simulated evidence publication failure/i,
      );
      assert.deepEqual(await readdir(absoluteEvidenceDir), []);
      await assertMissing(path.join(absoluteEvidenceDir, "evidence.json"));
    });
  } finally {
    await rm(absoluteEvidenceDir, { recursive: true, force: true });
  }
});

test("qualified Pi config and argv pin the output field, trusted extension, and no-session mode", async () => {
  const { buildQualifiedPiArgv, buildRuntimeModelsConfig } = await import(
    "./pi-acceptance.mjs"
  );
  assert.deepEqual(
    buildRuntimeModelsConfig("http://127.0.0.1:11435/v1", 1024),
    {
      providers: {
        loxa: {
          baseUrl: "http://127.0.0.1:11435/v1",
          api: "openai-completions",
          apiKey: "loxa-dummy-key",
          models: [
            {
              id: "loxa",
              name: "Loxa",
              reasoning: false,
              input: ["text"],
              contextWindow: 8192,
              maxTokens: 1024,
              compat: { maxTokensField: "max_tokens" },
            },
          ],
        },
      },
    },
  );
  const argv = buildQualifiedPiArgv("/trusted/acceptance-gate.mjs", "prompt");
  assert.deepEqual(argv, [
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
    "/trusted/acceptance-gate.mjs",
    "--no-skills",
    "--no-prompt-templates",
    "--no-context-files",
    "--no-themes",
    "--no-approve",
    "--offline",
    "prompt",
  ]);
});

test("qualified Pi JSONL requires the version-3 session, agent end, and agent settled", () => {
  assert.throws(
    () =>
      adaptQualifiedPiJsonl(
        successfulQualifiedLines().filter(
          (line) => JSON.parse(line).type !== "agent_end",
        ),
      ),
    /agent_end/i,
  );
});

test("qualified Pi executable must resolve absolutely and report exactly version 0.82.1", async () => {
  const { qualifyPiExecutable } = await import("./pi-acceptance.mjs");
  await assert.doesNotReject(
    qualifyPiExecutable("/opt/pi", {
      resolveExecutable: async () => "/opt/pi",
      readVersion: async () => "0.82.1\n",
    }),
  );
  for (const [resolved, version] of [
    ["pi", "0.82.1\n"],
    ["/opt/pi", "0.82.0\n"],
  ]) {
    await assert.rejects(
      qualifyPiExecutable("/opt/pi", {
        resolveExecutable: async () => resolved,
        readVersion: async () => version,
      }),
      /qualified Pi executable/i,
    );
  }
});

test("Pi version probe bounds hung and oversized executable output", async () => {
  const { readPiVersion } = await import("./pi-acceptance.mjs");
  for (const scenario of [
    { hang: true },
    { output: "x".repeat(4097) },
  ]) {
    const capture = { kills: [] };
    const version = readPiVersion("/opt/pi", {
      spawnProcess: () => {
        const child = new EventEmitter();
        child.stdout = new PassThrough();
        child.kill = (signal) => {
          capture.kills.push(signal);
          return true;
        };
        queueMicrotask(() => {
          if (scenario.output !== undefined) {
            child.stdout.write(scenario.output);
          }
        });
        return child;
      },
      timeoutMs: 10,
      forceKillDelayMs: 0,
      terminalTimeoutMs: 10,
    });
    await assert.rejects(version, /qualified Pi executable verification failed/i);
    assert.deepEqual(capture.kills, ["SIGTERM", "SIGKILL"]);
  }
});

test("Windows taskkill cleanup resolves the system executable and waits for close or error", async () => {
  const { terminateOwnedProcessTree } = await import("./pi-acceptance.mjs");
  for (const terminal of [
    ["close", 0, false],
    ["close", 1, true],
    ["error", new Error("unavailable"), true],
  ]) {
    const events = new EventEmitter();
    events.unref = () => {};
    const child = { pid: 42, kill: () => assert.fail("fallback must not run") };
    const completion = terminateOwnedProcessTree({
      child,
      platform: "win32",
      signal: "SIGTERM",
      signalProcess: () => assert.fail("POSIX signal must not run"),
      spawnTreeKiller: (program, argv) => {
        assert.equal(program, "C:\\Windows\\System32\\taskkill.exe");
        assert.deepEqual(argv, ["/PID", "42", "/T"]);
        return events;
      },
      taskkillExecutable: "C:\\Windows\\System32\\taskkill.exe",
    });
    const [event, value, rejected] = terminal;
    if (rejected) {
      events.emit(event, value);
      await assert.rejects(completion, /Pi process cleanup failed/i);
    } else {
      let settled = false;
      completion.then(() => {
        settled = true;
      });
      await Promise.resolve();
      assert.equal(settled, false);
      events.emit(event, value);
      await completion;
    }
  }
});

test("Windows taskkill cleanup terminates a no-event helper by its terminal deadline", async () => {
  const { terminateOwnedProcessTree } = await import("./pi-acceptance.mjs");
  const killer = new EventEmitter();
  const kills = [];
  killer.unref = () => {};
  killer.kill = (signal) => {
    kills.push(signal);
    return true;
  };
  await assert.rejects(
    terminateOwnedProcessTree({
      child: { pid: 42, kill: () => assert.fail("fallback must not run") },
      platform: "win32",
      signal: "SIGTERM",
      signalProcess: () => assert.fail("POSIX signal must not run"),
      spawnTreeKiller: () => killer,
      taskkillExecutable: "C:\\Windows\\System32\\taskkill.exe",
      terminalTimeoutMs: 10,
    }),
    /Pi process cleanup failed/i,
  );
  assert.deepEqual(kills, ["SIGKILL"]);
});

test("qualified Pi adapter settles and cleans up after Windows taskkill emits no terminal event", async () => {
  await withFakeGateway(async ({ baseUrl }) => {
    const capture = {};
    const adapter = runQualifiedPiAdapter(
      {
        phase: "mac-local",
        baseUrl,
        piBin: "/fake/pi",
        qualifiedMaxTokens: 1024,
        processTimeoutMs: 10,
      },
      {
        platform: "darwin",
        processPlatform: "win32",
        taskkillTerminalTimeoutMs: 10,
        sourceEnvironment: { PATH: "/usr/bin:/bin" },
        spawnProcess: fakeSpawn(
          { hang: true, pid: 4242, ignoreAllKills: true },
          capture,
        ),
        spawnTreeKiller: () => {
          const killer = new EventEmitter();
          killer.unref = () => {};
          killer.kill = () => true;
          return killer;
        },
      },
    );
    const outcome = await observeSettlement(adapter, 1000);
    assert.equal(outcome.status, "rejected");
    assert.match(outcome.error.message, /Pi process cleanup failed/i);
    await assertMissing(capture.options.cwd);
  });
});

test("qualified Pi JSONL maps correlated successful tools to the semantic trace", () => {
  const lines = successfulQualifiedLines();
  lines.splice(
    3,
    0,
    JSON.stringify({
      type: "tool_execution_update",
      toolCallId: "read-1",
      toolName: "read",
      args: { path: "source.txt" },
      partialResult: { content: "private partial result" },
    }),
  );
  const trace = adaptQualifiedPiJsonl(lines);

  assert.deepEqual(trace, successfulTrace());
  assert.equal(JSON.stringify(trace).includes("private"), false);
});

test("qualified Pi JSONL rejects concurrent tool starts before the prior result", () => {
  const lines = successfulQualifiedLines();
  lines.splice(
    2,
    0,
    JSON.stringify({
      type: "tool_execution_start",
      toolCallId: "bash-1",
      toolName: "bash",
      args: { command: "node verify.mjs --precheck" },
    }),
  );
  assert.throws(() => adaptQualifiedPiJsonl(lines), /concurrent tool/i);
});

test("qualified Pi JSONL rejects edit as the canonical third completion", () => {
  assert.throws(
    () => adaptQualifiedPiJsonl(successfulQualifiedLines("edit")),
    /tool correlation/i,
  );
});

test("qualified Pi JSONL rejects every extra successful allowed tool completion", () => {
  const insertions = [
    { index: 2, toolCallId: "extra-before", toolName: "bash" },
    { index: 4, toolCallId: "extra-within", toolName: "read" },
    { index: 10, toolCallId: "extra-after", toolName: "write" },
  ];
  for (const { index, toolCallId, toolName } of insertions) {
    const lines = successfulQualifiedLines();
    lines.splice(
      index,
      0,
      JSON.stringify({
        type: "tool_execution_start",
        toolCallId,
        toolName,
        args: { private: "PRIVATE ARGUMENT" },
      }),
      JSON.stringify({
        type: "tool_execution_end",
        toolCallId,
        toolName,
        result: { content: "PRIVATE RESULT" },
        isError: false,
      }),
    );

    assert.throws(
      () => adaptQualifiedPiJsonl(lines),
      (error) =>
        /exactly four successful tool completions/i.test(error.message) &&
        !error.message.includes("PRIVATE"),
    );
  }
  assert.throws(
    () =>
      validateSemanticToolTrace([
        ...successfulTrace(),
        { tool: "read", status: "success" },
      ]),
    /tool trace/i,
  );
});

test("qualified Pi JSONL requires exact private payload object fields", () => {
  const mutations = [
    ["tool_execution_start", "args", undefined],
    ["tool_execution_start", "args", []],
    ["tool_execution_update", "args", undefined],
    ["tool_execution_update", "args", "PRIVATE ARGUMENT"],
    ["tool_execution_update", "partialResult", undefined],
    ["tool_execution_update", "partialResult", []],
    ["tool_execution_end", "result", undefined],
    ["tool_execution_end", "result", "PRIVATE RESULT"],
  ];
  for (const [type, field, value] of mutations) {
    const lines = successfulQualifiedLines();
    if (type === "tool_execution_update") {
      lines.splice(
        3,
        0,
        JSON.stringify({
          type,
          toolCallId: "read-1",
          toolName: "read",
          args: { path: "PRIVATE PATH" },
          partialResult: { content: "PRIVATE RESULT" },
        }),
      );
    }
    const index = lines.findIndex(
      (line) => JSON.parse(line).type === type,
    );
    const record = JSON.parse(lines[index]);
    if (value === undefined) {
      delete record[field];
    } else {
      record[field] = value;
    }
    lines[index] = JSON.stringify(record);

    assert.throws(
      () => adaptQualifiedPiJsonl(lines),
      (error) =>
        /Pi JSONL event shape is invalid/i.test(error.message) &&
        !error.message.includes("PRIVATE"),
    );
  }
});

test("qualified Pi JSONL rejects malformed ambiguous mismatched and failed records privately", () => {
  const rejected = [
    ["{PRIVATE MALFORMED", /invalid Pi JSONL/i],
    [
      JSON.stringify({
        type: "tool_execution_end",
        toolCallId: "unknown",
        toolName: "read",
        result: { content: "PRIVATE RESULT" },
        isError: false,
      }),
      /tool correlation/i,
    ],
    [
      [
        ...successfulQualifiedLines().slice(0, 3),
        JSON.stringify({
          type: "tool_execution_end",
          toolCallId: "read-1",
          toolName: "bash",
          result: { content: "PRIVATE RESULT" },
          isError: false,
        }),
      ],
      /tool correlation/i,
    ],
    [
      [
        JSON.stringify({
          type: "tool_execution_start",
          toolCallId: "read-1",
          toolName: "read",
          args: { path: "PRIVATE PATH" },
        }),
        JSON.stringify({
          type: "tool_execution_end",
          toolCallId: "read-1",
          toolName: "read",
          result: { content: "PRIVATE RESULT" },
          isError: true,
        }),
      ],
      /tool execution failed/i,
    ],
    [JSON.stringify({ type: "unknown_private_event" }), /unknown Pi JSONL/i],
    [
      [
        JSON.stringify({ type: "agent_start" }),
        JSON.stringify({ type: "session", version: 3 }),
      ],
      /session/i,
    ],
    [
      [
        JSON.stringify({ type: "agent_end" }),
        JSON.stringify({ type: "agent_end" }),
      ],
      /duplicate terminal/i,
    ],
    [
      [
        JSON.stringify({
          type: "tool_execution_start",
          toolCallId: "read-1",
          toolName: "read",
          args: { path: "PRIVATE PATH" },
        }),
        JSON.stringify({
          type: "tool_execution_update",
          toolCallId: "other",
          toolName: "read",
          args: { path: "PRIVATE PATH" },
          partialResult: { content: "PRIVATE RESULT" },
        }),
      ],
      /tool correlation/i,
    ],
    [
      [
        JSON.stringify({
          type: "tool_execution_start",
          toolCallId: "read-1",
          toolName: "read",
          args: { path: "PRIVATE PATH" },
        }),
        JSON.stringify({
          type: "tool_execution_end",
          toolCallId: "read-1",
          toolName: "read",
          result: { content: "PRIVATE RESULT" },
        }),
      ],
      /tool execution failed/i,
    ],
    [
      [
        ...successfulQualifiedLines(),
        JSON.stringify({ type: "agent_settled" }),
      ],
      /duplicate terminal/i,
    ],
  ];

  for (const [lines, expected] of rejected) {
    assert.throws(
      () => adaptQualifiedPiJsonl(Array.isArray(lines) ? lines : [lines]),
      (error) =>
        expected.test(error.message) && !error.message.includes("PRIVATE"),
    );
  }
});

test("qualified Pi JSONL requires settled lifecycle and no pending tool call", () => {
  assert.throws(
    () => adaptQualifiedPiJsonl(successfulQualifiedLines().slice(0, -1)),
    /agent_settled/i,
  );
  assert.throws(
    () =>
      adaptQualifiedPiJsonl([
        JSON.stringify({
          type: "tool_execution_start",
          toolCallId: "pending",
          toolName: "read",
          args: { path: "source.txt" },
        }),
        JSON.stringify({ type: "agent_settled" }),
      ]),
    /pending tool/i,
  );
  assert.throws(
    () =>
      adaptQualifiedPiJsonl(
        Array.from({ length: 10_001 }, () =>
          JSON.stringify({ type: "agent_start" }),
        ),
      ),
    /line count/i,
  );
});

test("qualified Pi adapter uses exact argv isolated env config endpoints and cleanup", async () => {
  await withFakeGateway(async ({ baseUrl, requests }) => {
    const capture = {};
    const result = await runQualifiedPiAdapter(
      {
        phase: "mac-local",
        baseUrl,
        piBin: "/fake/pi",
        maxTokens: 1024,
        processTimeoutMs: 1000,
      },
      {
        platform: "darwin",
        sourceEnvironment: {
          PATH: "/usr/bin:/bin",
          LANG: "en_US.UTF-8",
          OPENAI_API_KEY: "must-not-leak",
        },
        spawnProcess: fakeSpawn(
          { lines: successfulQualifiedLines(), writeExpectedResult: true },
          capture,
        ),
      },
    );

    const prompt = await readFile(
      path.join(repositoryRoot, "examples/pi/tool-loop/prompt.txt"),
      "utf8",
    );
    const extensionPath = path.join(
      repositoryRoot,
      "examples/pi/tool-loop/acceptance-gate.mjs",
    );
    assert.equal(capture.program, "/fake/pi");
    assert.deepEqual(capture.argv, [
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
    ]);
    assert.equal(capture.argv.includes("--print"), false);
    assert.equal(capture.options.detached, true);
    assert.equal(capture.options.shell, false);
    assert.deepEqual(capture.options.stdio, ["ignore", "pipe", "pipe"]);
    assert.equal(capture.options.windowsHide, true);
    assert.equal(
      path.relative(repositoryRoot, capture.options.cwd).startsWith(".."),
      true,
    );
    assert.equal(capture.options.env.OPENAI_API_KEY, undefined);
    assert.deepEqual(Object.keys(capture.options.env).sort(), [
      "HOME",
      "LANG",
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
    assert.equal(capture.options.env.PI_OFFLINE, "1");
    assert.equal(capture.options.env.PI_TELEMETRY, "0");
    assert.equal(capture.options.env.PI_SKIP_VERSION_CHECK, "1");
    assert.equal(
      path.basename(
        path.join(
          capture.options.env.PI_CODING_AGENT_DIR,
          "models.json",
        ),
      ),
      "models.json",
    );
    assert.equal(
      capture.modelsConfig.providers.loxa.models[0].maxTokens,
      1024,
    );
    assert.equal(
      capture.modelsConfig.providers.loxa.models[0].compat.maxTokensField,
      "max_tokens",
    );
    assert.equal(capture.modelsConfig.providers.loxa.baseUrl, baseUrl);
    assert.deepEqual(requests, [
      "/v1/models",
      "/loxa/status",
      "/v1/models",
      "/loxa/status",
    ]);
    assert.deepEqual(result.semanticTrace, successfulTrace());
    assert.match(result.providerConfigSha256, /^[a-f0-9]{64}$/);
    assert.equal(JSON.stringify(result).includes("private"), false);
    await assertMissing(capture.options.cwd);
    await assertMissing(capture.options.env.HOME);
    await assertMissing(capture.options.env.PI_CODING_AGENT_DIR);
  });
});

test("qualified Pi adapter accepts its live default process environment without a real spawn", async () => {
  await withFakeGateway(async ({ baseUrl }) => {
    const capture = {};
    const result = await runQualifiedPiAdapter(
      {
        phase: "mac-local",
        baseUrl,
        piBin: "/fake/pi",
        qualifiedMaxTokens: 1024,
        processTimeoutMs: 1000,
      },
      {
        platform: "darwin",
        spawnProcess: fakeSpawn(
          { lines: successfulQualifiedLines(), writeExpectedResult: true },
          capture,
        ),
      },
    );
    const allowedKeys = new Set([
      "PATH",
      "LANG",
      "LC_ALL",
      "LC_CTYPE",
      "HOME",
      "XDG_CONFIG_HOME",
      "XDG_CACHE_HOME",
      "XDG_DATA_HOME",
      "TMPDIR",
      "PI_CODING_AGENT_DIR",
      "PI_OFFLINE",
      "PI_SKIP_VERSION_CHECK",
      "PI_TELEMETRY",
    ]);

    assert.deepEqual(result.semanticTrace, successfulTrace());
    assert.equal(
      Object.keys(capture.options.env).every((key) =>
        allowedKeys.has(key),
      ),
      true,
    );
    assert.equal(capture.options.env.OPENAI_API_KEY, undefined);
    await assertMissing(capture.options.cwd);
  });
});

test("qualified Pi adapter bounds gateway response bodies before spawning", async () => {
  let spawned = false;
  await withFakeGateway(
    async ({ baseUrl }) => {
      await assert.rejects(
        runQualifiedPiAdapter(
          {
            phase: "mac-local",
            baseUrl,
            piBin: "/fake/pi",
            qualifiedMaxTokens: 1024,
            processTimeoutMs: 1000,
          },
          {
            platform: "darwin",
            sourceEnvironment: { PATH: "/usr/bin:/bin" },
            spawnProcess: () => {
              spawned = true;
              throw new Error("must not spawn");
            },
          },
        ),
        /gateway acceptance response exceeded/i,
      );
    },
    { models: JSON.stringify({ padding: "x".repeat(64 * 1024) }) },
  );
  assert.equal(spawned, false);
});

test("qualified Pi adapter rejects nonzero exit and process errors without leaking stderr", async () => {
  for (const scenario of [
    {
      lines: successfulQualifiedLines(),
      stderrBytes: "PRIVATE STDERR",
      exitCode: 7,
    },
    { processError: true },
  ]) {
    await withFakeGateway(async ({ baseUrl }) => {
      await assert.rejects(
        runQualifiedPiAdapter(
          {
            phase: "mac-local",
            baseUrl,
            piBin: "/fake/pi",
            qualifiedMaxTokens: 1024,
            processTimeoutMs: 1000,
          },
          {
            platform: "darwin",
            sourceEnvironment: { PATH: "/usr/bin:/bin" },
            spawnProcess: fakeSpawn(scenario),
          },
        ),
        (error) =>
          /Pi process/i.test(error.message) &&
          !error.message.includes("PRIVATE"),
      );
    });
  }
});

test("qualified Pi adapter enforces stdout stderr line and lifecycle bounds", async () => {
  const scenarios = [
    { stdoutBytes: Buffer.alloc(1024 * 1024 + 1, 120) },
    { stdoutBytes: `${"x".repeat(64 * 1024 + 1)}\n` },
    { stderrBytes: Buffer.alloc(64 * 1024 + 1, 120), hang: false },
  ];
  for (const scenario of scenarios) {
    await withFakeGateway(async ({ baseUrl }) => {
      await assert.rejects(
        runQualifiedPiAdapter(
          {
            phase: "mac-local",
            baseUrl,
            piBin: "/fake/pi",
            qualifiedMaxTokens: 1024,
            processTimeoutMs: 1000,
          },
          {
            platform: "darwin",
            sourceEnvironment: { PATH: "/usr/bin:/bin" },
            spawnProcess: fakeSpawn(scenario),
          },
        ),
        /Pi process output limit/i,
      );
    });
  }
});

test("qualified Pi adapter times out and honors cancellation with child termination", async () => {
  await withFakeGateway(async ({ baseUrl }) => {
    const timeoutCapture = {};
    await assert.rejects(
      runQualifiedPiAdapter(
        {
          phase: "mac-local",
          baseUrl,
          piBin: "/fake/pi",
          qualifiedMaxTokens: 1024,
          processTimeoutMs: 20,
        },
        {
          platform: "darwin",
          sourceEnvironment: { PATH: "/usr/bin:/bin" },
          spawnProcess: fakeSpawn({ hang: true }, timeoutCapture),
        },
      ),
      /timed out/i,
    );
    assert.deepEqual(timeoutCapture.kills, ["SIGTERM", "SIGKILL"]);

    const controller = new AbortController();
    const cancellationCapture = {};
    await assert.rejects(
      runQualifiedPiAdapter(
        {
          phase: "mac-local",
          baseUrl,
          piBin: "/fake/pi",
          qualifiedMaxTokens: 1024,
          processTimeoutMs: 1000,
          signal: controller.signal,
        },
        {
          platform: "darwin",
          sourceEnvironment: { PATH: "/usr/bin:/bin" },
          spawnProcess: fakeSpawn(
            { hang: true, onStart: () => controller.abort() },
            cancellationCapture,
          ),
        },
      ),
      /cancelled/i,
    );
    assert.deepEqual(cancellationCapture.kills, ["SIGTERM", "SIGKILL"]);

    const fallbackCapture = {};
    await assert.rejects(
      runQualifiedPiAdapter(
        {
          phase: "mac-local",
          baseUrl,
          piBin: "/fake/pi",
          qualifiedMaxTokens: 1024,
          processTimeoutMs: 20,
        },
        {
          platform: "darwin",
          sourceEnvironment: { PATH: "/usr/bin:/bin" },
          spawnProcess: fakeSpawn(
            { hang: true, ignoreSigterm: true },
            fallbackCapture,
          ),
        },
      ),
      /timed out/i,
    );
    assert.deepEqual(fallbackCapture.kills, ["SIGTERM", "SIGKILL"]);
  });
});

test("qualified Pi adapter owns macOS and Windows process trees", async () => {
  const cases = [
    {
      platform: "darwin",
      expected: [
        { pid: -4242, signal: "SIGTERM" },
        { pid: -4242, signal: "SIGKILL" },
      ],
    },
    {
      platform: "win32",
      expected: [
        {
          program: "C:\\Windows\\System32\\taskkill.exe",
          argv: ["/PID", "4242", "/T"],
          options: {
            shell: false,
            stdio: "ignore",
            windowsHide: true,
          },
        },
        {
          program: "C:\\Windows\\System32\\taskkill.exe",
          argv: ["/PID", "4242", "/T", "/F"],
          options: {
            shell: false,
            stdio: "ignore",
            windowsHide: true,
          },
        },
      ],
    },
  ];
  for (const { platform, expected } of cases) {
    await withFakeGateway(async ({ baseUrl }) => {
      const capture = {};
      const treeSignals = [];
      const treeKillers = [];
      const adapter = runQualifiedPiAdapter(
        {
          phase: "mac-local",
          baseUrl,
          piBin: "/fake/pi",
          qualifiedMaxTokens: 1024,
          processTimeoutMs: 10,
        },
        {
          platform: "darwin",
          processPlatform: platform,
          sourceEnvironment: { PATH: "/usr/bin:/bin" },
          spawnProcess: fakeSpawn(
            { hang: true, pid: 4242, ignoreAllKills: true },
            capture,
          ),
          signalProcess: (pid, signal) => {
            treeSignals.push({ pid, signal });
          },
          spawnTreeKiller: (program, argv, options) => {
            treeKillers.push({ program, argv, options });
            const killer = new EventEmitter();
            killer.unref = () => {};
            queueMicrotask(() => killer.emit("close", 0));
            return killer;
          },
        },
      );
      const outcome = await observeSettlement(adapter, 1000);
      if (outcome.status === "watchdog") {
        capture.close(null, "SIGKILL");
        await assert.rejects(adapter);
      }

      assert.equal(outcome.status, "rejected");
      assert.match(outcome.error.message, /timed out/i);
      assert.deepEqual(
        platform === "win32" ? treeKillers : treeSignals,
        expected,
      );
      assert.equal(capture.options.detached, platform !== "win32");
      await assertMissing(capture.options.cwd);
    });
  }
});

test("qualified Pi adapter forces the owned group after the direct child closes", async () => {
  await withFakeGateway(async ({ baseUrl }) => {
    const capture = {};
    const treeSignals = [];
    const adapter = runQualifiedPiAdapter(
      {
        phase: "mac-local",
        baseUrl,
        piBin: "/fake/pi",
        qualifiedMaxTokens: 1024,
        processTimeoutMs: 10,
      },
      {
        platform: "darwin",
        sourceEnvironment: { PATH: "/usr/bin:/bin" },
        spawnProcess: fakeSpawn(
          { hang: true, pid: 4242, ignoreAllKills: true },
          capture,
        ),
        signalProcess: (pid, signal) => {
          treeSignals.push({ pid, signal });
          if (signal === "SIGTERM") {
            queueMicrotask(() => capture.close(null, signal));
          }
        },
      },
    );

    await assert.rejects(adapter, /timed out/i);
    assert.deepEqual(treeSignals, [
      { pid: -4242, signal: "SIGTERM" },
      { pid: -4242, signal: "SIGKILL" },
    ]);
    await assertMissing(capture.options.cwd);
  });
});

test("qualified Pi adapter handles asynchronous Windows tree-killer failure privately", async () => {
  await withFakeGateway(async ({ baseUrl }) => {
    const capture = {};
    const adapter = runQualifiedPiAdapter(
      {
        phase: "mac-local",
        baseUrl,
        piBin: "/fake/pi",
        qualifiedMaxTokens: 1024,
        processTimeoutMs: 10,
      },
      {
        platform: "darwin",
        processPlatform: "win32",
        sourceEnvironment: { PATH: "/usr/bin:/bin" },
        spawnProcess: fakeSpawn(
          { hang: true, pid: 4242, ignoreAllKills: true },
          capture,
        ),
        spawnTreeKiller: () => {
          const killer = new EventEmitter();
          killer.unref = () => {};
          queueMicrotask(() =>
            killer.emit("error", new Error("PRIVATE TASKKILL ERROR")),
          );
          return killer;
        },
      },
    );

    await assert.rejects(
      adapter,
      (error) =>
        /Pi process cleanup failed/i.test(error.message) &&
        !error.message.includes("PRIVATE"),
    );
    assert.deepEqual(capture.kills, []);
    await assertMissing(capture.options.cwd);
  });
});

test("qualified Pi adapter rejects by a terminal deadline and cleans descendant-held pipes", async () => {
  for (const scenario of [
    { hang: true, ignoreAllKills: true },
    { hang: true, descendantKeepsPipes: true },
  ]) {
    await withFakeGateway(async ({ baseUrl }) => {
      const capture = {};
      const adapter = runQualifiedPiAdapter(
        {
          phase: "mac-local",
          baseUrl,
          piBin: "/fake/pi",
          qualifiedMaxTokens: 1024,
          processTimeoutMs: 10,
        },
        {
          platform: "darwin",
          sourceEnvironment: { PATH: "/usr/bin:/bin" },
          spawnProcess: fakeSpawn(scenario, capture),
        },
      );
      const outcome = await observeSettlement(adapter, 1000);
      if (outcome.status === "watchdog") {
        capture.close(null, "SIGKILL");
        await assert.rejects(adapter);
      }

      assert.equal(outcome.status, "rejected");
      assert.match(outcome.error.message, /timed out/i);
      assert.equal(capture.child.stdout.destroyed, true);
      assert.equal(capture.child.stderr.destroyed, true);
      await assertMissing(capture.options.cwd);
    });
  }
});

test("live Pi adapter remains blocked until a safe output limit is qualified", async () => {
  await assert.rejects(
    runQualifiedPiAdapter(),
    (error) =>
      error instanceof QualificationRequiredError &&
      /safe output-token limit qualification required/i.test(error.message),
  );
  for (const invalid of [0, 8192, 1.5, "1024"]) {
    await assert.rejects(
      runQualifiedPiAdapter({ qualifiedMaxTokens: invalid }),
      /qualified output-token limit/i,
    );
  }
});
