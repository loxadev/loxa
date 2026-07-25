import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";

import {
  adaptQualifiedPiJsonl,
  buildQualifiedPiArgv,
  createBridgeCancellation,
  parseBridgeArguments,
  qualifyPiEntrypoint,
  readPiVersion,
  runQualifiedPiBridge,
  terminateOwnedProcessTree,
  validateIsolatedEnvironment,
  validateSemanticToolTrace,
} from "./pi-acceptance.mjs";

test("private bridge control channel cancels on a byte or EOF", async () => {
  for (const action of ["byte", "eof"]) {
    const control = new PassThrough();
    const cancellation = createBridgeCancellation(control);
    assert.equal(cancellation.signal.aborted, false);

    if (action === "byte") {
      control.write(Buffer.from([1]));
    } else {
      control.end();
    }
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(cancellation.signal.aborted, true);
    cancellation.dispose();
    control.destroy();
  }
});

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
    child.pid = scenario.pid ?? 4242;
    capture.child = child;
    let closed = false;
    const close = (code, signal = null) => {
      if (closed) return;
      closed = true;
      child.stdout.end();
      child.stderr.end();
      child.emit("close", code, signal);
    };
    capture.close = close;
    child.kill = (signal = "SIGTERM") => {
      capture.kills.push(signal);
      if (!scenario.ignoreAllKills) {
        queueMicrotask(() => close(null, signal));
      }
      return true;
    };
    queueMicrotask(() => {
      if (scenario.hang) return;
      if (scenario.stdoutBytes) {
        child.stdout.write(scenario.stdoutBytes);
      } else {
        for (const line of scenario.lines ?? successfulQualifiedLines()) {
          child.stdout.write(`${line}\n`);
        }
      }
      if (scenario.stderrBytes) child.stderr.write(scenario.stderrBytes);
      if (scenario.processError) {
        child.emit("error", new Error("private process error"));
      } else {
        close(scenario.exitCode ?? 0);
      }
    });
    return child;
  };
}

async function isolatedBridgeOptions(run) {
  const root = await mkdtemp(path.join(os.tmpdir(), "loxa-pi-bridge-test-"));
  const options = {
    piEntrypoint: path.join(root, "dist", "cli.js"),
    extension: path.join(root, "acceptance-gate.mjs"),
    prompt: "prompt",
    processTimeoutMs: 1000,
    cwd: path.join(root, "workspace"),
    environment: {
      PATH: "/usr/bin:/bin",
      HOME: path.join(root, "home"),
      XDG_CONFIG_HOME: path.join(root, "home", ".config"),
      XDG_CACHE_HOME: path.join(root, "home", ".cache"),
      XDG_DATA_HOME: path.join(root, "home", ".local", "share"),
      TMPDIR: path.join(root, "tmp"),
      PI_CODING_AGENT_DIR: path.join(root, "pi-config"),
      PI_OFFLINE: "1",
      PI_TELEMETRY: "0",
      PI_SKIP_VERSION_CHECK: "1",
    },
  };
  try {
    return await run(options);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function withActualProcessEnvironment(environment, run) {
  const original = { ...process.env };
  try {
    for (const key of Object.keys(process.env)) {
      delete process.env[key];
    }
    Object.assign(process.env, environment);
    return await run(process.env);
  } finally {
    for (const key of Object.keys(process.env)) {
      delete process.env[key];
    }
    Object.assign(process.env, original);
  }
}

test("bridge parser accepts only Pi process inputs", () => {
  assert.deepEqual(
    parseBridgeArguments([
      "--pi-entrypoint",
      "/opt/pi/dist/cli.js",
      "--extension",
      "/opt/acceptance-gate.mjs",
      "--prompt",
      "prompt",
      "--timeout-ms",
      "120000",
    ]),
    {
      piEntrypoint: "/opt/pi/dist/cli.js",
      extension: "/opt/acceptance-gate.mjs",
      prompt: "prompt",
      processTimeoutMs: 120000,
    },
  );
  for (const arguments_ of [
    ["--phase", "mac-local"],
    ["--base-url", "http://127.0.0.1/v1"],
    ["--evidence-dir", "target/pi-acceptance/test"],
    ["--max-tokens", "4096"],
  ]) {
    assert.throws(() => parseBridgeArguments(arguments_), /arguments/i);
  }
});

test("bridge source contains no gateway fixture config workspace or evidence orchestration", async () => {
  const source = await readFile(new URL("./pi-acceptance.mjs", import.meta.url), "utf8");
  for (const forbidden of [
    "fetch(",
    "mkdtemp",
    "buildRuntimeModelsConfig",
    "validateGatewayAcceptance",
    "validateExactWorkspace",
    "writeSanitizedEvidence",
    "evidenceDir",
    "maxTokens",
    "baseUrl",
  ]) {
    assert.equal(source.includes(forbidden), false, `forbidden Node ownership: ${forbidden}`);
  }
});

test("committed prompt seed expected result and provider examples retain the qualified contract", async () => {
  const repository = path.resolve(import.meta.dirname, "..");
  const prompt = await readFile(
    path.join(repository, "examples/pi/tool-loop/prompt.txt"),
    "utf8",
  );
  for (const instruction of [
    "Make exactly four tool calls and no others, in this exact order: read, bash, write, bash.",
    "node verify.mjs --precheck",
    "write result.txt",
    "node verify.mjs",
    "Do not use edit or retry any tool call.",
  ]) {
    assert.ok(prompt.includes(instruction), `missing prompt contract: ${instruction}`);
  }
  assert.equal(
    await readFile(
      path.join(repository, "examples/pi/tool-loop/seed/source.txt"),
      "utf8",
    ),
    "alpha=7\nbeta=11\n",
  );
  assert.equal(
    await readFile(
      path.join(repository, "examples/pi/tool-loop/seed/result.txt"),
      "utf8",
    ),
    "sum=pending\n",
  );
  assert.equal(
    await readFile(
      path.join(repository, "examples/pi/tool-loop/expected/result.txt"),
      "utf8",
    ),
    "sum=18\n",
  );
  const verify = await readFile(
    path.join(repository, "examples/pi/tool-loop/seed/verify.mjs"),
    "utf8",
  );
  assert.ok(verify.includes('"sum=pending\\n"'));
  assert.ok(verify.includes('"sum=18\\n"'));

  for (const [filename, baseUrl] of [
    ["models.local.json", "http://127.0.0.1:11435/v1"],
    ["models.tailnet.json", "https://loxa-node.invalid/v1"],
  ]) {
    const config = JSON.parse(
      await readFile(path.join(repository, "examples/pi", filename), "utf8"),
    );
    assert.deepEqual(Object.keys(config.providers), ["loxa"]);
    const provider = config.providers.loxa;
    assert.equal(provider.baseUrl, baseUrl);
    assert.equal(provider.api, "openai-completions");
    assert.equal(provider.apiKey, "loxa-dummy-key");
    assert.equal(provider.models.length, 1);
    assert.deepEqual(provider.models[0], {
      id: "loxa",
      name: "Loxa",
      reasoning: false,
      input: ["text"],
      contextWindow: 8192,
    });
  }
});

test("committed acceptance gate advances only after each exact successful result", async () => {
  const { createAcceptanceGate } = await import(
    "../examples/pi/tool-loop/acceptance-gate.mjs"
  );
  const gate = createAcceptanceGate();
  const steps = [
    ["read-1", "read", { path: "source.txt" }],
    ["bash-1", "bash", { command: "node verify.mjs --precheck" }],
    ["write-1", "write", { path: "result.txt", content: "sum=18\n" }],
    ["bash-2", "bash", { command: "node verify.mjs" }],
  ];
  for (const [toolCallId, toolName, input] of steps) {
    assert.equal(await gate({ toolCallId, toolName, input }), undefined);
    assert.deepEqual(
      await gate({
        toolCallId: `${toolCallId}-concurrent`,
        toolName,
        input,
      }),
      {
        block: true,
        reason: "Pi acceptance tool call is not the next exact step.",
      },
    );
    assert.equal(
      await gate.toolResult({
        toolCallId,
        toolName,
        input,
        isError: false,
      }),
      undefined,
    );
  }
  assert.deepEqual(
    await gate({
      toolCallId: "extra",
      toolName: "read",
      input: { path: "source.txt" },
    }),
    {
      block: true,
      reason: "Pi acceptance tool call is not the next exact step.",
    },
  );
});

test("bridge consumes only a prebuilt isolated environment", async () => {
  await isolatedBridgeOptions(async (options) => {
    assert.equal(
      validateIsolatedEnvironment(options.environment, options.cwd, "darwin"),
      true,
    );
    for (const mutation of [
      { PI_OFFLINE: "0" },
      { PI_CODING_AGENT_DIR: "relative" },
      { HOME: "/outside/home", USERPROFILE: "/outside/home" },
      { OPENAI_API_KEY: "must-not-leak" },
    ]) {
      assert.throws(
        () =>
          validateIsolatedEnvironment(
            { ...options.environment, ...mutation },
            options.cwd,
            "darwin",
          ),
        /isolated|absolute|unapproved/i,
      );
    }
  });
});

test("bridge environment validator accepts Node's actual process.env record", async () => {
  await isolatedBridgeOptions(async (options) => {
    await withActualProcessEnvironment(options.environment, (environment) => {
      assert.notEqual(Object.getPrototypeOf(environment), Object.prototype);
      assert.equal(
        validateIsolatedEnvironment(environment, options.cwd, "darwin"),
        true,
      );
    });
  });
});

test("bridge default environment accepts actual process.env without spawning Pi", async () => {
  await isolatedBridgeOptions(async (options) => {
    const isolatedEnvironment = options.environment;
    delete options.environment;
    await withActualProcessEnvironment(isolatedEnvironment, async () => {
      const capture = {};
      const result = await runQualifiedPiBridge(options, {
        platform: "darwin",
        spawnProcess: fakeSpawn(
          { lines: successfulQualifiedLines() },
          capture,
        ),
      });

      assert.deepEqual(result, {
        schemaVersion: 1,
        toolTrace: successfulTrace(),
      });
      assert.equal(capture.options.env.OPENAI_API_KEY, undefined);
      assert.deepEqual(capture.options.env, isolatedEnvironment);
    });
  });
});

test("bridge accepts macOS-injected text encoding in actual default environment", async () => {
  await isolatedBridgeOptions(async (options) => {
    const isolatedEnvironment = {
      ...options.environment,
      __CF_USER_TEXT_ENCODING: "0x1F5:0x0:0x0",
    };
    delete options.environment;
    await withActualProcessEnvironment(isolatedEnvironment, async () => {
      const capture = {};
      const result = await runQualifiedPiBridge(options, {
        platform: "darwin",
        spawnProcess: fakeSpawn(
          { lines: successfulQualifiedLines() },
          capture,
        ),
      });

      assert.deepEqual(result, {
        schemaVersion: 1,
        toolTrace: successfulTrace(),
      });
      assert.equal(
        capture.options.env.__CF_USER_TEXT_ENCODING,
        "0x1F5:0x0:0x0",
      );
    });
  });
});

test("bridge environment rejects non-record containers and invalid values", async () => {
  await isolatedBridgeOptions(async (options) => {
    for (const environment of [
      null,
      Object.assign([], options.environment),
      Object.assign(new Date(0), options.environment),
      Object.assign(new Map(), options.environment),
    ]) {
      assert.throws(
        () => validateIsolatedEnvironment(environment, options.cwd, "darwin"),
        /environment/i,
      );
    }
    for (const mutation of [
      { PATH: 42 },
      { LANG: "en_US.UTF-8\0PRIVATE" },
      { __CF_USER_TEXT_ENCODING: 42 },
      { __CF_USER_TEXT_ENCODING: "0x1F5:0x0:0x0\0PRIVATE" },
    ]) {
      assert.throws(
        () =>
          validateIsolatedEnvironment(
            { ...options.environment, ...mutation },
            options.cwd,
            "darwin",
          ),
        /environment|invalid/i,
      );
    }
  });
});

test("bridge accepts the exact Windows profile and rejects extra keys", () => {
  const environment = {
    PATH: String.raw`C:\Windows\System32`,
    SYSTEMROOT: String.raw`C:\Windows`,
    COMSPEC: String.raw`C:\Windows\System32\cmd.exe`,
    HOME: String.raw`C:\Temp\run\home`,
    USERPROFILE: String.raw`C:\Temp\run\home`,
    HOMEDRIVE: "C:",
    HOMEPATH: String.raw`\Temp\run\home`,
    APPDATA: String.raw`C:\Temp\run\home\AppData\Roaming`,
    LOCALAPPDATA: String.raw`C:\Temp\run\home\AppData\Local`,
    XDG_CONFIG_HOME: String.raw`C:\Temp\run\home\.config`,
    XDG_CACHE_HOME: String.raw`C:\Temp\run\home\.cache`,
    XDG_DATA_HOME: String.raw`C:\Temp\run\home\.local\share`,
    TEMP: String.raw`C:\Temp\run\tmp`,
    TMP: String.raw`C:\Temp\run\tmp`,
    PI_CODING_AGENT_DIR: String.raw`C:\Temp\run\pi-config`,
    PI_OFFLINE: "1",
    PI_TELEMETRY: "0",
    PI_SKIP_VERSION_CHECK: "1",
  };
  assert.equal(
    validateIsolatedEnvironment(
      environment,
      String.raw`C:\Temp\run\workspace`,
      "win32",
    ),
    true,
  );
  assert.throws(
    () =>
      validateIsolatedEnvironment(
        { ...environment, OPENAI_API_KEY: "must-not-leak" },
        String.raw`C:\Temp\run\workspace`,
        "win32",
      ),
    /unapproved/i,
  );
  assert.throws(
    () =>
      validateIsolatedEnvironment(
        {
          ...environment,
          __CF_USER_TEXT_ENCODING: "0x1F5:0x0:0x0",
        },
        String.raw`C:\Temp\run\workspace`,
        "win32",
      ),
    /unapproved/i,
  );
});

test("bridge emits only bounded schema and semantic tool trace", async () => {
  await isolatedBridgeOptions(async (options) => {
    const capture = {};
    const result = await runQualifiedPiBridge(options, {
      platform: "darwin",
      spawnProcess: fakeSpawn({ lines: successfulQualifiedLines() }, capture),
    });
    assert.deepEqual(result, {
      schemaVersion: 1,
      toolTrace: successfulTrace(),
    });
    assert.deepEqual(Object.keys(result), ["schemaVersion", "toolTrace"]);
    assert.equal(JSON.stringify(result).includes("private"), false);
    assert.equal(capture.program, process.execPath);
    assert.equal(capture.argv[0], options.piEntrypoint);
    assert.deepEqual(
      capture.argv.slice(1),
      buildQualifiedPiArgv(options.extension, options.prompt),
    );
    assert.equal(capture.options.cwd, options.cwd);
    assert.equal(capture.options.shell, false);
    assert.deepEqual(capture.options.stdio, ["ignore", "pipe", "pipe"]);
  });
});

test("qualified Pi JSONL maps exact correlated successful tools", () => {
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
  assert.deepEqual(adaptQualifiedPiJsonl(lines), successfulTrace());
});

test("qualified Pi JSONL rejects concurrency extra tools and edit", () => {
  const concurrent = successfulQualifiedLines();
  concurrent.splice(
    2,
    0,
    JSON.stringify({
      type: "tool_execution_start",
      toolCallId: "bash-1",
      toolName: "bash",
      args: { command: "node verify.mjs --precheck" },
    }),
  );
  assert.throws(() => adaptQualifiedPiJsonl(concurrent), /concurrent tool/i);
  assert.throws(
    () => adaptQualifiedPiJsonl(successfulQualifiedLines("edit")),
    /tool correlation/i,
  );
  assert.throws(
    () =>
      validateSemanticToolTrace([
        ...successfulTrace(),
        { tool: "read", status: "success" },
      ]),
    /tool trace/i,
  );
});

test("qualified Pi JSONL rejects malformed private payloads without leaking them", () => {
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
    [JSON.stringify({ type: "unknown_PRIVATE_event" }), /unknown Pi JSONL/i],
  ];
  for (const [line, expected] of rejected) {
    assert.throws(
      () => adaptQualifiedPiJsonl([line]),
      (error) => expected.test(error.message) && !error.message.includes("PRIVATE"),
    );
  }
});

test("qualified Pi JSONL requires session terminal lifecycle and no pending calls", () => {
  assert.throws(
    () =>
      adaptQualifiedPiJsonl(
        successfulQualifiedLines().filter(
          (line) => JSON.parse(line).type !== "agent_end",
        ),
      ),
    /agent_end/i,
  );
  assert.throws(
    () => adaptQualifiedPiJsonl(successfulQualifiedLines().slice(0, -1)),
    /agent_settled/i,
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

test("qualified Pi entrypoint resolves absolutely and pins 0.82.1 through Node", async () => {
  const calls = [];
  assert.deepEqual(
    await qualifyPiEntrypoint("/opt/pi/dist/cli.js", {
      nodeExecutable: "/usr/bin/node",
      resolveEntrypoint: async () => "/opt/pi/dist/cli.js",
      readVersion: async (program, argv) => {
        calls.push([program, argv]);
        return "0.82.1\n";
      },
    }),
    {
      program: "/usr/bin/node",
      prefixArgs: ["/opt/pi/dist/cli.js"],
    },
  );
  assert.deepEqual(calls, [
    ["/usr/bin/node", ["/opt/pi/dist/cli.js", "--version"]],
  ]);
  for (const [resolved, version] of [
    ["pi", "0.82.1\n"],
    ["/opt/pi/dist/cli.js", "0.82.0\n"],
    ["/opt/pi/pi.cmd", "0.82.1\n"],
  ]) {
    await assert.rejects(
      qualifyPiEntrypoint("/opt/pi/dist/cli.js", {
        nodeExecutable: "/usr/bin/node",
        resolveEntrypoint: async () => resolved,
        readVersion: async () => version,
      }),
      /qualified Pi CLI entrypoint/i,
    );
  }
});

test("Pi version probe bounds hangs and oversized output", async () => {
  for (const scenario of [{ hang: true }, { output: "x".repeat(4097) }]) {
    const kills = [];
    await assert.rejects(
      readPiVersion("/usr/bin/node", ["/opt/pi/dist/cli.js", "--version"], {
        spawnProcess: () => {
          const child = new EventEmitter();
          child.stdout = new PassThrough();
          child.kill = (signal) => {
            kills.push(signal);
            return true;
          };
          queueMicrotask(() => {
            if (scenario.output) child.stdout.write(scenario.output);
          });
          return child;
        },
        timeoutMs: 10,
        forceKillDelayMs: 0,
        terminalTimeoutMs: 10,
      }),
      /qualified Pi executable verification failed/i,
    );
    assert.deepEqual(kills, ["SIGTERM", "SIGKILL"]);
  }
});

test("bridge times out and requests owned process-tree termination", async () => {
  await isolatedBridgeOptions(async (options) => {
    options.processTimeoutMs = 10;
    const capture = {};
    await assert.rejects(
      runQualifiedPiBridge(options, {
        platform: "darwin",
        spawnProcess: fakeSpawn({ hang: true }, capture),
      }),
      /timed out|terminate/i,
    );
    assert.deepEqual(capture.kills.slice(0, 2), ["SIGTERM", "SIGKILL"]);
    assert.equal(capture.options.detached, false);
  });
});

test("bridge cooperatively terminates the exact Pi child in Rust's inherited group", async () => {
  await isolatedBridgeOptions(async (options) => {
    options.processTimeoutMs = 10;
    const capture = {};
    await assert.rejects(
      runQualifiedPiBridge(options, {
        platform: "darwin",
        spawnProcess: fakeSpawn({ hang: true, pid: 5151 }, capture),
      }),
      /timed out|terminate/i,
    );
    assert.deepEqual(capture.kills, ["SIGTERM", "SIGKILL"]);
    assert.equal(capture.options.detached, false);
  });
});

test("bridge settles by its terminal deadline when descendants hold pipes", async () => {
  await isolatedBridgeOptions(async (options) => {
    options.processTimeoutMs = 10;
    const capture = {};
    const started = Date.now();
    await assert.rejects(
      runQualifiedPiBridge(options, {
        platform: "darwin",
        spawnProcess: fakeSpawn({ hang: true, pid: 6161 }, capture),
      }),
      /timed out|terminate/i,
    );
    assert.ok(Date.now() - started < 1000);
    assert.equal(capture.child.stdout.destroyed, true);
    assert.equal(capture.child.stderr.destroyed, true);
  });
});

test("bridge handles asynchronous Windows tree-killer failure privately", async () => {
  await isolatedBridgeOptions(async (options) => {
    options.piEntrypoint = String.raw`C:\Temp\run\node_modules\@mariozechner\pi-coding-agent\dist\cli.js`;
    options.extension = String.raw`C:\Temp\run\acceptance-gate.mjs`;
    options.cwd = String.raw`C:\Temp\run\workspace`;
    options.environment = {
      PATH: String.raw`C:\Windows\System32`,
      SYSTEMROOT: String.raw`C:\Windows`,
      COMSPEC: String.raw`C:\Windows\System32\cmd.exe`,
      HOME: String.raw`C:\Temp\run\home`,
      USERPROFILE: String.raw`C:\Temp\run\home`,
      HOMEDRIVE: "C:",
      HOMEPATH: String.raw`\Temp\run\home`,
      APPDATA: String.raw`C:\Temp\run\home\AppData\Roaming`,
      LOCALAPPDATA: String.raw`C:\Temp\run\home\AppData\Local`,
      XDG_CONFIG_HOME: String.raw`C:\Temp\run\home\.config`,
      XDG_CACHE_HOME: String.raw`C:\Temp\run\home\.cache`,
      XDG_DATA_HOME: String.raw`C:\Temp\run\home\.local\share`,
      TEMP: String.raw`C:\Temp\run\tmp`,
      TMP: String.raw`C:\Temp\run\tmp`,
      PI_CODING_AGENT_DIR: String.raw`C:\Temp\run\pi-config`,
      PI_OFFLINE: "1",
      PI_TELEMETRY: "0",
      PI_SKIP_VERSION_CHECK: "1",
    };
    options.processTimeoutMs = 10;
    const capture = {};
    await assert.rejects(
      runQualifiedPiBridge(options, {
        platform: "win32",
        spawnProcess: fakeSpawn(
          { hang: true, pid: 7171, ignoreAllKills: true },
          capture,
        ),
        spawnTreeKiller: () => {
          const killer = new EventEmitter();
          killer.unref = () => {};
          queueMicrotask(() => killer.emit("error", new Error("PRIVATE")));
          return killer;
        },
        taskkillExecutable: String.raw`C:\Windows\System32\taskkill.exe`,
      }),
      (error) =>
        /cleanup failed/i.test(error.message) &&
        !error.message.includes("PRIVATE"),
    );
    assert.deepEqual(capture.kills, ["SIGTERM", "SIGKILL"]);
  });
});

test("Windows taskkill is bounded when its helper never settles", async () => {
  const killer = new EventEmitter();
  const kills = [];
  killer.unref = () => {};
  killer.kill = (signal) => {
    kills.push(signal);
    return true;
  };
  const childKills = [];
  await assert.rejects(
    terminateOwnedProcessTree({
      child: {
        pid: 42,
        kill: (signal) => {
          childKills.push(signal);
          return true;
        },
      },
      platform: "win32",
      signal: "SIGTERM",
      signalProcess: () => assert.fail("POSIX signal must not run"),
      spawnTreeKiller: () => killer,
      taskkillExecutable: "C:\\Windows\\System32\\taskkill.exe",
      terminalTimeoutMs: 10,
    }),
    /cleanup failed/i,
  );
  assert.deepEqual(kills, ["SIGKILL"]);
  assert.deepEqual(childKills, ["SIGTERM"]);
});

test("Windows taskkill nonzero exit falls back to the exact child", async () => {
  const killer = new EventEmitter();
  killer.unref = () => {};
  const childKills = [];
  const cleanup = terminateOwnedProcessTree({
    child: {
      pid: 42,
      kill: (signal) => {
        childKills.push(signal);
        return true;
      },
    },
    platform: "win32",
    signal: "SIGTERM",
    signalProcess: () => assert.fail("POSIX signal must not run"),
    spawnTreeKiller: () => killer,
    taskkillExecutable: "C:\\Windows\\System32\\taskkill.exe",
  });
  queueMicrotask(() => killer.emit("close", 1));

  await assert.rejects(cleanup, /cleanup failed/i);
  assert.deepEqual(childKills, ["SIGTERM"]);
});
