import assert from "node:assert/strict";
import {
  chmod,
  cp,
  link,
  mkdtemp,
  readFile,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  QualificationRequiredError,
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

function successfulTrace(editTool = "edit") {
  return [
    { tool: "read", status: "success" },
    { tool: "bash", stage: "precheck", status: "success" },
    { tool: editTool, status: "success" },
    { tool: "bash", stage: "verification", status: "success" },
  ];
}

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
    validatePhaseEndpoint("windows-tailnet", "https://node-one.invalid/v1")
      .hostname,
    "node-one.invalid",
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

test("gateway preflight and postflight validators require model loxa and ready status", () => {
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
      engine: { name: "llama.cpp", version: "b10107" },
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
  assert.doesNotThrow(() => validateSemanticToolTrace(successfulTrace("edit")));
  assert.doesNotThrow(() => validateSemanticToolTrace(successfulTrace("write")));
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
      "--expected-config-sha256",
      "b".repeat(64),
      "--evidence-dir",
      "target/pi-acceptance/run",
    ]),
    {
      phase: "post-recovery",
      baseUrl: "http://127.0.0.1:11435/v1",
      piBin: "/opt/pi",
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
        "--expected-config-sha256",
        "not-a-digest",
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
      ]),
    /invalid/i,
  );
});

test("live Pi adapter fails truthfully until CLI flags and JSONL schema are qualified", async () => {
  await assert.rejects(
    runQualifiedPiAdapter(),
    (error) =>
      error instanceof QualificationRequiredError &&
      /qualification required/i.test(error.message),
  );
});
