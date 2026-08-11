import { createHash } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { createServer, createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

import { inspectFinalBundle } from "./runtime-bundle.mjs";

const MODEL_SIZE = 88202080;
const MODEL_SHA256 = "741ad12b64088fedc17c33aacb22e48be1972ef36a39f03666dd68bd15614fb9";
const MODEL_ALIAS = "loxa-runtime-smoke";
const MAX_LOG_BYTES = 2 * 1024 * 1024;

function invariant(condition, message) {
  if (!condition) throw new Error(message);
}

async function sha256(path) {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

async function verifyModel(path) {
  const metadata = await lstat(path).catch(() => null);
  invariant(metadata?.isFile() && metadata.nlink === 1, "small-model fixture must be a single-link regular file");
  invariant(metadata.size === MODEL_SIZE, "small-model fixture has the wrong size");
  invariant((metadata.mode & 0o111) === 0, "small-model fixture must not be executable");
  invariant((await sha256(path)) === MODEL_SHA256, "small-model fixture has the wrong SHA-256");
}

async function assignedLoopbackPort() {
  const server = createServer();
  await new Promise((accept, reject) => {
    server.once("error", reject);
    server.listen({ host: "127.0.0.1", port: 0, exclusive: true }, accept);
  });
  const address = server.address();
  invariant(address && typeof address !== "string", "could not obtain a loopback port");
  await new Promise((accept, reject) => server.close((error) => (error ? reject(error) : accept())));
  return address.port;
}

function exactEnvironment(home, temporary) {
  return {
    HOME: home,
    TMPDIR: temporary,
    PATH: "/usr/bin:/bin",
    LC_ALL: "C",
    NO_PROXY: "127.0.0.1,localhost",
    no_proxy: "127.0.0.1,localhost",
    HTTP_PROXY: "http://127.0.0.1:9",
    HTTPS_PROXY: "http://127.0.0.1:9",
    ALL_PROXY: "http://127.0.0.1:9",
  };
}

async function fetchJson(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(1000) });
  invariant(response.ok, `${url} returned HTTP ${response.status}`);
  return response.json();
}

async function waitForModels(child, port, logs) {
  const deadline = Date.now() + 45_000;
  let lastError = "not ready";
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`embedded server exited before readiness: ${logs.value}`);
    }
    try {
      const body = await fetchJson(`http://127.0.0.1:${port}/v1/models`);
      const ids = body?.data?.map((entry) => entry.id);
      invariant(Array.isArray(ids), "models response has no data array");
      invariant(ids.length === 1 && ids[0] === MODEL_ALIAS, "models response has the wrong alias");
      return ids;
    } catch (error) {
      lastError = error.message;
      await delay(100);
    }
  }
  throw new Error(`embedded server did not become ready: ${lastError}; ${logs.value}`);
}

async function portIsClosed(port) {
  return new Promise((accept) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    const finish = (closed) => {
      socket.destroy();
      accept(closed);
    };
    socket.once("connect", () => finish(false));
    socket.once("error", (error) => finish(error.code === "ECONNREFUSED"));
    socket.setTimeout(1000, () => finish(false));
  });
}

function processIsGone(pid) {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    return error.code === "ESRCH";
  }
}

function appendLogs(logs, chunk) {
  if (logs.value.length >= MAX_LOG_BYTES) return;
  logs.value += chunk.toString("utf8").slice(0, MAX_LOG_BYTES - logs.value.length);
}

async function stopProcessGroup(child, exitPromise) {
  let forcedKill = false;
  if (child.exitCode === null && child.signalCode === null) {
    try {
      process.kill(-child.pid, "SIGINT");
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  }
  let timer;
  let exit = await Promise.race([
    exitPromise,
    new Promise((accept) => {
      timer = setTimeout(() => accept(null), 10_000);
      timer.unref();
    }),
  ]);
  clearTimeout(timer);
  if (!exit) {
    forcedKill = true;
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
    exit = await exitPromise;
  }
  return { forcedKill, exit };
}

export async function smokeFinalRuntime(appPath, modelPath) {
  const app = resolve(appPath);
  const model = resolve(modelPath);
  const inventory = await inspectFinalBundle(app);
  const signature = spawnSync("/usr/bin/codesign", ["--verify", "--deep", "--strict", "--verbose=4", app], {
    encoding: "utf8",
    env: { HOME: "/nonexistent", PATH: "/usr/bin:/bin", LC_ALL: "C" },
  });
  invariant(signature.status === 0, `application signature is invalid: ${signature.stderr || signature.stdout}`);
  await verifyModel(model);

  const root = await mkdtemp(join(tmpdir(), "loxa-runtime-smoke-"));
  const home = join(root, "home");
  const port = await assignedLoopbackPort();
  const helper = join(app, "Contents/MacOS/llama-server");
  const logs = { value: "" };
  let child;
  let exitPromise;
  let modelIds;
  let stop = { forcedKill: false, exit: null };
  try {
    await mkdir(home, { mode: 0o700 });
    child = spawn(
      helper,
      [
        "--model", model,
        "--alias", MODEL_ALIAS,
        "--host", "127.0.0.1",
        "--cors-origins", "localhost",
        "--no-ui",
        "--port", String(port),
        "--ctx-size", "512",
        "--parallel", "1",
        "--threads", "2",
        "--n-gpu-layers", "all",
        "--fit", "off",
        "--jinja",
        "--reasoning", "off",
      ],
      {
        detached: true,
        env: exactEnvironment(home, root),
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    child.stdout.on("data", (chunk) => appendLogs(logs, chunk));
    child.stderr.on("data", (chunk) => appendLogs(logs, chunk));
    exitPromise = new Promise((accept, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => accept({ code, signal }));
    });
    modelIds = await waitForModels(child, port, logs);
  } finally {
    if (child && exitPromise) stop = await stopProcessGroup(child, exitPromise);
  }

  const listenerClosed = await portIsClosed(port);
  const processGone = processIsGone(child.pid);
  const homeEntries = await readdir(home);
  await rm(root, { recursive: true, force: true });
  invariant(stop.exit?.code === 0, `embedded server did not exit cleanly: ${JSON.stringify(stop.exit)}; ${logs.value}`);
  invariant(!stop.forcedKill, "embedded server required SIGKILL");
  invariant(listenerClosed, "embedded server listener remained reachable");
  invariant(processGone, "embedded server process remained alive");
  invariant(homeEntries.length === 0, "embedded server mutated the sanitized home");

  return {
    runtimeBuild: inventory.runtime.build,
    modelSha256: MODEL_SHA256,
    modelSize: MODEL_SIZE,
    modelIds,
    ready: true,
    forcedKill: stop.forcedKill,
    processGone,
    listenerClosed,
    homeEntries,
  };
}

async function main() {
  const args = process.argv.slice(2);
  invariant(args.length === 4 && args[0] === "--app" && args[2] === "--model", "usage: runtime-smoke.mjs --app /path/to/Loxa.app --model /path/to/model.gguf");
  const result = await smokeFinalRuntime(args[1], args[3]);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`Runtime smoke failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}
