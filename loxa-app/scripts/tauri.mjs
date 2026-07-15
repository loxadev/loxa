import { spawn } from "node:child_process";

import { buildTauriArgs, startFrontend } from "./dev-runtime.mjs";

const args = process.argv.slice(2);
const isDesktopDev = args[0] === "dev";
let frontend;
let child;

try {
  if (isDesktopDev) {
    await run("pnpm", ["prepare:sidecar", "--", "--dev"]);
    frontend = await startFrontend();
    console.info(`Starting Loxa desktop development at ${frontend.origin}`);
  }

  const origin = frontend?.origin ?? "http://127.0.0.1:1420";
  child = spawn("pnpm", ["exec", "tauri", ...buildTauriArgs(args, origin)], {
    env: isDesktopDev ? { ...process.env, LOXA_DEV_ORIGIN: origin } : process.env,
    stdio: "inherit",
  });

  process.once("SIGINT", () => child?.kill("SIGINT"));
  process.once("SIGTERM", () => child?.kill("SIGTERM"));

  const code = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (exitCode, signal) => resolve(exitCode ?? (signal ? 1 : 0)));
  });
  process.exitCode = code;
} catch (error) {
  console.error(`Unable to start Tauri: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
} finally {
  await frontend?.server.close();
}

function run(command, commandArgs) {
  return new Promise((resolve, reject) => {
    const subprocess = spawn(command, commandArgs, { stdio: "inherit" });
    subprocess.once("error", reject);
    subprocess.once("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited with ${code ?? signal ?? "an unknown status"}`));
    });
  });
}
