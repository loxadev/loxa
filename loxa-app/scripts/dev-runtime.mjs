import { createServer as createViteServer } from "vite";

export async function startFrontend({ preferredPort = 1420, configFile } = {}) {
  const server = await createViteServer({
    ...(configFile === false ? { configFile: false } : {}),
    server: {
      host: "127.0.0.1",
      port: preferredPort,
      strictPort: false,
    },
  });
  try {
    await server.listen();
    const address = server.httpServer?.address();
    if (!address || typeof address === "string" || address.address !== "127.0.0.1") {
      throw new Error("Vite did not bind to the expected IPv4 loopback address");
    }
    return { server, origin: `http://127.0.0.1:${address.port}` };
  } catch (error) {
    await server.close();
    throw error;
  }
}

export function buildDevConfig(origin) {
  assertLoopbackOrigin(origin);
  return {
    build: {
      devUrl: origin,
      beforeDevCommand: "",
    },
  };
}

export function buildTauriArgs(args, origin) {
  if (args[0] !== "dev") return [...args];
  return ["dev", "--config", JSON.stringify(buildDevConfig(origin)), ...args.slice(1)];
}

function assertLoopbackOrigin(origin) {
  if (!/^http:\/\/127\.0\.0\.1:(?:[1-9]\d{0,4})$/.test(origin)) {
    throw new Error("Development origin must be a canonical IPv4 loopback URL");
  }
  const port = Number(origin.slice(origin.lastIndexOf(":") + 1));
  if (!Number.isSafeInteger(port) || port > 65_535) {
    throw new Error("Development origin port is out of range");
  }
}
