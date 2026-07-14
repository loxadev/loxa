import { cspProbeStore } from "./cspProbeStore";

type ConsoleMethod = (...args: unknown[]) => void;

export type CapturedConsole = {
  warn: ConsoleMethod;
  error: ConsoleMethod;
};

const installations = new WeakMap<CapturedConsole, object>();

export function installConsoleCountProbe(target: CapturedConsole): () => void {
  if (installations.has(target)) throw new Error("Console count probe is already installed");

  const token = {};
  const originalWarn = target.warn;
  const originalError = target.error;
  const wrappedWarn: ConsoleMethod = (...args) => {
    cspProbeStore.recordConsole("warn");
    Reflect.apply(originalWarn, target, args);
  };
  const wrappedError: ConsoleMethod = (...args) => {
    cspProbeStore.recordConsole("error");
    Reflect.apply(originalError, target, args);
  };

  target.warn = wrappedWarn;
  target.error = wrappedError;
  installations.set(target, token);
  let cleaned = false;

  return () => {
    if (cleaned) return;
    cleaned = true;
    if (target.warn === wrappedWarn) target.warn = originalWarn;
    if (target.error === wrappedError) target.error = originalError;
    if (installations.get(target) === token) installations.delete(target);
  };
}
