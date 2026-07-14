import { cspProbeStore } from "./cspProbeStore";

type ConsoleMethod = (...args: unknown[]) => void;

export type CapturedConsole = {
  warn: ConsoleMethod;
  error: ConsoleMethod;
};

const installations = new WeakSet<CapturedConsole>();

export function installConsoleCountProbe(target: CapturedConsole): () => void {
  if (installations.has(target)) throw new Error("Console count probe is already installed");

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
  installations.add(target);

  return () => {
    if (target.warn === wrappedWarn) target.warn = originalWarn;
    if (target.error === wrappedError) target.error = originalError;
    installations.delete(target);
  };
}
