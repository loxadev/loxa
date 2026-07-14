import { describe, expect, it, vi } from "vitest";

import { cspProbeStore, serializeEvidence } from "./cspProbeStore";
import { installConsoleCountProbe } from "./consoleCountProbe";

function consoleTarget() {
  return {
    warn: vi.fn(function (this: unknown, ...args: unknown[]) {
      void args;
      return this;
    }),
    error: vi.fn(function (this: unknown, ...args: unknown[]) {
      void args;
      return this;
    }),
    log: vi.fn(),
    info: vi.fn(),
  };
}

describe("console count probe", () => {
  it("forwards original argument identity and receiver while retaining only counts", () => {
    cspProbeStore.reset();
    const target = consoleTarget();
    const warn = target.warn;
    const error = target.error;
    const secret = { token: "private-model-token", path: "/Users/alice/models/private.gguf" };
    const cleanup = installConsoleCountProbe(target);

    target.warn("do-not-retain", secret);
    target.error(secret);

    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]?.[1]).toBe(secret);
    expect(warn.mock.instances[0]).toBe(target);
    expect(error).toHaveBeenCalledOnce();
    expect(error.mock.calls[0]?.[0]).toBe(secret);
    expect(error.mock.instances[0]).toBe(target);
    expect(target.log).not.toHaveBeenCalled();
    expect(target.info).not.toHaveBeenCalled();
    expect(serializeEvidence()).toBe('{"schemaVersion":1,"cspViolations":[],"consoleCounts":{"warn":1,"error":1}}');
    expect(serializeEvidence()).not.toMatch(/do-not-retain|private-model-token|alice|private\.gguf/);

    cleanup();
    expect(target.warn).toBe(warn);
    expect(target.error).toBe(error);
  });

  it("rejects duplicate installation and never double-counts", () => {
    cspProbeStore.reset();
    const target = consoleTarget();
    const cleanup = installConsoleCountProbe(target);

    expect(() => installConsoleCountProbe(target)).toThrow(/already installed/i);
    target.warn("one call");
    expect(cspProbeStore.getEvidenceSnapshot().consoleCounts.warn).toBe(1);

    cleanup();
  });

  it("does not overwrite later console hooks during cleanup", () => {
    cspProbeStore.reset();
    const target = consoleTarget();
    const originalError = target.error;
    const cleanup = installConsoleCountProbe(target);
    const replacementWarn = vi.fn();
    target.warn = replacementWarn;

    cleanup();

    expect(target.warn).toBe(replacementWarn);
    expect(target.error).toBe(originalError);
  });

  it("keeps a newer installation registered when stale cleanup runs again", () => {
    cspProbeStore.reset();
    const target = consoleTarget();
    const cleanupA = installConsoleCountProbe(target);
    cleanupA();
    const cleanupB = installConsoleCountProbe(target);

    cleanupA();

    expect(() => installConsoleCountProbe(target)).toThrow(/already installed/i);
    target.warn("one call through installation B");
    expect(cspProbeStore.getEvidenceSnapshot().consoleCounts.warn).toBe(1);
    cleanupB();
  });
});
