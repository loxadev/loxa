import { describe, expect, it, vi } from "vitest";

import { cspProbeStore } from "./cspProbeStore";

function violation(overrides: Record<string, unknown> = {}) {
  return {
    effectiveDirective: "img-src",
    blockedURI: "https://user:secret@example.com:8443/private/model?token=prompt#response",
    sourceFile: "file:///Users/alice/private/project/src/main.tsx?credential=secret#fragment",
    lineNumber: 12.9,
    columnNumber: 4,
    ...overrides,
  };
}

describe("CSP probe store", () => {
  it("publishes only sanitized immutable records", () => {
    cspProbeStore.reset();
    const before = cspProbeStore.getSnapshot();
    cspProbeStore.recordViolation(violation());
    const snapshot = cspProbeStore.getSnapshot();

    expect(snapshot).not.toBe(before);
    expect(snapshot).toEqual([
      {
        effectiveDirective: "img-src",
        blockedTarget: "https://example.com/[redacted]",
        sourceBasename: "main.tsx",
        line: 12,
        column: 4,
      },
    ]);
    expect(cspProbeStore.getSnapshot()).toBe(snapshot);
    const exported = cspProbeStore.exportJson();
    expect(exported).toBe(JSON.stringify(snapshot));
    expect(exported).not.toMatch(/user|secret|8443|private|token|prompt|response|alice|credential|fragment/);
  });

  it.each([
    ["inline", "inline"],
    ["eval", "eval"],
    ["data", "data"],
    ["data:image/png;base64,secret", "unknown"],
    ["file:///Users/alice/private/token.txt", "unknown"],
    ["/Users/alice/private/token.txt", "unknown"],
    ["mailto:secret@example.com", "unknown"],
    ["https:///", "unknown"],
    ["not a url", "unknown"],
  ])("reduces %s to %s", (blockedURI, blockedTarget) => {
    cspProbeStore.reset();
    cspProbeStore.recordViolation(violation({ blockedURI }));
    expect(cspProbeStore.getSnapshot()[0]?.blockedTarget).toBe(blockedTarget);
  });

  it.each([
    ["/Users/alice/project/main.tsx?secret=1", "main.tsx"],
    ["C:\\Users\\alice\\project\\bootstrap.ts#secret", "bootstrap.ts"],
  ])("keeps only the source basename from %s", (sourceFile, expected) => {
    cspProbeStore.reset();
    cspProbeStore.recordViolation(violation({ sourceFile }));
    expect(cspProbeStore.getSnapshot()[0]?.sourceBasename).toBe(expected);
  });

  it.each([
    "file:///Users/alice/private/?secret=1#fragment",
    "/Users/alice/private/#fragment",
    "C:\\Users\\alice\\private\\?secret=1#fragment",
  ])("rejects the trailing private directory in %s", (sourceFile) => {
    cspProbeStore.reset();
    cspProbeStore.recordViolation(violation({ sourceFile }));
    expect(cspProbeStore.getSnapshot()[0]?.sourceBasename).toBe("unknown");
    expect(cspProbeStore.exportJson()).not.toContain("private");
  });

  it("normalizes invalid coordinates without changing stable snapshots between mutations", () => {
    cspProbeStore.reset();
    const empty = cspProbeStore.getSnapshot();
    expect(cspProbeStore.getSnapshot()).toBe(empty);
    cspProbeStore.recordViolation(violation({ lineNumber: Number.POSITIVE_INFINITY, columnNumber: -5 }));
    const populated = cspProbeStore.getSnapshot();
    expect(populated[0]).toMatchObject({ line: 0, column: 0 });
    expect(cspProbeStore.getSnapshot()).toBe(populated);
  });

  it("notifies subscribers, supports unsubscribe and reset, and exports the snapshot", () => {
    cspProbeStore.reset();
    const listener = vi.fn();
    const unsubscribe = cspProbeStore.subscribe(listener);
    cspProbeStore.recordViolation(violation({ blockedURI: "inline", sourceFile: "" }));
    expect(listener).toHaveBeenCalledTimes(1);
    expect(JSON.parse(cspProbeStore.exportJson())).toEqual(cspProbeStore.getSnapshot());

    unsubscribe();
    cspProbeStore.reset();
    expect(listener).toHaveBeenCalledTimes(1);
    expect(cspProbeStore.getSnapshot()).toEqual([]);
  });
});
