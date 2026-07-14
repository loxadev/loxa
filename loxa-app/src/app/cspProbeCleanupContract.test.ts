// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

const productionProbeInventory = [
  "index.html",
  "src/main.tsx",
  "src/app/cspProbeBootstrap.ts",
  "src/app/cspProbeStore.ts",
  "src/app/consoleCountProbe.ts",
  "src/app/CspProbePanel.tsx",
  "src/styles/platform.css",
] as const;

const removalSentinels = {
  VITE_LOXA_CSP_PROBE: ["src/app/cspProbeBootstrap.ts", "src/main.tsx"],
  VITE_LOXA_CSP_PROBE_CASE: ["src/app/cspProbeBootstrap.ts"],
  cspProbeBootstrap: ["index.html"],
  cspProbeStore: ["src/app/cspProbeBootstrap.ts", "src/app/cspProbeStore.ts", "src/app/CspProbePanel.tsx"],
  CspProbePanel: ["src/app/CspProbePanel.tsx", "src/main.tsx"],
  consoleCountProbe: ["src/app/cspProbeBootstrap.ts"],
  installConsoleCountProbe: ["src/app/consoleCountProbe.ts", "src/app/cspProbeBootstrap.ts"],
  CspProbeEvidence: ["src/app/cspProbeStore.ts"],
  consoleCounts: ["src/app/cspProbeStore.ts", "src/app/CspProbePanel.tsx"],
  serializeEvidence: ["src/app/cspProbeStore.ts", "src/app/CspProbePanel.tsx"],
  getEvidenceSnapshot: ["src/app/cspProbeStore.ts", "src/app/CspProbePanel.tsx"],
  recordConsole: ["src/app/cspProbeStore.ts", "src/app/consoleCountProbe.ts"],
  clearViolations: ["src/app/cspProbeStore.ts", "src/app/CspProbePanel.tsx"],
  securitypolicyviolation: ["src/app/cspProbeBootstrap.ts"],
  "early-blocked-image": ["src/app/cspProbeBootstrap.ts"],
  "Sanitized probe JSON": ["src/app/CspProbePanel.tsx"],
  "Packaged probe evidence": ["src/app/CspProbePanel.tsx"],
  ".csp-probe-panel": ["src/styles/platform.css"],
  ".csp-probe-heading": ["src/styles/platform.css"],
  ".csp-probe-actions": ["src/styles/platform.css"],
  ".csp-probe-records": ["src/styles/platform.css"],
  ".csp-probe-json-label": ["src/styles/platform.css"],
  ".csp-probe-json": ["src/styles/platform.css"],
} as const;

describe("Task 9 CSP probe cleanup inventory", () => {
  it("keeps every production probe integration point explicit for final deletion", () => {
    const contents = new Map(productionProbeInventory.map((path) => [path, readFileSync(resolve(path), "utf8")]));

    for (const [sentinel, owners] of Object.entries(removalSentinels)) {
      for (const owner of owners) {
        expect(contents.get(owner), `${owner} must own the Task 9 sentinel ${sentinel}`).toContain(sentinel);
      }
    }

    expect(productionProbeInventory).toEqual([
      "index.html",
      "src/main.tsx",
      "src/app/cspProbeBootstrap.ts",
      "src/app/cspProbeStore.ts",
      "src/app/consoleCountProbe.ts",
      "src/app/CspProbePanel.tsx",
      "src/styles/platform.css",
    ]);
  });
});
