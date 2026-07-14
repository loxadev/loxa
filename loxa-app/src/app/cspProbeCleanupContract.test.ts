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

describe("Task 9 CSP probe cleanup inventory", () => {
  it("keeps every production probe integration point explicit for final deletion", () => {
    const contents = productionProbeInventory.map((path) => [path, readFileSync(resolve(path), "utf8")] as const);
    expect(contents.find(([path]) => path === "index.html")?.[1]).toContain("cspProbeBootstrap.ts");
    expect(contents.find(([path]) => path === "src/main.tsx")?.[1]).toContain("CspProbePanel");
    expect(contents.find(([path]) => path === "src/styles/platform.css")?.[1]).toContain(".csp-probe-panel");
    expect(productionProbeInventory).toContain("src/app/consoleCountProbe.ts");
  });
});
