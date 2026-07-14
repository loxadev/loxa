/// <reference types="node" />

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const html = readFileSync(`${process.cwd()}/index.html`, "utf8");
const entry = readFileSync(`${process.cwd()}/src/settings/prepaint.ts`, "utf8");

describe("theme prepaint entrypoint", () => {
  it("invokes prepaint before the React entrypoint", () => {
    expect(entry).toContain('import { prepaintTheme } from "./themeRuntime"');
    expect(entry).toContain("prepaintTheme();");

    const probe = html.indexOf('src="/src/app/cspProbeBootstrap.ts"');
    const prepaint = html.indexOf('src="/src/settings/prepaint.ts"');
    const react = html.indexOf('src="/src/main.tsx"');
    expect(probe).toBeGreaterThan(-1);
    expect(prepaint).toBeGreaterThan(probe);
    expect(prepaint).toBeGreaterThan(-1);
    expect(react).toBeGreaterThan(prepaint);
  });
});
