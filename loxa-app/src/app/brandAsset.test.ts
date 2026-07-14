/// <reference types="node" />

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

describe("packaged brand asset", () => {
  it("emits the Loxa mark as a bundled file under strict img-src CSP", () => {
    const appSource = readFileSync(`${process.cwd()}/src/app/App.tsx`, "utf8");
    expect(appSource).toContain('import mark from "../assets/brand/loxa-mark.svg?no-inline"');
    expect(appSource).not.toContain('import mark from "../assets/brand/loxa-mark.svg";');
  });
});
