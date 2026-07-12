/// <reference types="node" />

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const css = readFileSync(`${process.cwd()}/src/App.css`, "utf8");
const tokens = readFileSync(`${process.cwd()}/src/styles/loxa.css`, "utf8");

describe("accessibility CSS contract", () => {
  it("uses the canonical minimum target and visible focus token", () => {
    expect(tokens).toContain("--loxa-component-minimum-interactive-target: 44px");
    expect(tokens).toContain("outline: 2px solid var(--loxa-focus)");
    expect(css).toContain("min-height: var(--loxa-component-minimum-interactive-target)");
  });

  it("provides reduced-motion, increased-contrast, and forced-color behavior", () => {
    expect(css).toContain("@media (prefers-reduced-motion: reduce)");
    expect(css).toContain("@media (prefers-contrast: more)");
    expect(css).toContain("@media (forced-colors: active)");
    expect(css).toContain("transition-duration: 0.01ms !important");
    expect(css).toContain("background: Highlight");
  });

  it("does not introduce a component palette or visual gimmicks", () => {
    expect(css).not.toMatch(/#[0-9a-f]{3,8}/i);
    expect(css).not.toMatch(/gradient|backdrop-filter|box-shadow/i);
  });
});
