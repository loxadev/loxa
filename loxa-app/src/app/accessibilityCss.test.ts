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
    expect(tokens).toContain("--loxa-motion-theme: 0.01ms");
    expect(css).toContain("background: Highlight");
  });

  it("applies the canonical theme transition to scoped theme-bearing properties", () => {
    expect(css).toContain("transition-property: color, background-color, border-color, outline-color, fill, stroke");
    expect(css).toContain("transition-duration: var(--loxa-motion-theme)");
    expect(css).toContain("transition-timing-function: var(--loxa-motion-easing)");
    expect(css).toMatch(/\.app-shell svg,\s*\.app-shell svg \*/);
  });

  it("keeps theme choices at 44px with focus, contrast, and forced-color coverage", () => {
    expect(css).toMatch(/\.theme-option[^}]*min-height: var\(--loxa-component-minimum-interactive-target\)/s);
    expect(css).toContain(".theme-option:has(input:focus-visible)");
    expect(css).toContain("outline: 2px solid var(--loxa-focus)");
    expect(css).toMatch(/@media \(prefers-contrast: more\)[\s\S]*\.theme-option/);
    expect(css).toMatch(/@media \(forced-colors: active\)[\s\S]*\.theme-option/);
    expect(css).toMatch(/@media \(prefers-reduced-motion: reduce\)/);
  });

  it("does not introduce a component palette or visual gimmicks", () => {
    expect(css).not.toMatch(/#[0-9a-f]{3,8}/i);
    expect(css).not.toMatch(/gradient|backdrop-filter|box-shadow/i);
  });

  it("keeps model controls readable, keyboard-sized, and visible in forced colors", () => {
    expect(css).toMatch(/\.model-row[^}]*display: grid/s);
    expect(css).toMatch(/\.model-actions button[^}]*min-height: var\(--loxa-component-minimum-interactive-target\)/s);
    expect(css).toMatch(/@media \(prefers-contrast: more\)[\s\S]*\.model-row/);
    expect(css).toMatch(/@media \(forced-colors: active\)[\s\S]*\.state-chip/);
  });

  it("pins the chat composer to the bottom of its independently scrolling screen", () => {
    expect(css).toMatch(/\.chat-screen[^}]*min-height: 100%/s);
    expect(css).toMatch(/\.chat-screen \.composer[^}]*position: sticky[^}]*bottom: 0/s);
  });
});
