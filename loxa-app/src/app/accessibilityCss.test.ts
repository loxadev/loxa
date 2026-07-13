/// <reference types="node" />

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const root = process.cwd();
const globalCss = readFileSync(`${root}/src/App.css`, "utf8");
const tokens = readFileSync(`${root}/src/styles/loxa.css`, "utf8");
const featureModules = [
  "src/node/NodeScreen.module.css",
  "src/models/ModelsScreen.module.css",
  "src/chat/ChatScreen.module.css",
  "src/settings/SettingsScreen.module.css",
].map((path) => ({ path, css: readFileSync(`${root}/${path}`, "utf8") }));

describe("integrated accessibility CSS contract", () => {
  it("keeps the global sheet limited to shell, navigation, and shared primitives", () => {
    expect(globalCss).toContain(".app-shell");
    expect(globalCss).toContain(".navigation-rail");
    expect(globalCss).toContain(".screen-header");
    expect(globalCss).toContain(".interactive-target");
    for (const obsolete of [
      ".status-grid",
      ".status-field",
      ".chat-screen",
      ".chat-output",
      ".composer-model-control",
      ".model-row",
      ".model-list",
      ".settings-group",
      ".theme-option",
    ]) {
      expect(globalCss, `${obsolete} belongs in a feature module`).not.toContain(obsolete);
    }
  });

  it("resolves every feature variable through the canonical distributed token sheet", () => {
    const definitions = new Set(
      [...tokens.matchAll(/(--loxa-[a-z0-9-]+)\s*:/gi)].map((match) => match[1]),
    );
    for (const { path, css } of featureModules) {
      const references = [...css.matchAll(/var\((--loxa-[a-z0-9-]+)/gi)].map((match) => match[1]);
      expect(references.length, `${path} should use canonical tokens`).toBeGreaterThan(0);
      expect(references.filter((reference) => !definitions.has(reference)), path).toEqual([]);
    }
  });

  it("keeps every feature keyboard-sized and adaptive to accessibility preferences", () => {
    expect(tokens).toContain("--loxa-component-minimum-interactive-target: 44px");
    expect(tokens).toContain("outline: 2px solid var(--loxa-focus)");
    for (const { path, css } of featureModules) {
      expect(featureContractErrors(css, tokens), path).toEqual([]);
      expect(css, path).toContain("var(--loxa-component-minimum-interactive-target)");
      expect(css, path).toContain("@media (max-width: 760px)");
      expect(css, path).toContain("@media (prefers-reduced-motion: reduce)");
      expect(css, path).toContain("@media (prefers-contrast: more)");
      expect(css, path).toContain("@media (forced-colors: active)");
      expect(css, path).not.toMatch(/#[0-9a-f]{3,8}/i);
      expect(css, path).not.toMatch(/gradient|backdrop-filter|box-shadow/i);
    }
    expect(tokens).toMatch(/:focus-visible\s*\{[^}]*outline:\s*2px solid var\(--loxa-focus\)/s);
  });

  it("fails the contract when compact, semantic-token, or focus coverage regresses", () => {
    const module = featureModules[0].css;
    expect(featureContractErrors(module.replace("@media (max-width: 760px)", "@media (min-width: 761px)"), tokens))
      .toContain("missing compact-width rule");
    expect(featureContractErrors(module.replace("--loxa-foreground", "--loxa-color-ink"), tokens))
      .toContain("primitive or theme-implementation token");
    expect(tokens.replace(":focus-visible", ":focus")).not.toMatch(
      /:focus-visible\s*\{[^}]*outline:\s*2px solid var\(--loxa-focus\)/s,
    );
  });

  it("keeps shared theme transitions scoped and removable for reduced motion", () => {
    expect(globalCss).toContain("transition-property: color, background-color, border-color, outline-color, fill, stroke");
    expect(globalCss).toContain("transition-duration: var(--loxa-motion-theme)");
    expect(globalCss).toContain("transition-timing-function: var(--loxa-motion-easing)");
    expect(globalCss).toContain("@media (prefers-reduced-motion: reduce)");
  });
});

function featureContractErrors(css: string, tokenCss: string) {
  const errors: string[] = [];
  const definitions = new Set(
    [...tokenCss.matchAll(/(--loxa-[a-z0-9-]+)\s*:/gi)].map((match) => match[1]),
  );
  const references = [...css.matchAll(/var\((--loxa-[a-z0-9-]+)/gi)].map((match) => match[1]);
  if (!css.includes("@media (max-width: 760px)")) errors.push("missing compact-width rule");
  if (references.some((reference) => /--loxa-(?:color|light|dark)-/.test(reference))) {
    errors.push("primitive or theme-implementation token");
  }
  if (references.some((reference) => !definitions.has(reference))) errors.push("undefined canonical token");
  return errors;
}
