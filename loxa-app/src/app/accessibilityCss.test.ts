/// <reference types="node" />

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const root = process.cwd();
const globalCss = readFileSync(`${root}/src/App.css`, "utf8");
const tokens = readFileSync(`${root}/src/styles/loxa.css`, "utf8");
const themeCss = readFileSync(`${root}/src/styles/theme.css`, "utf8");
const featureModules = [
  { path: "src/node/NodeScreen.module.css", files: ["src/node/NodeScreen.module.css"] },
  { path: "src/models/ModelsScreen.module.css", files: ["src/models/ModelsScreen.module.css"] },
  {
    path: "src/chat/*.module.css",
    files: ["src/chat/ChatScreen.module.css", "src/chat/ChatComposer.module.css", "src/chat/ChatTranscript.module.css"],
  },
  { path: "src/settings/SettingsScreen.module.css", files: ["src/settings/SettingsScreen.module.css"] },
].map(({ path, files }) => ({ path, css: files.map((file) => readFileSync(`${root}/${file}`, "utf8")).join("\n") }));

describe("integrated accessibility CSS contract", () => {
  it("keeps the global sheet limited to shell, navigation, and shared primitives", () => {
    expect(globalCss).toContain(".app-shell");
    expect(globalCss).toContain(".app-sidebar");
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

  it("owns one responsive canvas and page frame for every route", () => {
    expect(globalCss).toMatch(/\.workspace-canvas\s*\{[^}]*background:\s*var\(--loxa-background\)/s);
    expect(globalCss).toMatch(/\.workspace-frame\s*\{[^}]*width:\s*min\(100%,\s*1200px\)/s);
    expect(globalCss).toMatch(/\.workspace-frame\s*\{[^}]*margin-inline:\s*auto/s);
    expect(responsiveCanvasErrors(globalCss)).toEqual([]);
  });

  it("anchors the operational navigation group at the rail bottom", () => {
    expect(globalCss).toMatch(/\.conversation-rail-slot\s*\{[^}]*flex:\s*1\s+1\s+auto/s);
    expect(globalCss).toMatch(/\.sidebar-footer\s*\{[^}]*display:\s*grid/s);
    expect(globalCss).toMatch(
      /\.global-node-status\s*\{[^}]*min-height:\s*var\(--loxa-component-minimum-interactive-target\)/s,
    );
  });

  it("resolves every feature variable through the canonical distributed token sheet", () => {
    const definitions = new Set([...tokens.matchAll(/(--loxa-[a-z0-9-]+)\s*:/gi)].map((match) => match[1]));
    for (const { path, css } of featureModules) {
      const references = [...css.matchAll(/var\((--loxa-[a-z0-9-]+)/gi)].map((match) => match[1]);
      expect(references.length, `${path} should use canonical tokens`).toBeGreaterThan(0);
      expect(
        references.filter((reference) => !definitions.has(reference)),
        path,
      ).toEqual([]);
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
    expect(
      featureContractErrors(module.replace("@media (max-width: 760px)", "@media (min-width: 761px)"), tokens),
    ).toContain("missing compact-width rule");
    expect(featureContractErrors(module.replace("--loxa-foreground", "--loxa-color-ink"), tokens)).toContain(
      "primitive or theme-implementation token",
    );
    expect(tokens.replace(":focus-visible", ":focus")).not.toMatch(
      /:focus-visible\s*\{[^}]*outline:\s*2px solid var\(--loxa-focus\)/s,
    );
  });

  it("fails when either compact or wide canvas rule is removed from its media block", () => {
    const compactMutation = globalCss
      .replace(
        "@media (max-width: 760px) {",
        "@media (max-width: 760px) {\n  .workspace-canvas-removed { padding: var(--loxa-space-6); }",
      )
      .replace(/(@media \(max-width: 760px\) \{[\s\S]*?)\.workspace-canvas\s*\{[^}]*\}/, "$1");
    const wideMutation = globalCss.replace(
      /(@media \(min-width: 1440px\) \{[\s\S]*?)\.workspace-canvas\s*\{[^}]*\}/,
      "$1",
    );

    expect(responsiveCanvasErrors(compactMutation)).toContain("missing compact canvas gutter");
    expect(responsiveCanvasErrors(wideMutation)).toContain("missing wide canvas gutter");
  });

  it("keeps shared theme transitions scoped and removable for reduced motion", () => {
    expect(globalCss).toContain(
      "transition-property: color, background-color, border-color, outline-color, fill, stroke",
    );
    expect(globalCss).toContain("transition-duration: var(--loxa-motion-theme)");
    expect(globalCss).toContain("transition-timing-function: var(--loxa-motion-easing)");
    expect(globalCss).toContain("@media (prefers-reduced-motion: reduce)");
  });

  it("uses the compact native shell typography and semantic shell tokens", () => {
    expect(themeCss).toContain(
      '--loxa-font-sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    );
    expect(themeCss).toContain("--loxa-type-body-size: 14px");
    expect(themeCss).toContain("--loxa-type-h2-size: 24px");
    expect(globalCss).toContain("font-size: var(--loxa-type-body-size)");
    expect(globalCss).toContain("min-height: var(--loxa-component-minimum-interactive-target)");
    expect(globalCss).toContain("@media (prefers-contrast: more)");
    expect(globalCss).toContain("@media (forced-colors: active)");
    expect(globalCss).not.toMatch(/#[0-9a-f]{3,8}/i);
  });

  it("keeps backend and session ownership out of presentation-only shell modules", () => {
    for (const file of [
      "AppShell.tsx",
      "AppSidebar.tsx",
      "SidebarHeader.tsx",
      "SidebarNavigation.tsx",
      "SidebarRuntimeStatus.tsx",
      "SidebarResizeHandle.tsx",
    ]) {
      const source = readFileSync(`${root}/src/app/${file}`, "utf8");
      expect(source, file).not.toMatch(/\b(?:services|endpoint|ownership|token|credential|session)\b/i);
    }
  });
});

function featureContractErrors(css: string, tokenCss: string) {
  const errors: string[] = [];
  const definitions = new Set([...tokenCss.matchAll(/(--loxa-[a-z0-9-]+)\s*:/gi)].map((match) => match[1]));
  const references = [...css.matchAll(/var\((--loxa-[a-z0-9-]+)/gi)].map((match) => match[1]);
  if (!css.includes("@media (max-width: 760px)")) errors.push("missing compact-width rule");
  if (references.some((reference) => /--loxa-(?:color|light|dark)-/.test(reference))) {
    errors.push("primitive or theme-implementation token");
  }
  if (references.some((reference) => !definitions.has(reference))) errors.push("undefined canonical token");
  return errors;
}

function responsiveCanvasErrors(css: string) {
  const errors: string[] = [];
  const compact = mediaContents(css, "max-width: 760px");
  const wide = mediaContents(css, "min-width: 1440px");
  if (!/\.workspace-canvas\s*\{[^}]*padding:\s*var\(--loxa-space-6\)/s.test(compact)) {
    errors.push("missing compact canvas gutter");
  }
  if (!/\.workspace-canvas\s*\{[^}]*padding:\s*var\(--loxa-space-12\)/s.test(wide)) {
    errors.push("missing wide canvas gutter");
  }
  return errors;
}

function mediaContents(css: string, condition: string) {
  const marker = `@media (${condition})`;
  const start = css.indexOf(marker);
  if (start < 0) return "";
  const open = css.indexOf("{", start + marker.length);
  if (open < 0) return "";
  let depth = 1;
  for (let index = open + 1; index < css.length; index += 1) {
    if (css[index] === "{") depth += 1;
    if (css[index] === "}") depth -= 1;
    if (depth === 0) return css.slice(open + 1, index);
  }
  return "";
}
