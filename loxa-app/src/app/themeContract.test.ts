/// <reference types="node" />

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { parseConfigFileTextToJson } from "typescript";
import { describe, expect, it } from "vitest";

const root = process.cwd();
const readOrEmpty = (path: string) => (existsSync(path) ? readFileSync(path, "utf8") : "");
const readJson = (path: string) => {
  return (parseConfigFileTextToJson(path, readOrEmpty(path)).config ?? {}) as {
    compilerOptions?: { baseUrl?: string; paths?: Record<string, string[]> };
  };
};

describe("canonical theme foundation", () => {
  const indexCss = readOrEmpty(resolve(root, "src/index.css"));
  const themeCss = readOrEmpty(resolve(root, "src/styles/theme.css"));
  const fontsCss = readOrEmpty(resolve(root, "src/styles/fonts.css"));
  const viteSource = readOrEmpty(resolve(root, "vite.config.ts"));

  it("loads Tailwind theme and utilities without Preflight", () => {
    expect(indexCss).toContain('@import "tailwindcss/theme.css" layer(theme)');
    expect(indexCss).toContain('@import "tailwindcss/utilities.css" layer(utilities)');
    expect(indexCss).not.toContain('@import "tailwindcss"');
  });

  it("maps Tailwind semantics to Loxa theme tokens and its attribute dark mode", () => {
    expect(themeCss).toContain("@theme inline");
    expect(themeCss).toContain("--color-background: var(--loxa-background)");
    expect(themeCss).toContain('[data-loxa-theme="dark"]');
    expect(indexCss).toContain('@custom-variant dark (&:where([data-loxa-theme="dark"], [data-loxa-theme="dark"] *))');
  });

  it("configures Vite and both TypeScript projects with the source alias", () => {
    expect(viteSource).toContain("tailwindcss()");
    for (const configPath of ["tsconfig.json", "tsconfig.node.json"]) {
      const config = readJson(resolve(root, configPath));
      expect(config.compilerOptions?.baseUrl, configPath).toBe(".");
      expect(config.compilerOptions?.paths, configPath).toEqual({ "@/*": ["./src/*"] });
    }
  });

  it("loads only the three bundled font assets", () => {
    expect([...fontsCss.matchAll(/@font-face/g)]).toHaveLength(3);
    for (const asset of [
      "../assets/fonts/InstrumentSans-Variable.woff2",
      "../assets/fonts/IBMPlexMono-Regular.woff2",
      "../assets/fonts/IBMPlexMono-Medium.woff2",
    ]) {
      expect(fontsCss).toContain(`url("${asset}")`);
    }
    expect(fontsCss).not.toMatch(/url\(["']?https?:/i);
  });
});
