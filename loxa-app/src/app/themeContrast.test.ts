/// <reference types="node" />

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

type Scheme = "light" | "dark";
type Pair = readonly [foreground: string, surface: string];

const themePath = resolve(process.cwd(), "src/styles/theme.css");
const themeCss = existsSync(themePath) ? readFileSync(themePath, "utf8") : "";

const normalTextPairs: Pair[] = [
  ["foreground", "background"],
  ["foreground", "surface"],
  ["foreground", "surface-subtle"],
  ["muted-foreground", "background"],
  ["muted-foreground", "surface"],
  ["muted-foreground", "surface-subtle"],
  ["selected-foreground", "selected-surface"],
  ["accent-foreground", "accent"],
  ["primary-foreground", "primary"],
  ["danger-foreground", "danger"],
  ["info-foreground", "info-surface"],
  ["success-foreground", "success-surface"],
  ["warning-foreground", "warning-surface"],
  ["danger-status-foreground", "danger-status-surface"],
];

const nonTextPairs: Pair[] = [
  ["control-border", "background"],
  ["focus", "background"],
  ["info-border", "info-surface"],
  ["info-icon", "info-surface"],
  ["success-border", "success-surface"],
  ["success-icon", "success-surface"],
  ["warning-border", "warning-surface"],
  ["warning-icon", "warning-surface"],
  ["danger-status-border", "danger-status-surface"],
  ["danger-status-icon", "danger-status-surface"],
];

describe("theme contrast", () => {
  it("explicitly covers every active semantic text pair", () => {
    for (const pair of [
      ["muted-foreground", "surface-subtle"],
      ["selected-foreground", "selected-surface"],
      ["accent-foreground", "accent"],
    ] satisfies Pair[]) {
      expect(normalTextPairs).toContainEqual(pair);
    }
  });

  for (const scheme of ["light", "dark"] satisfies Scheme[]) {
    it(`${scheme} normal-text pairs meet 4.5:1`, () => {
      assertContrast(scheme, normalTextPairs, 4.5);
    });

    it(`${scheme} status, border, icon, and focus pairs meet 3:1`, () => {
      assertContrast(scheme, nonTextPairs, 3);
    });
  }
});

function assertContrast(scheme: Scheme, pairs: Pair[], minimum: number) {
  for (const [foreground, surface] of pairs) {
    const foregroundValue = readHexToken(scheme, foreground);
    const surfaceValue = readHexToken(scheme, surface);
    expect(
      contrastRatio(foregroundValue, surfaceValue),
      `${scheme} --loxa-${foreground} on --loxa-${surface}`,
    ).toBeGreaterThanOrEqual(minimum);
  }
}

function readHexToken(scheme: Scheme, role: string) {
  const match = themeCss.match(new RegExp(`--loxa-${scheme}-${role}:\\s*(#[0-9a-f]{6})`, "i"));
  expect(match?.[1], `missing explicit --loxa-${scheme}-${role}`).toBeTruthy();
  return match?.[1] ?? "#000000";
}

function contrastRatio(left: string, right: string) {
  const [lighter, darker] = [relativeLuminance(left), relativeLuminance(right)].sort((a, b) => b - a);
  return (lighter + 0.05) / (darker + 0.05);
}

function relativeLuminance(hex: string) {
  const channels = hex
    .slice(1)
    .match(/.{2}/g)!
    .map((channel) => Number.parseInt(channel, 16) / 255)
    .map((channel) => (channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4));
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}
