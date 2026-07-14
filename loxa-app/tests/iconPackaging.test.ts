import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";

import { describe, expect, test } from "vitest";

const catalog = "icons/AppIcon.appiconset";
const expectedIcons = [
  "icon_16x16.png",
  "icon_16x16@2x.png",
  "icon_32x32.png",
  "icon_32x32@2x.png",
  "icon_128x128.png",
  "icon_128x128@2x.png",
  "icon_256x256.png",
  "icon_256x256@2x.png",
  "icon_512x512.png",
  "icon_512x512@2x.png",
] as const;

const expectedHashes: Record<(typeof expectedIcons)[number], string> = {
  "icon_16x16.png": "e07d8b0e8bb67c8cc87101f0c995ebc3af274e1461c5c4e7f6e448e581e235c9",
  "icon_16x16@2x.png": "08f37eec53b81205ae20874c4cbf37bf1fda9fef914fb201078021af21a45fd8",
  "icon_32x32.png": "08f37eec53b81205ae20874c4cbf37bf1fda9fef914fb201078021af21a45fd8",
  "icon_32x32@2x.png": "74ba940a76241338a8815b2de167b53cf6159be8eb08bfc037ba24e60d526663",
  "icon_128x128.png": "703ffc71522e8f3ca76de0bb560bed64de9dc34c0070780f8cc4dbcbc8fed16c",
  "icon_128x128@2x.png": "4ec1f4968e4f572fd2f2637b67f9b3c93678f1eaf769c27dea92a8b858ca4272",
  "icon_256x256.png": "4ec1f4968e4f572fd2f2637b67f9b3c93678f1eaf769c27dea92a8b858ca4272",
  "icon_256x256@2x.png": "afb6de486e675f99e76d57131fcba063299e10571c47923bdb7109a2db455c90",
  "icon_512x512.png": "afb6de486e675f99e76d57131fcba063299e10571c47923bdb7109a2db455c90",
  "icon_512x512@2x.png": "f5853da72cbce118f13139f006e9e0da284b4c033b157f45457ab2793765581c",
};

describe("macOS icon packaging", () => {
  test("packages every byte-identical canonical catalog slot", async () => {
    const tauriRoot = path.resolve(import.meta.dirname, "../src-tauri");
    const config = JSON.parse(await readFile(path.join(tauriRoot, "tauri.conf.json"), "utf8"));

    expect(config.bundle.icon).toEqual(expectedIcons.map((name) => `${catalog}/${name}`));

    for (const name of expectedIcons) {
      const bytes = await readFile(path.join(tauriRoot, catalog, name));
      expect(createHash("sha256").update(bytes).digest("hex"), name).toBe(expectedHashes[name]);
    }
  });
});
