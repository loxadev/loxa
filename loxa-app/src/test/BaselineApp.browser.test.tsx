import { act } from "react";
import { expect, test } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import { applyTheme, writeThemePreference, type ThemeMode } from "@/settings/theme";
import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

async function waitForThemeTransition() {
  const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
  await nextFrame();
  await nextFrame();
  while (true) {
    const running = document.getAnimations().filter(({ playState }) => playState !== "finished");
    if (running.length === 0) return;
    await Promise.all(running.map((animation) => animation.finished.catch(() => undefined)));
    await nextFrame();
  }
}

test("captures the untouched desktop shell in fixed light and dark modes", async () => {
  await page.viewport(800, 600);
  const { host } = mountBrowser(<App services={createAppServicesFixture()} />);
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
  await expect.element(page.getByText("Node ready — no model loaded")).toBeVisible();
  const [instrumentSansFaces, ibmPlexMonoFaces] = await Promise.all([
    document.fonts.load(`600 48px "Instrument Sans"`, "Node"),
    document.fonts.load(`500 12px "IBM Plex Mono"`, "LOCAL RUNTIME"),
  ]);
  await document.fonts.ready;
  expect(instrumentSansFaces.length).toBeGreaterThan(0);
  expect(ibmPlexMonoFaces.length).toBeGreaterThan(0);
  expect(document.fonts.check(`600 48px "Instrument Sans"`, "Node")).toBe(true);
  expect(document.fonts.check(`500 12px "IBM Plex Mono"`, "LOCAL RUNTIME")).toBe(true);
  expect(getComputedStyle(document.querySelector("h1") as HTMLElement).fontFamily).toContain("Instrument Sans");
  expect(getComputedStyle(document.querySelector(".eyebrow") as HTMLElement).fontFamily).toContain("IBM Plex Mono");

  for (const mode of ["light", "dark"] satisfies ThemeMode[]) {
    writeThemePreference(window.localStorage, mode);
    applyTheme(document.documentElement, mode, false);
    host.dataset.loxaTheme = mode;
    await waitForThemeTransition();
    await expectNoAxeViolations(document);
    await expect(document.body).toMatchScreenshot(`baseline-shell-${mode}-800x600`, {
      comparatorName: "pixelmatch",
      comparatorOptions: {
        allowedMismatchedPixelRatio: 0.005,
        includeAA: false,
        threshold: 0.2,
      },
      screenshotOptions: {
        animations: "disabled",
        caret: "hide",
        scale: "css",
      },
    });
  }
});
