import { act } from "react";
import { expect, test } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import { applyTheme, writeThemePreference, type ThemeMode } from "@/settings/theme";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

async function settleRenderedTheme() {
  await document.fonts.ready;
  await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  const animations = document.getAnimations().filter(({ playState }) => playState !== "finished");
  await Promise.all(animations.map((animation) => animation.finished.catch(() => undefined)));
}

test("keeps the shell equivalent while exposing the semantic attribute theme", async () => {
  await page.viewport(800, 600);
  const { host } = mountBrowser(
    <>
      <App services={createAppServicesFixture()} />
      <div id="theme-utility-probe" className="bg-background dark:bg-primary" hidden />
    </>,
  );

  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
  await expect.element(page.getByText("Node ready — no model loaded")).toBeVisible();
  const utilityProbe = document.querySelector<HTMLElement>("#theme-utility-probe");
  expect(utilityProbe).not.toBeNull();
  const loadedCss = [...document.styleSheets]
    .flatMap((sheet) => [...sheet.cssRules])
    .map((rule) => rule.cssText)
    .join("\n");
  expect(loadedCss).toContain(".bg-background");

  for (const mode of ["light", "dark"] satisfies ThemeMode[]) {
    writeThemePreference(window.localStorage, mode);
    applyTheme(document.documentElement, mode, false);
    host.dataset.loxaTheme = mode;
    await settleRenderedTheme();

    const rootStyle = getComputedStyle(document.documentElement);
    expect(rootStyle.backgroundColor).toBe(mode === "light" ? "rgb(244, 246, 240)" : "rgb(16, 20, 16)");
    expect(getComputedStyle(utilityProbe!).backgroundColor).toBe(
      mode === "light" ? "rgb(244, 246, 240)" : "rgb(183, 237, 98)",
    );
    await expect(document.body).toMatchScreenshot(`baseline-shell-${mode}-800x600`, {
      comparatorName: "pixelmatch",
      comparatorOptions: { allowedMismatchedPixelRatio: 0.005, includeAA: false, threshold: 0.2 },
      screenshotOptions: { animations: "disabled", caret: "hide", scale: "css" },
    });
  }

  const settingsLink = document.querySelector<HTMLAnchorElement>('a[href="#settings"]');
  expect(settingsLink).not.toBeNull();
  settingsLink?.focus();
  const focusStyle = getComputedStyle(settingsLink!);
  expect(focusStyle.outlineStyle).toBe("solid");
  expect(focusStyle.outlineWidth).toBe("2px");

  const remoteResources = performance
    .getEntriesByType("resource")
    .map(({ name }) => new URL(name, window.location.href))
    .filter(({ protocol, origin }) => /^https?:$/.test(protocol) && origin !== window.location.origin);
  expect(remoteResources.map(({ href }) => href)).toEqual([]);
});
