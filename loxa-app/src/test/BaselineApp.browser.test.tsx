import { act } from "react";
import { expect, test } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import { applyTheme, writeThemePreference, type ThemeMode } from "@/settings/theme";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

test("captures the untouched desktop shell in fixed light and dark modes", async () => {
  await page.viewport(800, 600);
  const { host } = mountBrowser(<App services={createAppServicesFixture()} />);
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
  await expect.element(page.getByText("Node ready — no model loaded")).toBeVisible();
  await document.fonts.ready;

  for (const mode of ["light", "dark"] satisfies ThemeMode[]) {
    writeThemePreference(window.localStorage, mode);
    applyTheme(document.documentElement, mode, false);
    host.dataset.loxaTheme = mode;
    await expect(document.body).toMatchScreenshot(`baseline-shell-${mode}-800x600`);
  }
});
