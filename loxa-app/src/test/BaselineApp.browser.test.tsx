import { act } from "react";
import { expect, test } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import { applyTheme, writeThemePreference, type ThemeMode } from "@/settings/theme";
import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";
import { shellScreenshotOptions } from "@/test/screenshot";

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

test("captures the chat-first desktop shell in fixed light and dark modes", async () => {
  await page.viewport(800, 600);
  const { host } = mountBrowser(<App services={createAppServicesFixture()} />);
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
  const chatHeading = page.getByRole("heading", { name: "Chat" });
  const chatNavigation = page.getByRole("link", { name: "Chat" });
  const conversationRail = page.getByRole("navigation", { name: "Chat conversations" });
  await expect.element(chatHeading).toBeVisible();
  await expect.element(chatNavigation).toHaveAttribute("aria-current", "page");
  await expect.element(conversationRail).toBeVisible();
  await expect.element(page.getByText("No conversations yet.")).toBeVisible();
  await expect.element(page.getByRole("link", { name: "Node online. No active model" })).toBeVisible();
  const technicalValue = document.querySelector<HTMLElement>(".global-node-status-model");
  expect(technicalValue).not.toBeNull();
  const ibmPlexMonoFaces = await document.fonts.load(`400 12px "IBM Plex Mono"`, "No active model");
  await document.fonts.ready;
  expect(ibmPlexMonoFaces.length).toBeGreaterThan(0);
  expect(document.fonts.check(`400 12px "IBM Plex Mono"`, "No active model")).toBe(true);
  expect(getComputedStyle(chatHeading.element()).fontFamily).toContain("ui-sans-serif");
  expect(getComputedStyle(document.body).fontFamily).toContain("ui-sans-serif");
  expect(getComputedStyle(technicalValue!).fontFamily).toContain("IBM Plex Mono");

  for (const mode of ["light", "dark"] satisfies ThemeMode[]) {
    writeThemePreference(window.localStorage, mode);
    applyTheme(document.documentElement, mode, false);
    host.dataset.loxaTheme = mode;
    await waitForThemeTransition();
    await expectNoAxeViolations(document);
    await expect(document.body).toMatchScreenshot(`baseline-shell-${mode}-800x600`, shellScreenshotOptions);
  }
});
