import { act } from "react";
import { expect, test } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import { useWorkspaceStore } from "@/stores/workspace-store";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

test.each(["light", "dark"] as const)(
  "keeps the %s message composer compact and usable at 800 by 600",
  async (theme) => {
    await page.viewport(800, 600);
    useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
    const { host } = mountBrowser(<App services={createAppServicesFixture()} />);
    host.dataset.loxaTheme = theme;

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    await expect.element(page.getByRole("heading", { name: "Chat" })).toBeVisible();

    const composer = page.getByRole("form", { name: "Message composer" }).element();
    const attachment = page.getByRole("button", { name: "Attach document" }).element();
    const model = page.getByRole("combobox", { name: "Choose model" }).element();
    const send = page.getByRole("button", { name: "Send message" }).element();
    const composerRect = composer.getBoundingClientRect();

    expect(composerRect.height).toBeLessThanOrEqual(190);
    expect(composer.scrollWidth).toBeLessThanOrEqual(composer.clientWidth);
    expect(getComputedStyle(composer).borderLeftWidth).toBe("0px");
    expect(getComputedStyle(composer).borderRadius).toBe("0px");

    for (const control of [attachment, model, send]) {
      const rect = control.getBoundingClientRect();
      expect(rect.width).toBeGreaterThanOrEqual(36);
      expect(rect.left).toBeGreaterThanOrEqual(composerRect.left);
      expect(rect.right).toBeLessThanOrEqual(composerRect.right);
    }

    expect(model.getBoundingClientRect().top).toBe(attachment.getBoundingClientRect().top);
    expect(send.getBoundingClientRect().top).toBe(attachment.getBoundingClientRect().top);
  },
);
