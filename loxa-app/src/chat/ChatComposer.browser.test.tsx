import { act } from "react";
import type { CDPSession } from "@vitest/browser-playwright";
import { expect, test } from "vitest";
import { cdp, page } from "vitest/browser";

import App from "@/App";
import { useWorkspaceStore } from "@/stores/workspace-store";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

function resolveColor(host: HTMLElement, color: string) {
  const reference = document.createElement("div");
  reference.style.color = color;
  host.append(reference);
  const resolved = getComputedStyle(reference).color;
  reference.remove();
  return resolved;
}

function createReadyAppServicesFixture() {
  return createAppServicesFixture({
    getStatus: async () => ({
      node_id: "loxa-browser-fixture",
      health: "ready",
      model: "loxa",
      engine: { name: "llama.cpp", version: "browser" },
      runtime_model: "loxa",
      profile: "default",
    }),
    getControlNode: async () => ({ status: "ready", activeModelId: "loxa", operationId: null, error: null }),
  });
}

test.each(["light", "dark"] as const)(
  "keeps the %s message composer compact and usable at 800 by 600",
  async (theme) => {
    await page.viewport(800, 600);
    useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
    const { host } = mountBrowser(<App services={createReadyAppServicesFixture()} />);
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
    const composerStyle = getComputedStyle(composer);
    const hostStyle = getComputedStyle(host);

    expect(composerRect.height).toBeLessThanOrEqual(190);
    expect(composer.scrollWidth).toBeLessThanOrEqual(composer.clientWidth);
    expect(composerStyle.borderTopWidth).toBe("1px");
    expect(composerStyle.borderRightWidth).toBe("1px");
    expect(composerStyle.borderBottomWidth).toBe("1px");
    expect(composerStyle.borderLeftWidth).toBe("1px");
    expect(composerStyle.borderColor).toBe(
      resolveColor(host, hostStyle.getPropertyValue("--loxa-control-border").trim()),
    );
    expect(composerStyle.backgroundColor).toBe(
      resolveColor(host, hostStyle.getPropertyValue("--loxa-background").trim()),
    );
    expect(composerStyle.borderRadius).toBe(hostStyle.getPropertyValue("--loxa-radius-lg").trim());
    const message = page.getByRole("textbox", { name: "Message" }).element();
    expect(getComputedStyle(message).backgroundColor).toBe("rgba(0, 0, 0, 0)");

    message.focus();
    expect(document.activeElement).toBe(message);
    const focusedComposerStyle = getComputedStyle(composer);
    expect(focusedComposerStyle.outlineStyle).toBe("solid");
    expect(focusedComposerStyle.outlineWidth).toBe("2px");
    expect(focusedComposerStyle.outlineColor).not.toBe("rgba(0, 0, 0, 0)");
    expect(composer.getBoundingClientRect().height).toBeLessThanOrEqual(190);
    expect(composer.scrollWidth).toBeLessThanOrEqual(composer.clientWidth);

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

test("keeps the focused composer visible and contained in forced colors", async () => {
  const session = cdp() as CDPSession;
  await session.send("Emulation.setEmulatedMedia", {
    features: [{ name: "forced-colors", value: "active" }],
  });

  try {
    await page.viewport(800, 600);
    useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
    mountBrowser(<App services={createReadyAppServicesFixture()} />);

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(matchMedia("(forced-colors: active)").matches).toBe(true);

    const composer = page.getByRole("form", { name: "Message composer" }).element();
    const message = page.getByRole("textbox", { name: "Message" }).element();
    message.focus();

    expect(document.activeElement).toBe(message);
    const style = getComputedStyle(composer);
    expect(style.borderTopWidth).toBe("1px");
    expect(style.borderRightWidth).toBe("1px");
    expect(style.borderBottomWidth).toBe("1px");
    expect(style.borderLeftWidth).toBe("1px");
    expect(style.borderColor).not.toBe("rgba(0, 0, 0, 0)");
    expect(style.outlineStyle).toBe("solid");
    expect(style.outlineWidth).toBe("2px");
    expect(style.outlineColor).not.toBe("rgba(0, 0, 0, 0)");
    expect(composer.getBoundingClientRect().height).toBeLessThanOrEqual(190);
    expect(composer.scrollWidth).toBeLessThanOrEqual(composer.clientWidth);
  } finally {
    await session.send("Emulation.setEmulatedMedia", { features: [] });
  }
});
