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
    useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 220 });
    const { host } = mountBrowser(<App />);
    host.dataset.loxaTheme = theme;

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    await expect.element(page.getByRole("heading", { name: "New Chat" })).toBeVisible();
    await act(async () => page.getByRole("heading", { name: "New Chat" }).hover());

    const composer = page.getByRole("form", { name: "Message composer" }).element();
    const attachment = page.getByRole("button", { name: "Attach document" }).element();
    const model = page.getByRole("combobox", { name: "Choose model" }).element();
    const send = page.getByRole("button", { name: "Send message" }).element();
    const supportReason = document.querySelector<HTMLElement>("#chat-support-reason");
    const attachmentReason = document.querySelector<HTMLElement>('[role="tooltip"]');
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
    expect(supportReason).toBeNull();
    expect(attachmentReason).not.toBeNull();
    expect(attachmentReason).toHaveTextContent("Document input support cannot be checked until the node is connected.");
    await expect.element(attachmentReason!).not.toBeVisible();
    expect(attachment).toHaveAttribute("aria-disabled", "true");
    expect(attachment).toHaveAttribute("aria-describedby", "attachment-support-reason");
    await act(async () => attachment.focus());
    await expect.element(attachmentReason!).toBeVisible();
    await act(async () => attachment.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })));
    await expect.element(attachmentReason!).not.toBeVisible();
    expect(document.activeElement).toBe(attachment);
    await act(async () => attachment.blur());
    await expect.element(attachmentReason!).not.toBeVisible();
    await act(async () => page.getByRole("button", { name: "Attach document" }).hover());
    await expect.element(attachmentReason!).toBeVisible();
    await act(async () => page.getByRole("tooltip").hover());
    await expect.element(attachmentReason!).toBeVisible();
    await act(async () => page.getByRole("heading", { name: "New Chat" }).hover());
    await expect.element(attachmentReason!).not.toBeVisible();
    expect(composer).not.toHaveTextContent("Active: None");
    expect(document.querySelector("#model-control-help")).toBeNull();
    const modelControl = page.getByRole("region", { name: "Chat model" }).element();
    expect(modelControl).toHaveTextContent("No active model");
    const message = page.getByRole("textbox", { name: "Message" }).element();
    expect(getComputedStyle(message).backgroundColor).toBe("rgba(0, 0, 0, 0)");
    expect(message).toBeDisabled();

    for (const control of [attachment, send]) {
      const rect = control.getBoundingClientRect();
      expect(rect.width).toBeGreaterThanOrEqual(36);
      expect(rect.height).toBeGreaterThanOrEqual(44);
      expect(rect.left).toBeGreaterThanOrEqual(composerRect.left);
      expect(rect.right).toBeLessThanOrEqual(composerRect.right);
    }

    expect(composer).not.toContainElement(model);
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
