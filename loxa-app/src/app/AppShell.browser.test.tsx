import { act } from "react";
import { expect, test } from "vitest";
import { page, userEvent } from "vitest/browser";

import App from "@/App";
import { applyTheme } from "@/settings/theme";
import { useWorkspaceStore } from "@/stores/workspace-store";
import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

async function settleShell() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
  await expect.element(page.getByRole("heading", { name: "Chat" })).toBeVisible();
}

async function settleTheme() {
  await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  const animations = document.getAnimations().filter(({ playState }) => playState !== "finished");
  await Promise.all(animations.map((animation) => animation.finished.catch(() => undefined)));
}

function relativeLuminance(color: string) {
  const channels = color.match(/\d+/g)?.slice(0, 3).map(Number);
  if (!channels || channels.length !== 3) throw new Error(`Unsupported color: ${color}`);
  const [red, green, blue] = channels.map((channel) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
}

function contrastRatio(foreground: string, background: string) {
  const [lighter, darker] = [relativeLuminance(foreground), relativeLuminance(background)].sort(
    (left, right) => right - left,
  );
  return (lighter + 0.05) / (darker + 0.05);
}

test("keeps expanded and collapsed shell geometry accessible at 800 by 600", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 400 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const shell = document.querySelector<HTMLElement>(".app-shell");
  const sidebar = document.querySelector<HTMLElement>(".app-sidebar");
  expect(shell).not.toBeNull();
  expect(sidebar?.getBoundingClientRect().width).toBe(400);
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
  await expectNoAxeViolations(document);

  await act(async () => {
    await page.getByRole("button", { name: "Collapse sidebar" }).click();
  });
  expect(sidebar?.getBoundingClientRect().width).toBe(56);
  const collapsedDivider = document.querySelector<HTMLElement>('[role="separator"]');
  expect(collapsedDivider).not.toBeNull();
  expect(collapsedDivider!.getBoundingClientRect().left).toBeGreaterThanOrEqual(
    sidebar!.getBoundingClientRect().right - 4,
  );
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
  await expectNoAxeViolations(document);
});

test("keeps Settings bottom-anchored in expanded and collapsed sidebars", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const sidebar = document.querySelector<HTMLElement>(".app-sidebar");
  const footer = document.querySelector<HTMLElement>(".sidebar-footer");
  expect(sidebar).not.toBeNull();
  expect(footer).not.toBeNull();

  const expectBottomAnchored = () => {
    const sidebarRect = sidebar!.getBoundingClientRect();
    const footerRect = footer!.getBoundingClientRect();
    const bottomInset = sidebarRect.bottom - footerRect.bottom;
    expect(bottomInset).toBeGreaterThanOrEqual(7);
    expect(bottomInset).toBeLessThanOrEqual(9);
  };

  for (const height of [600, 800]) {
    await page.viewport(800, height);
    expectBottomAnchored();

    await act(async () => page.getByRole("button", { name: "Collapse sidebar" }).click());
    expectBottomAnchored();

    await act(async () => page.getByRole("button", { name: "Expand sidebar" }).click());
    expectBottomAnchored();
  }
});

test("shows keyboard focus on collapsed navigation without horizontal overflow", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: true, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const chatLink = document.querySelector<HTMLAnchorElement>('a[href="#chat"]');
  expect(chatLink).not.toBeNull();
  await act(async () => chatLink?.focus());
  const focusStyle = getComputedStyle(chatLink!);
  expect(focusStyle.outlineStyle).toBe("solid");
  expect(focusStyle.outlineWidth).toBe("2px");
  expect(document.body.scrollWidth).toBeLessThanOrEqual(document.body.clientWidth);
});

test("uses the divider to collapse, expand, and toggle with pointer gestures", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const divider = page.getByRole("separator", { name: "Resize navigation and conversation rail" });
  const element = divider.element();
  await act(async () => {
    element.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, pointerId: 1, button: 0, clientX: 280 }));
    window.dispatchEvent(new PointerEvent("pointermove", { bubbles: true, pointerId: 1, clientX: 220 }));
    window.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, pointerId: 1, clientX: 220 }));
  });
  expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(true);

  await act(async () => {
    element.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, pointerId: 2, button: 0, clientX: 56 }));
    window.dispatchEvent(new PointerEvent("pointermove", { bubbles: true, pointerId: 2, clientX: 116 }));
    window.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, pointerId: 2, clientX: 116 }));
  });
  expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(false);

  await act(async () => element.dispatchEvent(new MouseEvent("dblclick", { bubbles: true })));
  expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(true);
});

test("shows tooltips for important icon-only controls only while collapsed", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: true, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  await act(async () => page.getByRole("link", { name: "Models" }).hover());
  await expect.element(page.getByRole("tooltip", { name: "Models" })).toBeVisible();

  await act(async () => page.getByRole("button", { name: "Expand sidebar" }).element().focus());
  await expect.element(page.getByRole("tooltip", { name: "Expand sidebar" })).toBeVisible();

  await act(async () => page.getByRole("button", { name: "Expand sidebar" }).click());
  await expect.element(page.getByRole("button", { name: "Collapse sidebar" })).toBeVisible();
  expect(document.querySelectorAll(".app-sidebar .tooltip-content")).toHaveLength(0);
});

test("keeps a collapsed-sidebar tooltip visible while the pointer moves onto it", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: true, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  await act(async () => page.getByRole("link", { name: "Models" }).hover());
  const tooltip = page.getByRole("tooltip", { name: "Models" });
  await expect.element(tooltip).toBeVisible();
  expect(getComputedStyle(tooltip.element()).pointerEvents).toBe("auto");

  await act(async () => tooltip.hover());
  await expect.element(tooltip).toBeVisible();

  const tooltipElement = tooltip.element();
  await act(async () => page.getByRole("heading", { name: "New Chat" }).hover());
  await expect.element(tooltipElement).not.toBeVisible();
  await act(async () => page.getByRole("link", { name: "Models" }).hover());
  await expect.element(tooltip).toBeVisible();
});

test("dismisses a focused collapsed-sidebar tooltip with Escape until focus returns", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: true, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const toggle = page.getByRole("button", { name: "Expand sidebar" });
  await act(async () => toggle.element().focus());
  const tooltip = page.getByRole("tooltip", { name: "Expand sidebar" });
  await expect.element(tooltip).toBeVisible();
  const tooltipElement = tooltip.element();

  await act(async () => userEvent.keyboard("{Escape}"));
  await expect.element(tooltipElement).not.toBeVisible();
  await expect.element(toggle).toHaveFocus();

  await act(async () => {
    toggle.element().blur();
    toggle.element().focus();
  });
  await expect.element(tooltip).toBeVisible();
});

test("renders the dark shell brand and new-chat foreground with visible contrast", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
  const { host } = mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  applyTheme(host, "dark", false);
  await settleTheme();

  expect(host.dataset.loxaTheme).toBe("dark");

  const brandMark = document.querySelector<HTMLImageElement>(".brand-lockup img");
  const emptyMark = document.querySelector<HTMLImageElement>('img[alt="Loxa"]');
  const newChat = page.getByRole("button", { name: "New chat" });
  const newChatIcon = newChat.element().querySelector("svg");
  const newChatText = newChat.element().querySelector("span");

  expect(brandMark).not.toBeNull();
  await expect.element(brandMark!).toBeVisible();
  expect(brandMark!.complete).toBe(true);
  expect(brandMark!.naturalWidth).toBeGreaterThan(0);
  expect(getComputedStyle(brandMark!).filter).not.toBe("none");

  expect(emptyMark).not.toBeNull();
  await expect.element(emptyMark!).toBeVisible();
  expect(emptyMark!.complete).toBe(true);
  expect(emptyMark!.naturalWidth).toBeGreaterThan(0);
  expect(getComputedStyle(emptyMark!).filter).not.toBe("none");

  await expect.element(newChat).toBeVisible();
  expect(newChatIcon).not.toBeNull();
  expect(newChatText).not.toBeNull();
  await expect.element(newChatIcon!).toBeVisible();
  await expect.element(newChatText!).toBeVisible();
  expect(newChatIcon!.getBoundingClientRect().width).toBeGreaterThan(0);
  expect(newChatText!.getBoundingClientRect().width).toBeGreaterThan(0);

  const buttonStyle = getComputedStyle(newChat.element());
  expect(contrastRatio(buttonStyle.color, buttonStyle.backgroundColor)).toBeGreaterThanOrEqual(4.5);
});
