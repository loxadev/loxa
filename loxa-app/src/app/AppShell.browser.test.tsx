import { act } from "react";
import { expect, test } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import { useWorkspaceStore } from "@/stores/workspace-store";
import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

async function settleShell() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
  await expect.element(page.getByRole("heading", { name: "New Chat" })).toBeVisible();
}

test("renders a fixed activity rail and independently sized conversation rail", async () => {
  await page.viewport(900, 650);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 320 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  expect(document.querySelector<HTMLElement>(".activity-rail")?.getBoundingClientRect().width).toBe(48);
  expect(document.querySelector<HTMLElement>(".conversation-panel")?.getBoundingClientRect().width).toBe(320);
  expect(document.querySelector<HTMLElement>(".app-sidebar")?.getBoundingClientRect().width).toBe(368);
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
  await expectNoAxeViolations(document);
});

test("resizes continuously within bounds and never collapses from pointer movement", async () => {
  await page.viewport(900, 650);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const divider = page.getByRole("separator", { name: "Resize conversation rail" }).element();
  await act(async () => {
    divider.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, pointerId: 1, button: 0, clientX: 328 }));
    window.dispatchEvent(new PointerEvent("pointermove", { bubbles: true, pointerId: 1, clientX: 168 }));
    window.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, pointerId: 1, clientX: 168 }));
  });

  expect(useWorkspaceStore.getState().expandedSidebarWidth).toBe(240);
  expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(false);
  await expect
    .element(page.getByRole("separator", { name: "Resize conversation rail" }))
    .toHaveAttribute("aria-valuenow", "240");
});

test("collapses conversations only through the explicit control and keeps navigation available", async () => {
  await page.viewport(900, 650);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 300 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  await act(async () => page.getByRole("button", { name: "Hide conversations" }).first().click());

  expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(true);
  expect(document.querySelector(".conversation-panel")).toBeNull();
  expect(document.querySelector<HTMLElement>(".app-sidebar")?.getBoundingClientRect().width).toBe(48);
  await expect.element(page.getByRole("link", { name: "Models" })).toBeVisible();

  await act(async () => page.getByRole("button", { name: "Show conversations" }).click());
  expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(false);
  expect(document.querySelector<HTMLElement>(".conversation-panel")?.getBoundingClientRect().width).toBe(300);
});

test("shows descriptive tooltips for the icon activity rail", async () => {
  await page.viewport(900, 650);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  await act(async () => page.getByRole("link", { name: "Models" }).hover());
  await expect.element(page.getByRole("tooltip", { name: "Models" })).toBeVisible();
  await expectNoAxeViolations(document);
});

test("overlays observability before simultaneous panels collapse the workspace", async () => {
  await page.viewport(900, 650);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: false, expandedSidebarWidth: 320 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  await act(async () => page.getByRole("button", { name: "Node online" }).click());

  await expect.element(page.getByRole("complementary", { name: "Health and observability inspector" })).toBeVisible();
  expect(document.querySelector<HTMLElement>(".workspace")?.getBoundingClientRect().width).toBeGreaterThanOrEqual(500);
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
});
