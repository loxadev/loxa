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
  await expect.element(page.getByRole("heading", { name: "Chat" })).toBeVisible();
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
  expect(document.querySelector('[role="separator"]')).toBeNull();
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
  await expectNoAxeViolations(document);
});

test("shows keyboard focus on collapsed navigation without horizontal overflow", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "chat", sidebarCollapsed: true, expandedSidebarWidth: 280 });
  mountBrowser(<App services={createAppServicesFixture()} />);
  await settleShell();

  const chatLink = document.querySelector<HTMLAnchorElement>('a[href="#chat"]');
  expect(chatLink).not.toBeNull();
  chatLink?.focus();
  const focusStyle = getComputedStyle(chatLink!);
  expect(focusStyle.outlineStyle).toBe("solid");
  expect(focusStyle.outlineWidth).toBe("2px");
  expect(document.body.scrollWidth).toBeLessThanOrEqual(document.body.clientWidth);
});
