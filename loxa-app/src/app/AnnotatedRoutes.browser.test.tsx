import { act } from "react";
import { expect, test, vi } from "vitest";
import { page } from "vitest/browser";

import App from "@/App";
import type { ControlStreamCallbacks } from "@/control/events";
import type { ModelInventoryEntry, OperationView } from "@/control/contracts";
import { applyTheme } from "@/settings/theme";
import { useWorkspaceStore } from "@/stores/workspace-store";
import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { createAppServicesFixture } from "@/test/fixtures";

const model: ModelInventoryEntry = {
  id: "gemma-browser",
  repo: "loxa/gemma-browser",
  revision: "0123456789abcdef",
  filename: "gemma-browser.gguf",
  sha256: "ab".repeat(32),
  sizeBytes: 1024,
  license: "Apache-2.0",
  params: "4B",
  quant: "Q4_K_M",
  minFreeMemoryGiB: 6,
  artifact: { kind: "not_downloaded" },
  compatibility: { compatible: true, reason: "Available memory meets the verified recipe minimum." },
  engine: { engine: "llama-cpp", eligible: true, reason: "Verified for llama.cpp." },
};

const runningDownload: OperationView = {
  id: "browser-download",
  kind: "download",
  status: "running",
  modelId: model.id,
  progress: { completedBytes: 512, totalBytes: 1024 },
  error: null,
  createdAtUnixMs: 1_700_000_000_000,
  updatedAtUnixMs: 1_700_000_000_001,
};

async function navigate(name: "Node" | "Models" | "Settings") {
  await act(async () => page.getByRole("link", { name, exact: true }).click());
}

async function settleApp() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
  });
}

async function settleVisuals() {
  await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  await Promise.all(
    document
      .getAnimations()
      .filter(({ playState }) => playState !== "finished")
      .map((animation) => animation.finished.catch(() => undefined)),
  );
}

function expectNoViewportOverflow() {
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
  expect(document.body.scrollWidth).toBeLessThanOrEqual(document.body.clientWidth);
}

test("covers the annotated Nodes, Models, Settings overview, and Runtime routes at 800 by 600", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({
    activeRoute: "chat",
    activeSettingsPage: "overview",
    sidebarCollapsed: false,
    expandedSidebarWidth: 220,
  });
  const services = createAppServicesFixture({ getInventory: async () => [model] });
  const { host } = mountBrowser(<App services={services} />);
  applyTheme(host, "light", false);
  await settleApp();

  await navigate("Node");
  await expect.element(page.getByRole("heading", { name: "Nodes" })).toBeVisible();
  await expect.element(page.getByRole("table", { name: "Local node inventory" })).toBeVisible();

  await navigate("Models");
  await expect.element(page.getByRole("heading", { name: "Models", exact: true })).toBeVisible();
  await expect.element(page.getByRole("heading", { name: model.id })).toBeVisible();

  await navigate("Settings");
  await expect.element(page.getByRole("heading", { name: "Settings", exact: true })).toBeVisible();
  await expect.element(page.getByText(/theme and sidebar display preferences are saved on this Mac/i)).toBeVisible();
  await act(async () => page.getByRole("button", { name: /Runtime/ }).click());
  await expect.element(page.getByRole("heading", { name: "Runtime", exact: true })).toBeVisible();
  await expect.element(page.getByText("http://127.0.0.1:8080")).toBeVisible();

  expectNoViewportOverflow();
  await settleVisuals();
  await expectNoAxeViolations(document);
});

test("wraps a long recovery-required error without hiding the Node recovery feedback", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "node", sidebarCollapsed: false, expandedSidebarWidth: 220 });
  const longError =
    "Recovery required after the local child exited without a clean ownership handoff. " +
    `diagnostic-${"x".repeat(180)}. Restart the private node before model controls continue.`;
  const base = createAppServicesFixture();
  const services = createAppServicesFixture({
    bootstrap: { ...base.bootstrap, start: vi.fn().mockRejectedValue(new Error(longError)) },
  });
  const { host } = mountBrowser(<App services={services} />);
  applyTheme(host, "light", false);
  await settleApp();

  const alert = page.getByRole("alert");
  await expect.element(page.getByRole("heading", { name: "Nodes" })).toBeVisible();
  await expect.element(alert).toHaveTextContent(longError);
  await expect.element(page.getByText("Recovery required", { exact: true }).first()).toBeVisible();
  expect(alert.element().getBoundingClientRect().right).toBeLessThanOrEqual(document.documentElement.clientWidth);
  expectNoViewportOverflow();
  await settleVisuals();
  await expectNoAxeViolations(document);
});

test("renders deterministic model download progress and recovery-safe controls at 800 by 600", async () => {
  await page.viewport(800, 600);
  useWorkspaceStore.setState({ activeRoute: "models", sidebarCollapsed: false, expandedSidebarWidth: 220 });
  const callbacks: ControlStreamCallbacks[] = [];
  const services = createAppServicesFixture({
    getInventory: async () => [model],
    createControlEventStream: (_endpoint, _token, _cursor, next) => {
      callbacks.push(next);
      return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise(() => undefined) };
    },
  });
  const { host } = mountBrowser(<App services={services} />);
  applyTheme(host, "light", false);
  await settleApp();

  await expect.element(page.getByRole("heading", { name: model.id })).toBeVisible();
  await vi.waitFor(() => expect(callbacks.length).toBeGreaterThanOrEqual(2));
  await act(async () => {
    callbacks[callbacks.length - 1]?.onSnapshot({
      cursor: 1,
      cursorGap: false,
      operations: [runningDownload],
      events: [],
    });
  });

  const progress = page.getByRole("progressbar", { name: `Download progress for ${model.id}` });
  await expect.element(progress).toHaveAttribute("value", "512");
  await expect.element(progress).toHaveAttribute("max", "1024");
  await expect.element(page.getByText("512 B of 1 KB")).toBeVisible();
  await expect.element(page.getByRole("button", { name: `Cancel download ${model.id}` })).toBeVisible();
  expectNoViewportOverflow();
  await settleVisuals();
  await expectNoAxeViolations(document);
});
