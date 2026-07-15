import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { SettingsScreen } from "./SettingsScreen";
import { useWorkspaceStore } from "../stores/workspace-store";

const runtime = {
  phase: "ready" as const,
  endpoint: "http://127.0.0.1:8080",
  ownership: "attached" as const,
  status: {
    node_id: "loxa-node-42",
    health: "ready" as const,
    model: "loxa" as const,
    engine: { name: "llama.cpp", version: "b4321" },
    runtime_model: "gemma-3-4b-it-q4",
    profile: "default",
  },
};

describe("SettingsScreen", () => {
  beforeEach(() => {
    useWorkspaceStore.setState({ activeSettingsPage: "overview" });
  });

  it("exposes Light, Dark, and System as an accessible keyboard-operated choice", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<SettingsScreen theme="system" onThemeChange={onChange} runtime={runtime} />);

    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("radiogroup", { name: "Appearance" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "System" })).toBeChecked();

    await user.click(screen.getByRole("radio", { name: "Dark" }));
    expect(onChange).toHaveBeenCalledWith("dark");
  });

  it("announces the active preference in text", () => {
    render(<SettingsScreen theme="light" onThemeChange={vi.fn()} runtime={runtime} />);

    expect(screen.getByRole("status")).toHaveTextContent("Theme set to Light");
  });

  it("describes the complete persisted display-preference boundary", () => {
    render(<SettingsScreen theme="light" onThemeChange={vi.fn()} runtime={runtime} />);

    expect(screen.getByText(/theme and sidebar display preferences are saved on this Mac/i)).toBeVisible();
    expect(screen.getByText(/backend, node, and model state are not stored here/i)).toBeVisible();
  });

  it("tabs to the selected choice and moves selection with arrow keys", async () => {
    const user = userEvent.setup();
    function Harness() {
      const [theme, setTheme] = useState<"light" | "dark" | "system">("system");
      return <SettingsScreen theme={theme} onThemeChange={setTheme} runtime={runtime} />;
    }
    render(<Harness />);

    await user.tab();
    expect(screen.getByRole("radio", { name: "System" })).toHaveFocus();

    await user.keyboard("{ArrowRight}");
    expect(screen.getByRole("radio", { name: "Light" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "Light" })).toHaveFocus();
    expect(screen.getByRole("status")).toHaveTextContent("Theme set to Light");

    await user.keyboard("{ArrowRight}");
    expect(screen.getByRole("radio", { name: "Dark" })).toBeChecked();
    expect(screen.getByRole("status")).toHaveTextContent("Theme set to Dark");
  });

  it("shows a keyboard-accessible Runtime row on the overview and hides runtime facts", async () => {
    const user = userEvent.setup();
    render(<SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={runtime} />);

    const runtimeRow = screen.getByRole("button", { name: /Runtime/ });
    expect(runtimeRow).toHaveTextContent("Read-only local node and runtime details");
    expect(screen.queryByRole("region", { name: "Local node/runtime" })).not.toBeInTheDocument();
    expect(screen.queryByText(runtime.endpoint)).not.toBeInTheDocument();

    runtimeRow.focus();
    await user.keyboard("{Enter}");

    const heading = screen.getByRole("heading", { name: "Runtime", level: 1 });
    expect(heading).toHaveFocus();
    const local = screen.getByRole("region", { name: "Local node/runtime" });
    expect(local).toBeVisible();
    const table = within(local).getByRole("table", { name: "Local node inventory" });
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["Node", "Status", "Active model", "Engine", "Version", "Profile", "Endpoint", "Ownership"]);
    expect(within(table).getAllByRole("row")).toHaveLength(2);
    expect(within(table).getByText("Local node")).toBeVisible();
    for (const value of [
      runtime.endpoint,
      "Externally attached",
      "loxa-node-42",
      "llama.cpp",
      "b4321",
      "gemma-3-4b-it-q4",
      "default",
    ]) {
      expect(screen.getByText(value)).toBeInTheDocument();
    }
    const badge = within(table).getByText("Ready", { selector: '[data-slot="status-badge"]' });
    expect(badge).toHaveAttribute("data-variant", "success");
    expect(badge).not.toHaveAttribute("aria-live");
    expect(within(table).queryByText(runtime.status.health)).not.toBeInTheDocument();
    expect(screen.getByText("llama.cpp")).toHaveClass("technical-value");
    expect(local.querySelectorAll("input, button, select, textarea")).toHaveLength(0);
    expect(within(table).queryByRole("columnheader", { name: "Actions" })).not.toBeInTheDocument();
    expect(screen.queryByText(/start on login|provider|sampling|authentication|LAN|logs/i)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Back to Settings" }));
    expect(screen.getByRole("heading", { name: "Settings", level: 1 })).toHaveFocus();
    expect(screen.getByRole("button", { name: /Runtime/ })).toBeVisible();
    expect(screen.queryByText(runtime.endpoint)).not.toBeInTheDocument();
  });

  it("renders unavailable runtime facts truthfully", () => {
    useWorkspaceStore.setState({ activeSettingsPage: "runtime" });
    render(
      <SettingsScreen
        theme="system"
        onThemeChange={vi.fn()}
        runtime={{ ...runtime, phase: "starting", ownership: "none", status: null }}
      />,
    );
    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).getByText("Starting", { selector: '[data-slot="status-badge"]' })).toBeVisible();
    const row = within(table).getAllByRole("row")[1];
    expect(within(row).getAllByText("—").length).toBeGreaterThan(1);
    expect(row).not.toHaveTextContent("No model loaded");
    expect(row).not.toHaveTextContent("Unavailable");
  });

  it("discloses plaintext local history and requires confirmation before clearing it", async () => {
    const user = userEvent.setup();
    const clear = vi.fn().mockResolvedValue(3);
    render(<SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={runtime} onClearChatHistory={clear} />);
    expect(screen.getByText(/stored as local plaintext/i)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Clear chat history" }));
    expect(clear).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Confirm clear chat history" }));
    expect(clear).toHaveBeenCalledOnce();
    expect(await screen.findByRole("status")).toHaveTextContent("Deleted 3 conversations");
  });

  it("aborts a rejecting clear request on unmount without publishing a stale error", async () => {
    const user = userEvent.setup();
    let signal: AbortSignal | undefined;
    const clear = vi.fn(
      (nextSignal: AbortSignal) =>
        new Promise<number>((_resolve, reject) => {
          signal = nextSignal;
          nextSignal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
        }),
    );
    const view = render(
      <SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={runtime} onClearChatHistory={clear} />,
    );
    await user.click(screen.getByRole("button", { name: "Clear chat history" }));
    await user.click(screen.getByRole("button", { name: "Confirm clear chat history" }));
    expect(signal).toBeInstanceOf(AbortSignal);

    view.unmount();
    expect(signal?.aborted).toBe(true);
    await act(async () => undefined);
  });

  it("aborts clear on window close and ignores a late successful completion", async () => {
    const user = userEvent.setup();
    let signal: AbortSignal | undefined;
    let resolveClear!: (deleted: number) => void;
    const clear = vi.fn((nextSignal: AbortSignal) => {
      signal = nextSignal;
      return new Promise<number>((resolve) => {
        resolveClear = resolve;
      });
    });
    render(<SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={runtime} onClearChatHistory={clear} />);
    await user.click(screen.getByRole("button", { name: "Clear chat history" }));
    await user.click(screen.getByRole("button", { name: "Confirm clear chat history" }));

    window.dispatchEvent(new Event("beforeunload"));
    expect(signal?.aborted).toBe(true);
    resolveClear(9);
    await act(async () => undefined);
    expect(screen.getByRole("status")).not.toHaveTextContent(/Deleted 9|Could not clear/i);
  });

  it("uses a feature-local canonical accessibility contract", () => {
    const css = readFileSync(resolve(process.cwd(), "src/settings/SettingsScreen.module.css"), "utf8");
    expect(css).toContain("var(--loxa-component-minimum-interactive-target)");
    expect(css).toContain("@media (prefers-contrast: more)");
    expect(css).toContain("@media (forced-colors: active)");
    expect(css).toContain("@media (prefers-reduced-motion: reduce)");
    expect(css).not.toMatch(/#[0-9a-f]{3,8}\b/i);
  });
});
