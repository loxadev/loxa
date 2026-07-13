import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { SettingsScreen } from "./SettingsScreen";

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

  it("organizes appearance and read-only local runtime facts without unsupported controls", () => {
    render(<SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={runtime} />);

    const appearance = screen.getByRole("radiogroup", { name: "Appearance" });
    const local = screen.getByRole("region", { name: "Local node/runtime" });
    expect(appearance.compareDocumentPosition(local) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    for (const value of [runtime.endpoint, "Externally attached", "loxa-node-42", "llama.cpp", "b4321", "gemma-3-4b-it-q4"]) {
      expect(screen.getByText(value)).toBeInTheDocument();
    }
    expect(screen.getByText("Theme is the only preference saved on this Mac. Node and model state are not stored here.")).toBeVisible();
    expect(screen.getByText("llama.cpp")).toHaveClass("technical-value");
    expect(local.querySelectorAll("input, button, select, textarea")).toHaveLength(0);
    expect(screen.queryByText(/start on login|provider|sampling|authentication|LAN|logs/i)).not.toBeInTheDocument();
  });

  it("renders unavailable runtime facts truthfully", () => {
    render(<SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={{ ...runtime, phase: "starting", ownership: "none", status: null }} />);
    expect(screen.getByText("Checking", { selector: "dd" })).toBeInTheDocument();
    expect(screen.getAllByText("Unavailable", { selector: "dd" }).length).toBeGreaterThan(1);
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
    const clear = vi.fn((nextSignal: AbortSignal) => new Promise<number>((_resolve, reject) => {
      signal = nextSignal;
      nextSignal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    }));
    const view = render(<SettingsScreen theme="system" onThemeChange={vi.fn()} runtime={runtime} onClearChatHistory={clear} />);
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
      return new Promise<number>((resolve) => { resolveClear = resolve; });
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
