import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { SettingsScreen } from "./SettingsScreen";

describe("SettingsScreen", () => {
  it("exposes Light, Dark, and System as an accessible keyboard-operated choice", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<SettingsScreen theme="system" onThemeChange={onChange} />);

    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("radiogroup", { name: "Appearance" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "System" })).toBeChecked();

    await user.click(screen.getByRole("radio", { name: "Dark" }));
    expect(onChange).toHaveBeenCalledWith("dark");
  });

  it("announces the active preference in text", () => {
    render(<SettingsScreen theme="light" onThemeChange={vi.fn()} />);

    expect(screen.getByRole("status")).toHaveTextContent("Theme set to Light");
  });

  it("tabs to the selected choice and moves selection with arrow keys", async () => {
    const user = userEvent.setup();
    function Harness() {
      const [theme, setTheme] = useState<"light" | "dark" | "system">("system");
      return <SettingsScreen theme={theme} onThemeChange={setTheme} />;
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
});
