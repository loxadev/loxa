import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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
});
