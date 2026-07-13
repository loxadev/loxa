import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  applyTheme,
  prepaintTheme,
  readThemePreference,
  resolveTheme,
  writeThemePreference,
  useThemePreference,
} from "./theme";

afterEach(() => {
  window.localStorage.clear();
  vi.unstubAllGlobals();
});

describe("theme preferences", () => {
  it.each(["light", "dark", "system"] as const)("hydrates the validated %s preference", (mode) => {
    const storage = { getItem: vi.fn().mockReturnValue(mode) };

    expect(readThemePreference(storage)).toBe(mode);
  });

  it("falls back to system for invalid or unavailable storage", () => {
    expect(readThemePreference({ getItem: vi.fn().mockReturnValue("sepia") })).toBe("system");
    expect(readThemePreference({ getItem: vi.fn(() => { throw new Error("blocked"); }) })).toBe("system");
  });

  it("keeps the selected mode session-only when storage is unavailable", () => {
    const storage = { setItem: vi.fn(() => { throw new Error("blocked"); }) };

    expect(writeThemePreference(storage, "dark")).toBe(false);
  });

  it("resolves system from the current preference and applies canonical theme attributes", () => {
    const root = document.createElement("html");

    expect(resolveTheme("system", true)).toBe("dark");
    expect(resolveTheme("system", false)).toBe("light");
    applyTheme(root, "system", true);

    expect(root).toHaveAttribute("data-loxa-theme", "dark");
    expect(root).toHaveAttribute("data-loxa-theme-preference", "system");
  });

  it("tracks system appearance changes and removes its listener on unmount", () => {
    let listener: (() => void) | undefined;
    const media = {
      matches: false,
      addEventListener: vi.fn((_type: string, next: () => void) => { listener = next; }),
      removeEventListener: vi.fn(),
    };
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue(media));

    const { unmount } = renderHook(() => useThemePreference());
    expect(document.documentElement).toHaveAttribute("data-loxa-theme", "light");

    media.matches = true;
    act(() => listener?.());
    expect(document.documentElement).toHaveAttribute("data-loxa-theme", "dark");

    unmount();
    expect(media.removeEventListener).toHaveBeenCalledWith("change", listener);
  });

  it("applies each setter selection to the document root before the event returns", () => {
    const media = {
      matches: true,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    };
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue(media));
    const { result } = renderHook(() => useThemePreference());
    expect(document.documentElement).toHaveAttribute("data-loxa-theme", "dark");

    act(() => {
      result.current[1]("light");
      expect(document.documentElement).toHaveAttribute("data-loxa-theme-preference", "light");
      expect(document.documentElement).toHaveAttribute("data-loxa-theme", "light");
    });
    act(() => {
      result.current[1]("dark");
      expect(document.documentElement).toHaveAttribute("data-loxa-theme", "dark");
    });
    media.matches = false;
    act(() => {
      result.current[1]("system");
      expect(document.documentElement).toHaveAttribute("data-loxa-theme-preference", "system");
      expect(document.documentElement).toHaveAttribute("data-loxa-theme", "light");
    });
    expect(window.localStorage.getItem("loxa.theme")).toBe("system");
  });

  it("rejects an invalid runtime setter value without changing root or storage", () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }));
    window.localStorage.setItem("loxa.theme", "light");
    const { result } = renderHook(() => useThemePreference());

    act(() => result.current[1]("sepia" as never));

    expect(document.documentElement).toHaveAttribute("data-loxa-theme", "light");
    expect(window.localStorage.getItem("loxa.theme")).toBe("light");
  });

  it("applies a validated stored preference before React renders", () => {
    window.localStorage.setItem("loxa.theme", "dark");
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: false }));

    prepaintTheme();

    expect(document.documentElement).toHaveAttribute("data-loxa-theme-preference", "dark");
    expect(document.documentElement).toHaveAttribute("data-loxa-theme", "dark");
  });
});
