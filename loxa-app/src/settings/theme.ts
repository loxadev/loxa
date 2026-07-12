import { useCallback, useEffect, useState } from "react";

export type ThemeMode = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

export const THEME_STORAGE_KEY = "loxa.theme";
const DARK_QUERY = "(prefers-color-scheme: dark)";

type ReadableStorage = Pick<Storage, "getItem">;
type WritableStorage = Pick<Storage, "setItem">;

export function isThemeMode(value: unknown): value is ThemeMode {
  return value === "light" || value === "dark" || value === "system";
}

export function readThemePreference(storage: ReadableStorage): ThemeMode {
  try {
    const value = storage.getItem(THEME_STORAGE_KEY);
    return isThemeMode(value) ? value : "system";
  } catch {
    return "system";
  }
}

export function writeThemePreference(storage: WritableStorage, mode: ThemeMode): boolean {
  try {
    storage.setItem(THEME_STORAGE_KEY, mode);
    return true;
  } catch {
    return false;
  }
}

export function resolveTheme(mode: ThemeMode, systemIsDark: boolean): ResolvedTheme {
  return mode === "system" ? (systemIsDark ? "dark" : "light") : mode;
}

export function applyTheme(root: HTMLElement, mode: ThemeMode, systemIsDark: boolean): void {
  root.dataset.loxaThemePreference = mode;
  root.dataset.loxaTheme = resolveTheme(mode, systemIsDark);
}

function getDarkPreference(): MediaQueryList | null {
  return typeof window.matchMedia === "function" ? window.matchMedia(DARK_QUERY) : null;
}

function getInitialMode(): ThemeMode {
  try {
    return readThemePreference(window.localStorage);
  } catch {
    return "system";
  }
}

function persistMode(mode: ThemeMode): boolean {
  try {
    return writeThemePreference(window.localStorage, mode);
  } catch {
    return false;
  }
}

export function prepaintTheme(): void {
  const mode = getInitialMode();
  applyTheme(document.documentElement, mode, getDarkPreference()?.matches ?? false);
}

export function useThemePreference(): readonly [ThemeMode, (mode: ThemeMode) => void] {
  const [mode, setMode] = useState<ThemeMode>(getInitialMode);

  useEffect(() => {
    const media = getDarkPreference();
    const update = () => applyTheme(document.documentElement, mode, media?.matches ?? false);
    update();
    media?.addEventListener("change", update);
    return () => media?.removeEventListener("change", update);
  }, [mode]);

  const selectMode = useCallback((next: ThemeMode) => {
    setMode(next);
    persistMode(next);
  }, []);

  return [mode, selectMode] as const;
}
