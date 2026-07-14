export type ThemeMode = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

export const THEME_STORAGE_KEY = "loxa.theme";
export const DARK_THEME_QUERY = "(prefers-color-scheme: dark)";

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

export function prepaintTheme(): void {
  let mode: ThemeMode = "system";
  try {
    mode = readThemePreference(window.localStorage);
  } catch {
    // Storage can be unavailable; keep the safe session default.
  }
  const systemIsDark = typeof window.matchMedia === "function" ? window.matchMedia(DARK_THEME_QUERY).matches : false;
  applyTheme(document.documentElement, mode, systemIsDark);
}
