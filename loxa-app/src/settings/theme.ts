import { useCallback, useEffect, useState } from "react";

import {
  applyTheme,
  DARK_THEME_QUERY,
  isThemeMode,
  readThemePreference,
  writeThemePreference,
  type ThemeMode,
} from "./themeRuntime";

export * from "./themeRuntime";

function getDarkPreference(): MediaQueryList | null {
  return typeof window.matchMedia === "function" ? window.matchMedia(DARK_THEME_QUERY) : null;
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
    if (!isThemeMode(next)) return;
    const media = getDarkPreference();
    applyTheme(document.documentElement, next, media?.matches ?? false);
    setMode(next);
    persistMode(next);
  }, []);

  return [mode, selectMode] as const;
}
