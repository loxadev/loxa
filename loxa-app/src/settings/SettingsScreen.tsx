import { useEffect, useRef } from "react";

import { selectActiveSettingsPage, selectSetActiveSettingsPage, useWorkspaceStore } from "../stores/workspace-store";
import { RuntimeSettingsScreen, type RuntimeFacts } from "./RuntimeSettingsScreen";
import { SettingsOverview } from "./SettingsOverview";
import type { ThemeMode } from "./theme";

export function SettingsScreen({
  theme,
  onThemeChange,
  runtime,
  onClearChatHistory,
}: {
  theme: ThemeMode;
  onThemeChange: (mode: ThemeMode) => void;
  runtime: RuntimeFacts;
  onClearChatHistory?: (signal: AbortSignal) => Promise<number>;
}) {
  const activePage = useWorkspaceStore(selectActiveSettingsPage);
  const setActivePage = useWorkspaceStore(selectSetActiveSettingsPage);
  const previousPage = useRef(activePage);
  const overviewHeading = useRef<HTMLHeadingElement>(null);
  const runtimeHeading = useRef<HTMLHeadingElement>(null);

  useEffect(() => {
    if (previousPage.current === activePage) return;
    previousPage.current = activePage;
    (activePage === "runtime" ? runtimeHeading : overviewHeading).current?.focus();
  }, [activePage]);

  return activePage === "runtime" ? (
    <RuntimeSettingsScreen runtime={runtime} headingRef={runtimeHeading} onBack={() => setActivePage("overview")} />
  ) : (
    <SettingsOverview
      theme={theme}
      onThemeChange={onThemeChange}
      onClearChatHistory={onClearChatHistory}
      headingRef={overviewHeading}
      onOpenRuntime={() => setActivePage("runtime")}
    />
  );
}
