import type { CSSProperties, ReactNode } from "react";

import { selectEffectiveSidebarWidth, useWorkspaceStore } from "../stores/workspace-store";
import { AppSidebar } from "./AppSidebar";

type AppShellProps = {
  brandMark: string;
  children: ReactNode;
  runtimeHealth: string;
  runtimeModel: string;
  onConversationTargetChange: (target: HTMLDivElement | null) => void;
};

export function AppShell({
  brandMark,
  children,
  runtimeHealth,
  runtimeModel,
  onConversationTargetChange,
}: AppShellProps) {
  const effectiveWidth = useWorkspaceStore(selectEffectiveSidebarWidth);

  return (
    <div
      className="app-shell"
      data-testid="app-shell"
      style={{ "--loxa-sidebar-width": `${effectiveWidth}px` } as CSSProperties}
    >
      <AppSidebar
        brandMark={brandMark}
        runtimeHealth={runtimeHealth}
        runtimeModel={runtimeModel}
        onConversationTargetChange={onConversationTargetChange}
      />
      <main className="workspace workspace-canvas">
        <div className="workspace-frame">{children}</div>
      </main>
    </div>
  );
}
