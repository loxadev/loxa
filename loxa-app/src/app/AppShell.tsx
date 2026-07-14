import type { CSSProperties, ReactNode } from "react";

import { selectEffectiveSidebarWidth, useWorkspaceStore } from "../stores/workspace-store";
import { AppSidebar } from "./AppSidebar";

type AppShellProps = {
  brandMark: string;
  children: ReactNode;
  conversationRail?: ReactNode;
  runtimeHealth: string;
  runtimeModel: string;
};

export function AppShell({ brandMark, children, conversationRail, runtimeHealth, runtimeModel }: AppShellProps) {
  const effectiveWidth = useWorkspaceStore(selectEffectiveSidebarWidth);

  return (
    <div
      className="app-shell"
      data-testid="app-shell"
      style={{ "--loxa-sidebar-width": `${effectiveWidth}px` } as CSSProperties}
    >
      <AppSidebar
        brandMark={brandMark}
        conversationRail={conversationRail}
        runtimeHealth={runtimeHealth}
        runtimeModel={runtimeModel}
      />
      <main className="workspace workspace-canvas">
        <div className="workspace-frame">{children}</div>
      </main>
    </div>
  );
}
