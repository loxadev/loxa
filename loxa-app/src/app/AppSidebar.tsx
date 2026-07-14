import type { ReactNode } from "react";

import { selectSidebarCollapsed, useWorkspaceStore } from "../stores/workspace-store";
import { SidebarHeader } from "./SidebarHeader";
import { SidebarNavigation } from "./SidebarNavigation";
import { SidebarResizeHandle } from "./SidebarResizeHandle";
import { SidebarRuntimeStatus } from "./SidebarRuntimeStatus";

type AppSidebarProps = {
  brandMark: string;
  conversationRail?: ReactNode;
  runtimeHealth: string;
  runtimeModel: string;
};

export function AppSidebar({ brandMark, conversationRail, runtimeHealth, runtimeModel }: AppSidebarProps) {
  const collapsed = useWorkspaceStore(selectSidebarCollapsed);

  return (
    <aside className="app-sidebar" aria-label="Primary" data-collapsed={collapsed || undefined}>
      <SidebarHeader brandMark={brandMark} />
      <SidebarNavigation />
      {conversationRail ? (
        <div className="conversation-rail-slot" hidden={collapsed}>
          {conversationRail}
        </div>
      ) : (
        <div className="sidebar-spacer" />
      )}
      <div className="sidebar-footer">
        <SidebarRuntimeStatus health={runtimeHealth} model={runtimeModel} />
        <SidebarNavigation footer />
      </div>
      {!collapsed ? <SidebarResizeHandle /> : null}
    </aside>
  );
}
