import { selectActiveRoute, selectSidebarCollapsed, useWorkspaceStore } from "../stores/workspace-store";
import { SidebarHeader } from "./SidebarHeader";
import { SidebarNavigation } from "./SidebarNavigation";
import { SidebarResizeHandle } from "./SidebarResizeHandle";
import { SidebarRuntimeStatus } from "./SidebarRuntimeStatus";

type AppSidebarProps = {
  brandMark: string;
  runtimeHealth: string;
  runtimeModel: string;
  onConversationTargetChange: (target: HTMLDivElement | null) => void;
};

export function AppSidebar({ brandMark, runtimeHealth, runtimeModel, onConversationTargetChange }: AppSidebarProps) {
  const route = useWorkspaceStore(selectActiveRoute);
  const collapsed = useWorkspaceStore(selectSidebarCollapsed);

  return (
    <aside className="app-sidebar" aria-label="Primary" data-collapsed={collapsed || undefined}>
      <SidebarHeader brandMark={brandMark} />
      <SidebarNavigation />
      {route === "chat" && !collapsed ? (
        <div className="conversation-rail-slot" ref={onConversationTargetChange} />
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
