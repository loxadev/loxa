import { PanelLeftClose, PanelLeftOpen } from "lucide-react";
import type { ReactNode } from "react";

import { IconButton } from "../components/ui/button";
import { Tooltip } from "../components/ui/tooltip";
import {
  DEFAULT_CONVERSATION_RAIL_WIDTH,
  MAX_CONVERSATION_RAIL_WIDTH,
  MIN_CONVERSATION_RAIL_WIDTH,
  selectExpandedSidebarWidth,
  selectSetExpandedSidebarWidth,
  selectToggleSidebar,
  selectSidebarCollapsed,
  useWorkspaceStore,
} from "../stores/workspace-store";
import { ResizablePanel } from "./shell/ResizablePanel";
import { SidebarHeader } from "./SidebarHeader";
import { SidebarNavigation } from "./SidebarNavigation";
import { SidebarRuntimeStatus } from "./SidebarRuntimeStatus";

type AppSidebarProps = {
  brandMark: string;
  conversationRail?: ReactNode;
  runtimeHealth: string;
  runtimeModel: string;
};

export function AppSidebar({ brandMark, conversationRail, runtimeHealth, runtimeModel }: AppSidebarProps) {
  const collapsed = useWorkspaceStore(selectSidebarCollapsed);
  const width = useWorkspaceStore(selectExpandedSidebarWidth);
  const setWidth = useWorkspaceStore(selectSetExpandedSidebarWidth);
  const toggle = useWorkspaceStore(selectToggleSidebar);
  const toggleLabel = collapsed ? "Show conversations" : "Hide conversations";

  return (
    <aside className="app-sidebar" aria-label="Primary" data-collapsed={collapsed || undefined}>
      <div className="activity-rail">
        <SidebarHeader brandMark={brandMark} />
        <SidebarNavigation />
        <div className="activity-rail-spacer" />
        {collapsed ? (
          <SidebarRuntimeStatus health={runtimeHealth} model={runtimeModel} />
        ) : (
          <Tooltip content={`${runtimeHealth}. ${runtimeModel}`}>
            <SidebarRuntimeStatus health={runtimeHealth} model={runtimeModel} />
          </Tooltip>
        )}
        {conversationRail && collapsed ? (
          <Tooltip content={toggleLabel}>
            <IconButton className="conversation-toggle" variant="quiet" label={toggleLabel} onClick={toggle}>
              {collapsed ? <PanelLeftOpen aria-hidden="true" /> : <PanelLeftClose aria-hidden="true" />}
            </IconButton>
          </Tooltip>
        ) : null}
        <SidebarNavigation footer />
      </div>

      {conversationRail && !collapsed ? (
        <ResizablePanel
          ariaLabel="Resize conversation rail"
          className="conversation-panel"
          defaultWidth={DEFAULT_CONVERSATION_RAIL_WIDTH}
          minWidth={MIN_CONVERSATION_RAIL_WIDTH}
          maxWidth={MAX_CONVERSATION_RAIL_WIDTH}
          onResize={setWidth}
          side="left"
          width={width}
        >
          <div className="conversation-panel-header">
            <span>Chats</span>
            <IconButton variant="quiet" label="Hide conversations" onClick={toggle}>
              <PanelLeftClose aria-hidden="true" />
            </IconButton>
          </div>
          <div className="conversation-rail-slot">{conversationRail}</div>
        </ResizablePanel>
      ) : null}
    </aside>
  );
}
