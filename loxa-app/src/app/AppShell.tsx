import { useState, type CSSProperties, type ReactNode } from "react";

import {
  ACTIVITY_RAIL_WIDTH,
  selectExpandedSidebarWidth,
  selectSidebarCollapsed,
  useWorkspaceStore,
} from "../stores/workspace-store";
import { AppSidebar } from "./AppSidebar";
import { ResizablePanel } from "./shell/ResizablePanel";

const DEFAULT_INSPECTOR_WIDTH = 320;
const MIN_INSPECTOR_WIDTH = 280;
const MAX_INSPECTOR_WIDTH = 420;

type AppShellProps = {
  brandMark: string;
  children: ReactNode;
  conversationRail?: ReactNode;
  inspector?: ReactNode;
  workspaceHeader?: ReactNode;
  runtimeHealth: string;
  runtimeModel: string;
};

export function AppShell({
  brandMark,
  children,
  conversationRail,
  inspector,
  runtimeHealth,
  runtimeModel,
  workspaceHeader,
}: AppShellProps) {
  const conversationWidth = useWorkspaceStore(selectExpandedSidebarWidth);
  const conversationCollapsed = useWorkspaceStore(selectSidebarCollapsed);
  const [inspectorWidth, setInspectorWidth] = useState(DEFAULT_INSPECTOR_WIDTH);
  const sidebarWidth = ACTIVITY_RAIL_WIDTH + (conversationRail && !conversationCollapsed ? conversationWidth : 0);

  return (
    <div
      className="app-shell"
      data-testid="app-shell"
      style={
        {
          "--loxa-activity-rail-width": `${ACTIVITY_RAIL_WIDTH}px`,
          "--loxa-conversation-rail-width": `${conversationWidth}px`,
          "--loxa-sidebar-width": `${sidebarWidth}px`,
        } as CSSProperties
      }
    >
      <AppSidebar
        brandMark={brandMark}
        conversationRail={conversationRail}
        runtimeHealth={runtimeHealth}
        runtimeModel={runtimeModel}
      />
      <main className="workspace workspace-canvas">
        {workspaceHeader ? <header className="workspace-topbar">{workspaceHeader}</header> : null}
        <div className="workspace-frame">{children}</div>
      </main>
      {inspector ? (
        <ResizablePanel
          ariaLabel="Resize observability inspector"
          className="inspector-panel"
          defaultWidth={DEFAULT_INSPECTOR_WIDTH}
          minWidth={MIN_INSPECTOR_WIDTH}
          maxWidth={MAX_INSPECTOR_WIDTH}
          onResize={setInspectorWidth}
          side="right"
          width={inspectorWidth}
        >
          <aside
            id="observability-inspector"
            className="workspace-inspector"
            aria-label="Health and observability inspector"
          >
            {inspector}
          </aside>
        </ResizablePanel>
      ) : null}
    </div>
  );
}
