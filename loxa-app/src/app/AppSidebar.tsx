import { useEffect, useRef, type KeyboardEvent, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";

import {
  MAX_EXPANDED_SIDEBAR_WIDTH,
  MIN_EXPANDED_SIDEBAR_WIDTH,
  SIDEBAR_KEYBOARD_STEP,
  selectExpandedSidebarWidth,
  selectResizeSidebarBy,
  selectSetExpandedSidebarWidth,
  selectSetSidebarCollapsed,
  selectSidebarCollapsed,
  selectToggleSidebar,
  useWorkspaceStore,
} from "../stores/workspace-store";
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
      <SidebarDivider />
    </aside>
  );
}

const SIDEBAR_COLLAPSE_THRESHOLD = 48;

function SidebarDivider() {
  const collapsed = useWorkspaceStore(selectSidebarCollapsed);
  const width = useWorkspaceStore(selectExpandedSidebarWidth);
  const setCollapsed = useWorkspaceStore(selectSetSidebarCollapsed);
  const toggle = useWorkspaceStore(selectToggleSidebar);
  const setWidth = useWorkspaceStore(selectSetExpandedSidebarWidth);
  const resizeBy = useWorkspaceStore(selectResizeSidebarBy);
  const handle = useRef<HTMLDivElement | null>(null);
  const pointerDrag = useRef<{
    pointerId: number;
    startX: number;
    startWidth: number;
    startedCollapsed: boolean;
  } | null>(null);

  useEffect(() => {
    const element = handle.current;
    const move = (event: PointerEvent) => {
      const active = pointerDrag.current;
      if (!active || event.pointerId !== active.pointerId) return;
      const delta = event.clientX - active.startX;
      if (active.startedCollapsed) {
        if (delta >= SIDEBAR_COLLAPSE_THRESHOLD) setCollapsed(false);
      } else if (delta <= -SIDEBAR_COLLAPSE_THRESHOLD) {
        setCollapsed(true);
      } else {
        setWidth(active.startWidth + delta);
      }
    };
    const finish = (event: PointerEvent) => {
      const active = pointerDrag.current;
      if (!active || event.pointerId !== active.pointerId) return;
      if (element?.hasPointerCapture?.(active.pointerId)) element.releasePointerCapture(active.pointerId);
      pointerDrag.current = null;
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", finish);
    window.addEventListener("pointercancel", finish);
    return () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", finish);
      window.removeEventListener("pointercancel", finish);
    };
  }, [setCollapsed, setWidth]);

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Enter" || event.key === " ") toggle();
    else if (event.key === "ArrowLeft") {
      if (!collapsed) resizeBy(-SIDEBAR_KEYBOARD_STEP);
    } else if (event.key === "ArrowRight") {
      if (collapsed) setCollapsed(false);
      else resizeBy(SIDEBAR_KEYBOARD_STEP);
    } else if (event.key === "Home") setWidth(MIN_EXPANDED_SIDEBAR_WIDTH);
    else if (event.key === "End") setWidth(MAX_EXPANDED_SIDEBAR_WIDTH);
    else return;
    event.preventDefault();
  };

  const onPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    pointerDrag.current = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startWidth: width,
      startedCollapsed: collapsed,
    };
    try {
      event.currentTarget.setPointerCapture?.(event.pointerId);
    } catch {
      // Synthetic pointer events do not always register as active native pointers.
    }
    event.preventDefault();
  };

  return (
    <div
      ref={handle}
      className="sidebar-resize-handle"
      role="separator"
      aria-label="Resize navigation and conversation rail"
      aria-orientation="vertical"
      aria-valuemin={MIN_EXPANDED_SIDEBAR_WIDTH}
      aria-valuemax={MAX_EXPANDED_SIDEBAR_WIDTH}
      aria-valuenow={width}
      aria-valuetext={collapsed ? "Collapsed" : `${width} pixels`}
      data-collapsed={collapsed || undefined}
      tabIndex={0}
      onKeyDown={onKeyDown}
      onPointerDown={onPointerDown}
      onDoubleClick={toggle}
    />
  );
}
