import { useEffect, useRef, type KeyboardEvent, type PointerEvent as ReactPointerEvent } from "react";

import {
  MAX_EXPANDED_SIDEBAR_WIDTH,
  MIN_EXPANDED_SIDEBAR_WIDTH,
  SIDEBAR_KEYBOARD_STEP,
  selectExpandedSidebarWidth,
  selectResetSidebarWidth,
  selectResizeSidebarBy,
  selectSetExpandedSidebarWidth,
  useWorkspaceStore,
} from "../stores/workspace-store";

export function SidebarResizeHandle() {
  const width = useWorkspaceStore(selectExpandedSidebarWidth);
  const setWidth = useWorkspaceStore(selectSetExpandedSidebarWidth);
  const resizeBy = useWorkspaceStore(selectResizeSidebarBy);
  const resetWidth = useWorkspaceStore(selectResetSidebarWidth);
  const handle = useRef<HTMLDivElement | null>(null);
  const pointerDrag = useRef<{ pointerId: number; startX: number; startWidth: number } | null>(null);

  useEffect(() => {
    const element = handle.current;
    const move = (event: PointerEvent) => {
      const active = pointerDrag.current;
      if (!active || event.pointerId !== active.pointerId) return;
      setWidth(active.startWidth + event.clientX - active.startX);
    };
    const finish = (event: PointerEvent) => {
      const active = pointerDrag.current;
      if (!active || event.pointerId !== active.pointerId) return;
      if (element?.hasPointerCapture?.(active.pointerId)) {
        element.releasePointerCapture(active.pointerId);
      }
      pointerDrag.current = null;
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", finish);
    window.addEventListener("pointercancel", finish);
    return () => {
      const active = pointerDrag.current;
      if (active && element?.hasPointerCapture?.(active.pointerId)) {
        element.releasePointerCapture(active.pointerId);
      }
      pointerDrag.current = null;
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", finish);
      window.removeEventListener("pointercancel", finish);
    };
  }, [setWidth]);

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "ArrowLeft") resizeBy(-SIDEBAR_KEYBOARD_STEP);
    else if (event.key === "ArrowRight") resizeBy(SIDEBAR_KEYBOARD_STEP);
    else if (event.key === "Home") setWidth(MIN_EXPANDED_SIDEBAR_WIDTH);
    else if (event.key === "End") setWidth(MAX_EXPANDED_SIDEBAR_WIDTH);
    else return;
    event.preventDefault();
  };

  const onPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    pointerDrag.current = { pointerId: event.pointerId, startX: event.clientX, startWidth: width };
    event.currentTarget.setPointerCapture?.(event.pointerId);
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
      aria-valuetext={`${width} pixels`}
      tabIndex={0}
      onKeyDown={onKeyDown}
      onPointerDown={onPointerDown}
      onDoubleClick={resetWidth}
    />
  );
}
