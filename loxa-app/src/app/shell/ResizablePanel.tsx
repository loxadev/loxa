import {
  useEffect,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from "react";

type ResizablePanelProps = {
  ariaLabel: string;
  children: ReactNode;
  className?: string;
  defaultWidth?: number;
  maxWidth: number;
  minWidth: number;
  onResize(width: number): void;
  side: "left" | "right";
  width: number;
};

const KEYBOARD_STEP = 16;

function clamp(width: number, minWidth: number, maxWidth: number) {
  return Math.min(maxWidth, Math.max(minWidth, width));
}

export function ResizablePanel({
  ariaLabel,
  children,
  className = "",
  defaultWidth,
  maxWidth,
  minWidth,
  onResize,
  side,
  width,
}: ResizablePanelProps) {
  const [resizing, setResizing] = useState(false);
  const handle = useRef<HTMLDivElement | null>(null);
  const drag = useRef<{ pointerId: number; startX: number; startWidth: number } | null>(null);
  const boundedWidth = clamp(width, minWidth, maxWidth);

  useEffect(() => {
    const move = (event: PointerEvent) => {
      const active = drag.current;
      if (!active || active.pointerId !== event.pointerId) return;
      const delta = event.clientX - active.startX;
      onResize(clamp(active.startWidth + (side === "left" ? delta : -delta), minWidth, maxWidth));
    };
    const finish = (event: PointerEvent) => {
      const active = drag.current;
      if (!active || active.pointerId !== event.pointerId) return;
      if (handle.current?.hasPointerCapture?.(active.pointerId)) {
        handle.current.releasePointerCapture(active.pointerId);
      }
      drag.current = null;
      setResizing(false);
    };

    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", finish);
    window.addEventListener("pointercancel", finish);
    return () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", finish);
      window.removeEventListener("pointercancel", finish);
    };
  }, [maxWidth, minWidth, onResize, side]);

  const startResize = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    drag.current = { pointerId: event.pointerId, startX: event.clientX, startWidth: boundedWidth };
    setResizing(true);
    try {
      event.currentTarget.setPointerCapture?.(event.pointerId);
    } catch {
      // Synthetic events and older webviews may not expose native pointer capture.
    }
    event.preventDefault();
  };

  const resizeWithKeyboard = (event: KeyboardEvent<HTMLDivElement>) => {
    let nextWidth: number | undefined;
    if (event.key === "Home") nextWidth = minWidth;
    if (event.key === "End") nextWidth = maxWidth;
    if (event.key === "ArrowLeft") nextWidth = boundedWidth + (side === "left" ? -KEYBOARD_STEP : KEYBOARD_STEP);
    if (event.key === "ArrowRight") nextWidth = boundedWidth + (side === "left" ? KEYBOARD_STEP : -KEYBOARD_STEP);
    if (nextWidth === undefined) return;
    onResize(clamp(nextWidth, minWidth, maxWidth));
    event.preventDefault();
  };

  const reset = () => {
    if (defaultWidth !== undefined) onResize(clamp(defaultWidth, minWidth, maxWidth));
  };

  return (
    <section
      className={`resizable-panel resizable-panel-${side} ${className}`.trim()}
      data-resizing={resizing || undefined}
      style={{ "--loxa-panel-width": `${boundedWidth}px` } as CSSProperties}
    >
      <div className="resizable-panel-content">{children}</div>
      <div
        ref={handle}
        className="resizable-panel-handle"
        role="separator"
        aria-label={ariaLabel}
        aria-orientation="vertical"
        aria-valuemin={minWidth}
        aria-valuemax={maxWidth}
        aria-valuenow={boundedWidth}
        tabIndex={0}
        onDoubleClick={reset}
        onKeyDown={resizeWithKeyboard}
        onPointerDown={startResize}
      />
    </section>
  );
}
