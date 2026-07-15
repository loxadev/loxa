import { cloneElement, useId, useRef, useState, type ReactElement, type ReactNode } from "react";

type TooltipProps = {
  children: ReactElement<{ "aria-describedby"?: string }>;
  content: ReactNode;
  id?: string;
  side?: "right" | "top";
};

function Tooltip({ children, content, id: providedId, side = "right" }: TooltipProps) {
  const generatedId = useId();
  const id = providedId ?? generatedId;
  const anchorRef = useRef<HTMLSpanElement>(null);
  const focusWithin = useRef(false);
  const pointerWithin = useRef(false);
  const [open, setOpen] = useState(false);
  const describedBy = [...new Set([children.props["aria-describedby"], id].filter(Boolean))].join(" ");

  return (
    <span
      className="tooltip-anchor"
      onBlurCapture={(event) => {
        if (event.relatedTarget instanceof Node && anchorRef.current?.contains(event.relatedTarget)) return;
        focusWithin.current = false;
        if (!pointerWithin.current) setOpen(false);
      }}
      onFocusCapture={() => {
        focusWithin.current = true;
        setOpen(true);
      }}
      onKeyDownCapture={(event) => {
        if (event.key === "Escape" && open) setOpen(false);
      }}
      onPointerEnter={() => {
        pointerWithin.current = true;
        setOpen(true);
      }}
      onPointerLeave={() => {
        pointerWithin.current = false;
        if (!focusWithin.current) setOpen(false);
      }}
      ref={anchorRef}
    >
      {cloneElement(children, { "aria-describedby": describedBy })}
      <span className="tooltip-content" data-open={open ? "true" : undefined} data-side={side} id={id} role="tooltip">
        {content}
      </span>
    </span>
  );
}

export { Tooltip };
