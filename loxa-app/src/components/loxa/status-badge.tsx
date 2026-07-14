import type { ReactNode } from "react";
import { Badge } from "../ui/badge";

type StatusBadgeProps = {
  tone: "neutral" | "info" | "success" | "warning" | "danger";
  children: ReactNode;
};

function StatusBadge({ children, tone }: StatusBadgeProps) {
  return (
    <Badge data-slot="status-badge" data-variant={tone} variant={tone}>
      {children}
    </Badge>
  );
}

export { StatusBadge };
export type { StatusBadgeProps };
