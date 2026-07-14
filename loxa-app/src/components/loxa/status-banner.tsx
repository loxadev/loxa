import type { ReactNode } from "react";
import { Alert, AlertDescription, AlertTitle } from "../ui/alert";
import { hasRenderableContent } from "./renderable";
import type { StatusBadgeProps } from "./status-badge";

type StatusBannerProps = Omit<StatusBadgeProps, "children"> & {
  title: string;
  children?: ReactNode;
  role?: "status" | "alert";
};

function StatusBanner({ children, role = "status", title, tone }: StatusBannerProps) {
  return (
    <Alert data-slot="status-banner" role={role} variant={tone}>
      <AlertTitle>{title}</AlertTitle>
      {hasRenderableContent(children) ? <AlertDescription>{children}</AlertDescription> : null}
    </Alert>
  );
}

export { StatusBanner };
export type { StatusBannerProps };
