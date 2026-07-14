import type { ReactNode } from "react";
import type { StatusBadgeProps } from "./status-badge";
import { StatusBadge } from "./status-badge";

type RuntimeStatusProps = {
  label: string;
  detail?: string;
  tone: StatusBadgeProps["tone"];
  action?: ReactNode;
};

function RuntimeStatus({ action, detail, label, tone }: RuntimeStatusProps) {
  return (
    <section data-slot="runtime-status" className="flex flex-wrap items-center justify-between gap-3">
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <StatusBadge tone={tone}>{label}</StatusBadge>
        {detail ? <p className="text-muted-foreground min-w-0 text-sm">{detail}</p> : null}
      </div>
      {action !== undefined && action !== null ? (
        <div data-slot="runtime-status-action" className="flex flex-wrap items-center gap-2">
          {action}
        </div>
      ) : null}
    </section>
  );
}

export { RuntimeStatus };
export type { RuntimeStatusProps };
