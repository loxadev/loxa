import type { ReactNode } from "react";

type EmptyStateProps = { title: string; description: string; action?: ReactNode };

function EmptyState({ action, description, title }: EmptyStateProps) {
  return (
    <section
      data-slot="empty-state"
      className="border-border bg-surface-subtle flex flex-col items-start gap-3 rounded-lg border border-dashed p-6"
    >
      <div className="space-y-1">
        <h2 className="text-foreground text-lg font-semibold">{title}</h2>
        <p className="text-muted-foreground text-sm">{description}</p>
      </div>
      {action !== undefined && action !== null ? (
        <div data-slot="empty-state-action" className="flex flex-wrap items-center gap-2">
          {action}
        </div>
      ) : null}
    </section>
  );
}

export { EmptyState };
export type { EmptyStateProps };
