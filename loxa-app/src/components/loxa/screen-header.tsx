import type { ReactNode } from "react";
import { hasRenderableContent } from "./renderable";

type ScreenHeaderProps = { eyebrow: string; title: string; summary?: string; actions?: ReactNode };

function ScreenHeader({ actions, eyebrow, summary, title }: ScreenHeaderProps) {
  return (
    <header data-slot="screen-header" className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0 flex-1 space-y-2">
        <p className="text-muted-foreground font-mono text-xs font-medium tracking-widest uppercase">{eyebrow}</p>
        <h1 className="text-foreground text-3xl font-semibold tracking-tight break-words">{title}</h1>
        {summary ? (
          <p data-slot="screen-header-summary" className="text-muted-foreground max-w-3xl text-sm">
            {summary}
          </p>
        ) : null}
      </div>
      {hasRenderableContent(actions) ? (
        <div data-slot="screen-header-actions" className="flex flex-wrap items-center gap-2">
          {actions}
        </div>
      ) : null}
    </header>
  );
}

export { ScreenHeader };
export type { ScreenHeaderProps };
