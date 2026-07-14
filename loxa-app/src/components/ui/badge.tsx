import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/utils";

const badgeVariants = cva(
  "inline-flex w-fit items-center rounded-full border px-2 py-0.5 text-xs font-medium contrast-more:border-2",
  {
    variants: {
      variant: {
        neutral: "border-border bg-muted text-muted-foreground",
        info: "border-info-border bg-info-surface text-info-foreground",
        success: "border-success-border bg-success-surface text-success-foreground",
        warning: "border-warning-border bg-warning-surface text-warning-foreground",
        danger: "border-danger-border bg-danger-surface text-danger-foreground",
      },
    },
    defaultVariants: { variant: "neutral" },
  },
);

function Badge({ className, variant, ...props }: React.ComponentProps<"span"> & VariantProps<typeof badgeVariants>) {
  return <span data-slot="badge" className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge };
