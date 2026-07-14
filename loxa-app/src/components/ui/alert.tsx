import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/utils";

const alertVariants = cva("box-border grid w-full gap-1 rounded-lg border p-4 text-sm contrast-more:border-2", {
  variants: {
    variant: {
      neutral: "border-border bg-card text-card-foreground",
      info: "border-info-border bg-info-surface text-info-foreground",
      success: "border-success-border bg-success-surface text-success-foreground",
      warning: "border-warning-border bg-warning-surface text-warning-foreground",
      danger: "border-danger-border bg-danger-surface text-danger-foreground",
    },
  },
  defaultVariants: { variant: "neutral" },
});

function Alert({ className, variant, ...props }: React.ComponentProps<"div"> & VariantProps<typeof alertVariants>) {
  return <div data-slot="alert" role="alert" className={cn(alertVariants({ variant }), className)} {...props} />;
}

function AlertTitle({ className, ...props }: React.ComponentProps<"div">) {
  return <div data-slot="alert-title" className={cn("font-medium", className)} {...props} />;
}

function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return <div data-slot="alert-description" className={cn("text-sm", className)} {...props} />;
}

export { Alert, AlertDescription, AlertTitle };
