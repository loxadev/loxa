import * as React from "react";

import { cn } from "../../lib/utils";

type ProgressProps = Omit<React.ComponentProps<"progress">, "max" | "value"> & {
  total: number;
  value: number;
};

function Progress({ className, total, value, ...props }: ProgressProps) {
  return <progress data-slot="progress" className={cn("h-2 w-full", className)} max={total} value={value} {...props} />;
}

export { Progress };
export type { ProgressProps };
