import * as React from "react";

import { cn } from "../../lib/utils";

type ProgressProps = Omit<React.ComponentProps<"progress">, "max" | "value"> & {
  total?: number;
  value?: number;
};

function Progress({ className, total, value, ...props }: ProgressProps) {
  return <progress {...props} data-slot="progress" className={cn("h-2 w-full", className)} max={total} value={value} />;
}

export { Progress };
export type { ProgressProps };
