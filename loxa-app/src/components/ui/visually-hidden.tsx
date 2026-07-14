import * as React from "react";

import { cn } from "../../lib/utils";

function VisuallyHidden({ className, ...props }: React.ComponentProps<"span">) {
  return <span data-slot="visually-hidden" className={cn("sr-only", className)} {...props} />;
}

export { VisuallyHidden };
