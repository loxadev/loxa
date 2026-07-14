import * as React from "react";

import { cn } from "../../lib/utils";

function Separator({ className, ...props }: React.ComponentProps<"hr">) {
  return <hr data-slot="separator" className={cn("border-border w-full border-0 border-t", className)} {...props} />;
}

export { Separator };
