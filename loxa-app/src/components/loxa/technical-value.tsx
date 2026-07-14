import type { ComponentPropsWithoutRef } from "react";
import { cn } from "../../lib/utils";

type TechnicalValueProps = ComponentPropsWithoutRef<"code">;

function TechnicalValue({ className, ...props }: TechnicalValueProps) {
  return (
    <code
      data-slot="technical-value"
      className={cn("text-foreground block max-w-full font-mono text-sm break-all whitespace-normal", className)}
      {...props}
    />
  );
}

export { TechnicalValue };
export type { TechnicalValueProps };
