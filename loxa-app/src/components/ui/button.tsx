import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/utils";

const buttonVariants = cva(
  "inline-flex min-h-interactive min-w-interactive items-center justify-center gap-2 rounded-md border border-transparent px-4 text-sm font-medium transition-colors [transition-duration:var(--loxa-motion-fast)] focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        primary: "bg-primary text-primary-foreground hover:bg-primary/80",
        secondary: "border-border bg-secondary text-secondary-foreground hover:bg-muted",
        quiet: "bg-transparent text-foreground hover:bg-muted",
        danger: "bg-destructive text-destructive-foreground hover:bg-destructive/80",
      },
      size: {
        default: "min-h-interactive px-4",
        icon: "size-interactive p-0",
      },
    },
    defaultVariants: {
      variant: "primary",
      size: "default",
    },
  },
);

type ButtonProps = React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & {
    busy?: boolean;
  };

function Button({ className, variant, size, busy = false, disabled, type = "button", ...props }: ButtonProps) {
  return (
    <button
      {...props}
      type={type}
      data-slot="button"
      data-variant={variant ?? "primary"}
      data-size={size ?? "default"}
      aria-busy={busy || undefined}
      className={cn(buttonVariants({ variant, size }), className)}
      disabled={busy || disabled}
    />
  );
}

type IconButtonProps = Omit<ButtonProps, "aria-describedby" | "aria-label" | "children" | "size"> & {
  children: React.ReactElement;
  helpId: string;
  label: string;
};

function IconButton({ children, helpId, label, ...props }: IconButtonProps) {
  if (!label.trim()) throw new Error("IconButton label must not be empty");
  if (!helpId.trim()) throw new Error("IconButton helpId must not be empty");
  const decorativeIcon = React.cloneElement(children as React.ReactElement<Record<string, unknown>>, {
    "aria-hidden": true,
    focusable: "false",
  });

  return (
    <Button {...props} size="icon" aria-label={label} aria-describedby={helpId}>
      {decorativeIcon}
    </Button>
  );
}

export { Button, IconButton };
export type { ButtonProps, IconButtonProps };
