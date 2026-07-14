import type { ComponentPropsWithoutRef } from "react";
import { Button } from "../ui/button";

type AsyncActionProps = ComponentPropsWithoutRef<typeof Button> & { pendingLabel: string };

function AsyncAction({ busy = false, children, pendingLabel, ...props }: AsyncActionProps) {
  return (
    <Button {...props} busy={busy}>
      {busy ? pendingLabel : children}
    </Button>
  );
}

export { AsyncAction };
export type { AsyncActionProps };
