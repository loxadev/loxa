import { useId } from "react";
import { Progress } from "../ui/progress";

type OperationProgressProps = { label: string; value?: number; total?: number; detail?: string };

function OperationProgress({ detail, label, total, value }: OperationProgressProps) {
  const labelId = useId();
  const detailId = useId();
  const isDeterminate = value !== undefined && total !== undefined;

  return (
    <div data-slot="operation-progress" className="space-y-2">
      <div className="flex flex-wrap items-baseline justify-between gap-2 text-sm">
        <span id={labelId} className="text-foreground font-medium">
          {label}
        </span>
        {detail ? (
          <span id={detailId} className="text-muted-foreground break-all">
            {detail}
          </span>
        ) : null}
      </div>
      <Progress
        aria-labelledby={labelId}
        aria-describedby={detail ? detailId : undefined}
        value={isDeterminate ? value : undefined}
        total={isDeterminate ? total : undefined}
      />
    </div>
  );
}

export { OperationProgress };
export type { OperationProgressProps };
