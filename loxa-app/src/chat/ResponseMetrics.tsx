import { CircleStop, Clock3, Gauge, Timer, WholeWord } from "lucide-react";
import type React from "react";

import { Badge } from "../components/ui/badge";
import type { ChatTurnMetrics } from "./turnMetrics";
import { deriveTurnMetricView } from "./turnMetrics";
import styles from "./TurnMetrics.module.css";

export function ResponseMetrics({ metrics }: { metrics?: ChatTurnMetrics | null }) {
  const view = deriveTurnMetricView(metrics);
  if (!view) return null;

  return (
    <div className={styles.metrics} role="group" aria-label="Response metrics">
      {view.tokensPerSecond !== null && <Metric icon={<Gauge />} label={`${view.tokensPerSecond.toFixed(2)} tok/s`} />}
      {view.outputTokens !== null && (
        <Metric icon={<WholeWord />} label={`${view.outputTokens} ${view.outputTokens === 1 ? "token" : "tokens"}`} />
      )}
      {view.totalDurationMs !== null && (
        <Metric icon={<Clock3 />} label={`${(view.totalDurationMs / 1_000).toFixed(2)}s`} />
      )}
      {view.ttftMs !== null && <Metric icon={<Timer />} label={`TTFT ${formatDuration(view.ttftMs)}`} />}
      {view.stopReason !== null && <Metric icon={<CircleStop />} label={`Stop reason: ${view.stopReason}`} />}
    </div>
  );
}

function Metric({ icon, label }: { icon: React.ReactElement; label: string }) {
  return (
    <Badge className={styles.metric} variant="neutral">
      <span className={styles.metricIcon} aria-hidden="true">
        {icon}
      </span>
      {label}
    </Badge>
  );
}

function formatDuration(durationMs: number): string {
  if (durationMs < 1_000) return `${Math.round(durationMs)}ms`;
  return `${(durationMs / 1_000).toFixed(2)}s`;
}
