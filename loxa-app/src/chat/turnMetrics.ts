export type ChatTurnMetrics = {
  outputTokens: number | null;
  totalDurationMs: number | null;
  ttftMs: number | null;
  stopReason: string | null;
};

export type TurnMetricView = {
  outputTokens: number | null;
  totalDurationMs: number | null;
  ttftMs: number | null;
  stopReason: string | null;
  tokensPerSecond: number | null;
};

export function emptyTurnMetrics(): ChatTurnMetrics {
  return { outputTokens: null, totalDurationMs: null, ttftMs: null, stopReason: null };
}

export function deriveTurnMetricView(metrics?: ChatTurnMetrics | null): TurnMetricView | null {
  if (!metrics) return null;

  const outputTokens = isNonNegativeInteger(metrics.outputTokens) ? metrics.outputTokens : null;
  const totalDurationMs = isNonNegativeFinite(metrics.totalDurationMs) ? metrics.totalDurationMs : null;
  const ttftMs =
    isNonNegativeFinite(metrics.ttftMs) && totalDurationMs !== null && metrics.ttftMs <= totalDurationMs
      ? metrics.ttftMs
      : null;
  const stopReason = normaliseStopReason(metrics.stopReason);
  const decodeDurationMs = totalDurationMs !== null && ttftMs !== null ? totalDurationMs - ttftMs : null;
  const tokensPerSecond =
    outputTokens !== null && outputTokens > 0 && decodeDurationMs !== null && decodeDurationMs > 0
      ? outputTokens / (decodeDurationMs / 1_000)
      : null;

  if (
    outputTokens === null &&
    totalDurationMs === null &&
    ttftMs === null &&
    stopReason === null &&
    tokensPerSecond === null
  ) {
    return null;
  }

  return { outputTokens, totalDurationMs, ttftMs, stopReason, tokensPerSecond };
}

function isNonNegativeInteger(value: number | null): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isNonNegativeFinite(value: number | null): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function normaliseStopReason(value: string | null): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}
