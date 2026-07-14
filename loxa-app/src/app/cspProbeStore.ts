export interface CspProbeRecord {
  effectiveDirective: string;
  blockedTarget: string;
  sourceBasename: string;
  line: number;
  column: number;
}

type ViolationInput = Pick<
  SecurityPolicyViolationEvent,
  "effectiveDirective" | "blockedURI" | "sourceFile" | "lineNumber" | "columnNumber"
>;

const EMPTY_SNAPSHOT: readonly CspProbeRecord[] = Object.freeze([]);
const listeners = new Set<() => void>();
let snapshot = EMPTY_SNAPSHOT;

function blockedTarget(value: string): string {
  if (value === "inline" || value === "eval") return value;
  if (value === "data") return "data";
  try {
    const url = new URL(value);
    if (!new Set(["http:", "https:", "ws:", "wss:"]).has(url.protocol) || !url.hostname) return "unknown";
    return `${url.protocol.toLowerCase()}//${url.hostname.toLowerCase()}/[redacted]`;
  } catch {
    return "unknown";
  }
}

function sourceBasename(value: string): string {
  const withoutSuffix = value.split(/[?#]/, 1)[0] ?? "";
  const segments = withoutSuffix.split(/[\\/]/).filter(Boolean);
  const basename = segments[segments.length - 1] ?? "";
  return /^[a-zA-Z0-9._-]{1,128}$/.test(basename) ? basename : "unknown";
}

function coordinate(value: number): number {
  return Number.isFinite(value) && value >= 0 ? Math.floor(value) : 0;
}

function directive(value: string): string {
  const normalized = value.toLowerCase();
  return /^[a-z0-9-]{1,64}$/.test(normalized) ? normalized : "unknown";
}

function emit() {
  listeners.forEach((listener) => listener());
}

export const cspProbeStore = {
  recordViolation(event: ViolationInput): void {
    const record = Object.freeze({
      effectiveDirective: directive(event.effectiveDirective),
      blockedTarget: blockedTarget(event.blockedURI),
      sourceBasename: sourceBasename(event.sourceFile),
      line: coordinate(event.lineNumber),
      column: coordinate(event.columnNumber),
    });
    snapshot = Object.freeze([...snapshot, record]);
    emit();
  },
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  getSnapshot(): readonly CspProbeRecord[] {
    return snapshot;
  },
  reset(): void {
    if (snapshot.length === 0) return;
    snapshot = EMPTY_SNAPSHOT;
    emit();
  },
  exportJson(): string {
    return JSON.stringify(snapshot);
  },
};
