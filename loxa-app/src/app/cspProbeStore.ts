export interface CspProbeRecord {
  effectiveDirective: string;
  blockedTarget: string;
  sourceBasename: string;
  line: number;
  column: number;
}

export type ConsoleProbeCategory = "warn" | "error";

export interface CspProbeEvidence {
  schemaVersion: 1;
  cspViolations: readonly CspProbeRecord[];
  consoleCounts: Readonly<Record<ConsoleProbeCategory, number>>;
}

type ViolationInput = Pick<
  SecurityPolicyViolationEvent,
  "effectiveDirective" | "blockedURI" | "sourceFile" | "lineNumber" | "columnNumber"
>;

const EMPTY_SNAPSHOT: readonly CspProbeRecord[] = /* @__PURE__ */ Object.freeze([]);
const EMPTY_CONSOLE_COUNTS = /* @__PURE__ */ Object.freeze({ warn: 0, error: 0 });
const listeners = new Set<() => void>();
let snapshot = EMPTY_SNAPSHOT;
let consoleCounts: Readonly<Record<ConsoleProbeCategory, number>> = EMPTY_CONSOLE_COUNTS;
let evidenceSnapshot = /* @__PURE__ */ createEvidenceSnapshot();

function createEvidenceSnapshot(): CspProbeEvidence {
  return Object.freeze({
    schemaVersion: 1 as const,
    cspViolations: snapshot,
    consoleCounts,
  });
}

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
  if (/[\\/]$/.test(withoutSuffix)) return "unknown";
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

function increment(value: number): number {
  return value < Number.MAX_SAFE_INTEGER ? value + 1 : Number.MAX_SAFE_INTEGER;
}

export function serializeEvidence(snapshotToSerialize = evidenceSnapshot): string {
  return JSON.stringify(snapshotToSerialize);
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
    evidenceSnapshot = createEvidenceSnapshot();
    emit();
  },
  recordConsole(category: unknown): void {
    if (category !== "warn" && category !== "error") return;
    consoleCounts = Object.freeze({
      warn: category === "warn" ? increment(consoleCounts.warn) : consoleCounts.warn,
      error: category === "error" ? increment(consoleCounts.error) : consoleCounts.error,
    });
    evidenceSnapshot = createEvidenceSnapshot();
  },
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  getSnapshot(): readonly CspProbeRecord[] {
    return snapshot;
  },
  getEvidenceSnapshot(): CspProbeEvidence {
    return evidenceSnapshot;
  },
  clearViolations(): void {
    if (snapshot.length === 0) return;
    snapshot = EMPTY_SNAPSHOT;
    evidenceSnapshot = createEvidenceSnapshot();
    emit();
  },
  reset(): void {
    if (snapshot.length === 0 && consoleCounts.warn === 0 && consoleCounts.error === 0) return;
    snapshot = EMPTY_SNAPSHOT;
    consoleCounts = EMPTY_CONSOLE_COUNTS;
    evidenceSnapshot = createEvidenceSnapshot();
    emit();
  },
  exportJson(): string {
    return JSON.stringify(snapshot);
  },
};
