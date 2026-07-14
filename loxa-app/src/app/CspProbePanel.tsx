import { useReducer, useSyncExternalStore } from "react";

import { cspProbeStore, serializeEvidence } from "./cspProbeStore";

export function CspProbePanel() {
  const evidence = useSyncExternalStore(cspProbeStore.subscribe, cspProbeStore.getEvidenceSnapshot);
  const [, refreshEvidence] = useReducer((value: number) => value + 1, 0);
  const records = evidence.cspViolations;
  const { warn, error } = evidence.consoleCounts;

  return (
    <section className="csp-probe-panel" aria-label="Packaged probe evidence">
      <div className="csp-probe-heading">
        <strong>Packaged probe evidence</strong>
        <div className="csp-probe-actions">
          <button type="button" onClick={cspProbeStore.clearViolations}>
            Reset CSP
          </button>
          <button type="button" onClick={refreshEvidence}>
            Refresh evidence
          </button>
        </div>
      </div>
      <p role="status" aria-live="polite">
        CSP violations: {records.length}; console warnings: {warn}; console errors: {error}
      </p>
      {evidence.cspViolations.length > 0 && (
        <ol className="csp-probe-records">
          {evidence.cspViolations.map((record, index) => (
            <li key={`${record.effectiveDirective}-${record.line}-${record.column}-${index}`}>
              <code>
                {record.effectiveDirective} {record.blockedTarget} {record.sourceBasename}:{record.line}:{record.column}
              </code>
            </li>
          ))}
        </ol>
      )}
      <label className="csp-probe-json-label" htmlFor="loxa-probe-json">
        Sanitized probe JSON
      </label>
      <textarea
        id="loxa-probe-json"
        className="csp-probe-json"
        readOnly
        spellCheck={false}
        value={serializeEvidence(evidence)}
      />
    </section>
  );
}
