import { useSyncExternalStore } from "react";

import { cspProbeStore } from "./cspProbeStore";

function exportSnapshot() {
  const blob = new Blob([cspProbeStore.exportJson()], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.download = "loxa-csp-probe.json";
  link.href = url;
  link.click();
  URL.revokeObjectURL(url);
}

export function CspProbePanel() {
  const records = useSyncExternalStore(cspProbeStore.subscribe, cspProbeStore.getSnapshot);

  return (
    <section className="csp-probe-panel" role="status" aria-live="polite" aria-label="CSP probe">
      <div className="csp-probe-heading">
        <strong>
          CSP probe: {records.length} {records.length === 1 ? "violation" : "violations"}
        </strong>
        <div className="csp-probe-actions">
          <button type="button" onClick={cspProbeStore.reset}>
            Reset
          </button>
          <button type="button" onClick={exportSnapshot}>
            Export JSON
          </button>
        </div>
      </div>
      {records.length > 0 && (
        <ol className="csp-probe-records">
          {records.map((record, index) => (
            <li key={`${record.effectiveDirective}-${record.line}-${record.column}-${index}`}>
              <code>
                {record.effectiveDirective} {record.blockedTarget} {record.sourceBasename}:{record.line}:{record.column}
              </code>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
