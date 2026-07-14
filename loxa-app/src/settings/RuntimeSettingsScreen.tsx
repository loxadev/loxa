import { ArrowLeft } from "lucide-react";
import type { RefObject } from "react";

import type { NodeSessionPhase } from "../node/NodeSession";
import type { NodeStatus } from "../node/contracts";
import type { NodeOwnership } from "../node/machine";
import styles from "./SettingsScreen.module.css";

export type RuntimeFacts = {
  phase: NodeSessionPhase;
  endpoint: string;
  ownership: NodeOwnership;
  status: NodeStatus | null;
};

export function RuntimeSettingsScreen({
  runtime,
  headingRef,
  onBack,
}: {
  runtime: RuntimeFacts;
  headingRef: RefObject<HTMLHeadingElement | null>;
  onBack: () => void;
}) {
  return (
    <section className={styles.screen} aria-labelledby="runtime-heading">
      <button className={`${styles.backButton} quiet-button interactive-target`} type="button" onClick={onBack}>
        <ArrowLeft aria-hidden="true" focusable="false" />
        Back to Settings
      </button>
      <header className="screen-header">
        <div>
          <p className="eyebrow">Settings</p>
          <h1 id="runtime-heading" ref={headingRef} tabIndex={-1}>
            Runtime
          </h1>
        </div>
      </header>

      <section className={styles.group} aria-labelledby="local-runtime-heading">
        <h2 id="local-runtime-heading">Local node/runtime</h2>
        <p className={styles.description}>Read-only facts from the shared authenticated node session.</p>
        <dl className={styles.facts}>
          <Fact label="Node state" value={runtimePhaseLabel(runtime.phase)} />
          <Fact label="Ownership" value={ownershipLabel(runtime.ownership)} />
          <Fact label="Endpoint" value={runtime.endpoint} technical />
          <Fact label="Node ID" value={runtime.status?.node_id ?? "Unavailable"} technical />
          <Fact label="Engine" value={runtime.status?.engine?.name ?? "Unavailable"} technical />
          <Fact label="Engine version" value={runtime.status?.engine?.version ?? "Unavailable"} technical />
          <Fact label="Active model" value={runtime.status?.runtime_model ?? "Unavailable"} technical />
        </dl>
      </section>
    </section>
  );
}

function Fact({ label, value, technical = false }: { label: string; value: string; technical?: boolean }) {
  return (
    <div className={styles.fact}>
      <dt>{label}</dt>
      <dd className={technical ? "technical-value" : undefined}>{value}</dd>
    </div>
  );
}

function runtimePhaseLabel(phase: NodeSessionPhase) {
  if (phase === "checking" || phase === "starting") return "Checking";
  if (phase === "unloaded") return "Ready — no model loaded";
  if (phase === "ready") return "Ready";
  if (phase === "reconciling") return "Updating model status";
  if (phase === "recovery-required") return "Recovery required";
  return phase[0].toUpperCase() + phase.slice(1);
}

function ownershipLabel(ownership: NodeOwnership) {
  if (ownership === "owned") return "App-owned node";
  if (ownership === "attached") return "Externally attached";
  return "No ownership";
}
