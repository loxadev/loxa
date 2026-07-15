import { ArrowLeft } from "lucide-react";
import type { RefObject } from "react";

import { NodeTable } from "../node/NodeTable";
import type { NodeSessionPhase } from "../node/NodeSession";
import type { NodeStatus } from "../node/contracts";
import type { NodeOwnership } from "../node/machine";
import { presentNode } from "../node/presentation";
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
        <NodeTable {...presentNode(runtime)} />
      </section>
    </section>
  );
}
