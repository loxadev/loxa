import { Activity, Cpu, Gauge, MemoryStick, X } from "lucide-react";

import { IconButton } from "../components/ui/button";
import styles from "./ObservabilityInspector.module.css";

export function ObservabilityInspector({ health, model, onClose }: { health: string; model: string; onClose(): void }) {
  return (
    <div className={styles.inspector}>
      <header className={styles.header}>
        <h2>Observability</h2>
        <IconButton variant="quiet" label="Close observability" onClick={onClose}>
          <X />
        </IconButton>
      </header>
      <section className={styles.section} aria-labelledby="runtime-observability-heading">
        <h3 id="runtime-observability-heading">Local runtime</h3>
        <Fact icon={<Activity />} label="Health" value={health} />
        <Fact icon={<Cpu />} label="Active model" value={model} />
      </section>
      <section className={styles.section} aria-labelledby="system-observability-heading">
        <h3 id="system-observability-heading">Live resources</h3>
        <Fact icon={<Cpu />} label="CPU usage" value="Unavailable" />
        <Fact icon={<MemoryStick />} label="Memory and swap" value="Unavailable" />
        <Fact icon={<Gauge />} label="Token throughput" value="Unavailable" />
        <p className={styles.explanation}>
          Live resource and inference metrics will appear when the node publishes the observability contract.
        </p>
      </section>
    </div>
  );
}

function Fact({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className={styles.fact}>
      <span className={styles.factIcon} aria-hidden="true">
        {icon}
      </span>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
