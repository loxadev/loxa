import type { ThemeMode } from "./theme";
import type { NodeStatus } from "../node/contracts";
import type { NodeSessionPhase } from "../node/NodeSession";
import type { NodeOwnership } from "../node/machine";
import styles from "./SettingsScreen.module.css";

const choices: ReadonlyArray<{ mode: ThemeMode; label: string; detail: string }> = [
  { mode: "light", label: "Light", detail: "Always use Loxa's light appearance." },
  { mode: "dark", label: "Dark", detail: "Always use Loxa's dark appearance." },
  { mode: "system", label: "System", detail: "Follow your Mac appearance automatically." },
];

export function SettingsScreen({
  theme,
  onThemeChange,
  runtime,
}: {
  theme: ThemeMode;
  onThemeChange: (mode: ThemeMode) => void;
  runtime: {
    phase: NodeSessionPhase;
    endpoint: string;
    ownership: NodeOwnership;
    status: NodeStatus | null;
  };
}) {
  const activeLabel = choices.find(({ mode }) => mode === theme)?.label ?? "System";

  return (
    <section className={styles.screen} aria-labelledby="settings-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Preferences</p>
          <h1 id="settings-heading">Settings</h1>
        </div>
      </header>

      <fieldset className={styles.group} role="radiogroup">
        <legend>Appearance</legend>
        <p className={styles.description}>Choose how Loxa looks. System updates when your Mac appearance changes.</p>
        <div className={styles.themeOptions}>
          {choices.map(({ mode, label, detail }) => (
            <label className={`${styles.themeOption} interactive-target`} key={mode}>
              <input
                type="radio"
                name="theme"
                value={mode}
                aria-label={label}
                aria-describedby={`theme-${mode}-detail`}
                checked={theme === mode}
                onChange={() => onThemeChange(mode)}
              />
              <span>
                <strong>{label}</strong>
                <small id={`theme-${mode}-detail`}>{detail}</small>
              </span>
            </label>
          ))}
        </div>
      </fieldset>
      <p className={styles.disclosure}>Theme is the only preference saved on this Mac. Node and model state are not stored here.</p>

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
      <p className="visually-hidden" role="status" aria-live="polite">Theme set to {activeLabel}</p>
    </section>
  );
}

function Fact({ label, value, technical = false }: { label: string; value: string; technical?: boolean }) {
  return <div className={styles.fact}><dt>{label}</dt><dd className={technical ? "technical-value" : undefined}>{value}</dd></div>;
}

function runtimePhaseLabel(phase: NodeSessionPhase) {
  if (phase === "checking" || phase === "starting") return "Checking";
  if (phase === "unloaded") return "Ready — no model loaded";
  if (phase === "ready") return "Ready";
  if (phase === "recovery-required") return "Recovery required";
  return phase[0].toUpperCase() + phase.slice(1);
}

function ownershipLabel(ownership: NodeOwnership) {
  if (ownership === "owned") return "App-owned node";
  if (ownership === "attached") return "Externally attached";
  return "No ownership";
}
