import { useEffect, useRef, useState } from "react";
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
  onClearChatHistory,
}: {
  theme: ThemeMode;
  onThemeChange: (mode: ThemeMode) => void;
  runtime: {
    phase: NodeSessionPhase;
    endpoint: string;
    ownership: NodeOwnership;
    status: NodeStatus | null;
  };
  onClearChatHistory?: (signal: AbortSignal) => Promise<number>;
}) {
  const activeLabel = choices.find(({ mode }) => mode === theme)?.label ?? "System";
  const [confirmClear, setConfirmClear] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [historyStatus, setHistoryStatus] = useState("");
  const mounted = useRef(true);
  const clearController = useRef<AbortController | null>(null);
  const clearGeneration = useRef(0);

  useEffect(() => {
    mounted.current = true;
    const dispose = () => {
      if (!mounted.current) return;
      mounted.current = false;
      clearGeneration.current += 1;
      clearController.current?.abort();
      clearController.current = null;
    };
    window.addEventListener("beforeunload", dispose);
    return () => {
      window.removeEventListener("beforeunload", dispose);
      dispose();
    };
  }, []);

  const clearHistory = async () => {
    if (!onClearChatHistory || clearing || !mounted.current) return;
    clearController.current?.abort();
    const controller = new AbortController();
    clearController.current = controller;
    const generation = ++clearGeneration.current;
    setClearing(true);
    setHistoryStatus("");
    try {
      const deleted = await onClearChatHistory(controller.signal);
      if (!mounted.current || controller.signal.aborted || generation !== clearGeneration.current) return;
      setHistoryStatus(`Deleted ${deleted} ${deleted === 1 ? "conversation" : "conversations"}.`);
      setConfirmClear(false);
    } catch {
      if (!mounted.current || controller.signal.aborted || generation !== clearGeneration.current) return;
      setHistoryStatus("Could not clear local chat history.");
    } finally {
      if (clearController.current === controller) clearController.current = null;
      if (mounted.current && !controller.signal.aborted && generation === clearGeneration.current) setClearing(false);
    }
  };

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
      <p className={styles.disclosure}>
        Theme is the only preference saved on this Mac. Node and model state are not stored here.
      </p>

      <section className={styles.group} aria-labelledby="chat-history-heading">
        <h2 id="chat-history-heading">Chat history</h2>
        <p className={styles.description}>
          Conversations are stored as local plaintext in Loxa's user-only data directory so the desktop app and CLI can
          share them. They are not synced.
        </p>
        {onClearChatHistory ? (
          <div className={styles.destructiveActions}>
            {!confirmClear ? (
              <button className="quiet-button interactive-target" type="button" onClick={() => setConfirmClear(true)}>
                Clear chat history
              </button>
            ) : (
              <div className={styles.confirmClear} role="group" aria-label="Confirm clear chat history">
                <p>This permanently deletes every saved conversation.</p>
                <button
                  className="quiet-button interactive-target"
                  type="button"
                  disabled={clearing}
                  onClick={() => void clearHistory()}
                >
                  Confirm clear chat history
                </button>
                <button
                  className="quiet-button interactive-target"
                  type="button"
                  disabled={clearing}
                  onClick={() => setConfirmClear(false)}
                >
                  Cancel
                </button>
              </div>
            )}
          </div>
        ) : null}
      </section>

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
      <p className="visually-hidden" role="status" aria-live="polite">
        Theme set to {activeLabel}. {historyStatus}
      </p>
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
