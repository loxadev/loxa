import { ChevronRight, Server } from "lucide-react";
import { useEffect, useRef, useState, type RefObject } from "react";

import styles from "./SettingsScreen.module.css";
import type { ThemeMode } from "./theme";

const choices: ReadonlyArray<{ mode: ThemeMode; label: string; detail: string }> = [
  { mode: "light", label: "Light", detail: "Always use Loxa's light appearance." },
  { mode: "dark", label: "Dark", detail: "Always use Loxa's dark appearance." },
  { mode: "system", label: "System", detail: "Follow your Mac appearance automatically." },
];

export function SettingsOverview({
  theme,
  onThemeChange,
  onClearChatHistory,
  headingRef,
  onOpenRuntime,
}: {
  theme: ThemeMode;
  onThemeChange: (mode: ThemeMode) => void;
  onClearChatHistory?: (signal: AbortSignal) => Promise<number>;
  headingRef: RefObject<HTMLHeadingElement | null>;
  onOpenRuntime: () => void;
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
          <h1 id="settings-heading" ref={headingRef} tabIndex={-1}>
            Settings
          </h1>
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
        Theme and sidebar display preferences are saved on this Mac. Backend, node, and model state are not stored here.
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

      <section className={styles.group} aria-labelledby="runtime-settings-heading">
        <h2 id="runtime-settings-heading">Runtime</h2>
        <button className={`${styles.navigationRow} interactive-target`} type="button" onClick={onOpenRuntime}>
          <Server aria-hidden="true" focusable="false" />
          <span>
            <strong>Runtime</strong>
            <small>Read-only local node and runtime details</small>
          </span>
          <ChevronRight aria-hidden="true" focusable="false" />
        </button>
      </section>

      <p className="visually-hidden" role="status" aria-live="polite">
        Theme set to {activeLabel}. {historyStatus}
      </p>
    </section>
  );
}
