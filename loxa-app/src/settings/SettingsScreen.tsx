import type { ThemeMode } from "./theme";

const choices: ReadonlyArray<{ mode: ThemeMode; label: string; detail: string }> = [
  { mode: "light", label: "Light", detail: "Always use Loxa's light appearance." },
  { mode: "dark", label: "Dark", detail: "Always use Loxa's dark appearance." },
  { mode: "system", label: "System", detail: "Follow your Mac appearance automatically." },
];

export function SettingsScreen({
  theme,
  onThemeChange,
}: {
  theme: ThemeMode;
  onThemeChange: (mode: ThemeMode) => void;
}) {
  const activeLabel = choices.find(({ mode }) => mode === theme)?.label ?? "System";

  return (
    <section aria-labelledby="settings-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Preferences</p>
          <h1 id="settings-heading">Settings</h1>
        </div>
      </header>

      <fieldset className="settings-group" role="radiogroup">
        <legend>Appearance</legend>
        <p className="settings-description">Choose how Loxa looks. System updates when your Mac appearance changes.</p>
        <div className="theme-options">
          {choices.map(({ mode, label, detail }) => (
            <label className="theme-option interactive-target" key={mode}>
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
      <p className="visually-hidden" role="status" aria-live="polite">Theme set to {activeLabel}</p>
    </section>
  );
}
