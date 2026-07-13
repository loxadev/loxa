import type { KeyboardEvent, RefObject } from "react";

import type { ModelInventoryEntry } from "../control/contracts";
import styles from "./ChatScreen.module.css";

type ChatComposerProps = {
  input: string;
  inputRef: RefObject<HTMLTextAreaElement | null>;
  canCompose: boolean;
  responseInProgress: boolean;
  supportReason: string;
  attachmentReason: string;
  activeModel: string | null;
  selectedModel: string;
  eligibleModels: ModelInventoryEntry[];
  modelBusy: boolean;
  modelOperation: "idle" | "switching";
  onInput(value: string): void;
  onSelectedModel(value: string): void;
  onSwitchModel(): void;
  onSend(): void;
  onStop(): void;
};

export function ChatComposer({
  input,
  inputRef,
  canCompose,
  responseInProgress,
  supportReason,
  attachmentReason,
  activeModel,
  selectedModel,
  eligibleModels,
  modelBusy,
  modelOperation,
  onInput,
  onSelectedModel,
  onSwitchModel,
  onSend,
  onStop,
}: ChatComposerProps) {
  const switchingDisabled = modelOperation === "switching" || modelBusy || responseInProgress;
  const submit = () => {
    if (responseInProgress) onStop();
    else onSend();
  };
  const keyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing || event.keyCode === 229) return;
    event.preventDefault();
    if (canCompose && input.trim()) onSend();
  };

  return (
    <form
      className={styles.composer}
      aria-label="Message composer"
      onSubmit={(event) => {
        event.preventDefault();
        submit();
      }}
    >
      <label className={styles.messageLabel} htmlFor="message">Message</label>
      <textarea
        ref={inputRef}
        id="message"
        className={styles.messageInput}
        rows={3}
        value={input}
        onChange={(event) => onInput(event.target.value)}
        onKeyDown={keyDown}
        disabled={!canCompose}
        aria-describedby={supportReason ? "chat-support-reason" : undefined}
        placeholder="Message the active local model"
      />
      {supportReason && <p id="chat-support-reason" className={styles.supportReason}>{supportReason}</p>}

      <div className={styles.composerFooter}>
        <div className={styles.composerTools}>
          <button
            className={`${styles.attachmentButton} quiet-button interactive-target`}
            type="button"
            aria-label="Attach document"
            aria-describedby="attachment-support-reason"
            disabled
          >+</button>
          <div className={styles.modelControl}>
            <label htmlFor="active-chat-model">Choose model</label>
            <select
              id="active-chat-model"
              className={styles.modelPicker}
              value={selectedModel}
              aria-describedby="model-control-help"
              disabled={switchingDisabled}
              onChange={(event) => onSelectedModel(event.target.value)}
            >
              <option value="">No active model</option>
              {eligibleModels.map((model) => <option key={model.id} value={model.id}>{model.id}</option>)}
            </select>
            {selectedModel !== activeModel && selectedModel && (
              <button
                className="secondary-button interactive-target"
                type="button"
                disabled={switchingDisabled}
                onClick={onSwitchModel}
                aria-label={`${activeModel === null ? "Load" : "Switch to"} ${selectedModel}`}
              >
                {modelOperation === "switching" ? "Loading…" : activeModel === null ? "Load" : "Switch"}
              </button>
            )}
            <span id="model-control-help" className={styles.modelHelp}>
              Active: <span className="technical-value">{activeModel ?? "None"}</span>
            </span>
          </div>
        </div>

        {responseInProgress ? (
          <button className={`${styles.primaryControl} secondary-button interactive-target`} type="button" aria-label="Stop response" onClick={onStop}>
            <span aria-hidden="true">■</span> Stop<span className={styles.visuallyHidden}> response</span>
          </button>
        ) : (
          <button className={`${styles.primaryControl} primary-button interactive-target`} type="submit" aria-label="Send message" disabled={!canCompose || !input.trim()}>
            Send<span className={styles.visuallyHidden}> message</span>
          </button>
        )}
      </div>
      <p id="attachment-support-reason" className={styles.attachmentReason}>{attachmentReason}</p>
    </form>
  );
}
