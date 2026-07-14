import type { KeyboardEvent, RefObject } from "react";
import { Paperclip, Send, Square } from "lucide-react";

import type { ModelInventoryEntry } from "../control/contracts";
import { Button, IconButton } from "../components/ui/button";
import styles from "./ChatComposer.module.css";

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
  modelControlsAvailable: boolean;
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
  modelControlsAvailable,
  onInput,
  onSelectedModel,
  onSwitchModel,
  onSend,
  onStop,
}: ChatComposerProps) {
  const switchingDisabled =
    !modelControlsAvailable || modelOperation === "switching" || modelBusy || responseInProgress;
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
      <label className={styles.messageLabel} htmlFor="message">
        Message
      </label>
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
      {supportReason && (
        <p id="chat-support-reason" className={styles.supportReason}>
          {supportReason}
        </p>
      )}

      <div className={styles.composerFooter}>
        <div className={styles.composerTools}>
          <span className={styles.attachmentControl}>
            <IconButton
              className={styles.attachmentButton}
              variant="quiet"
              label="Attach document"
              helpId={attachmentReason ? "attachment-support-reason" : undefined}
              aria-disabled="true"
              onClick={(event) => event.preventDefault()}
            >
              <Paperclip />
            </IconButton>
            {attachmentReason && (
              <span id="attachment-support-reason" className={styles.attachmentTooltip} role="tooltip">
                {attachmentReason}
              </span>
            )}
          </span>
          <div className={styles.modelControl}>
            <label htmlFor="active-chat-model">Choose model</label>
            <select
              id="active-chat-model"
              className={styles.modelPicker}
              value={selectedModel}
              disabled={switchingDisabled}
              onChange={(event) => onSelectedModel(event.target.value)}
            >
              <option value="">No active model</option>
              {eligibleModels.map((model) => (
                <option key={model.id} value={model.id}>
                  {model.id}
                </option>
              ))}
            </select>
            {selectedModel !== activeModel && selectedModel && (
              <Button
                variant="secondary"
                type="button"
                disabled={switchingDisabled}
                onClick={onSwitchModel}
                aria-label={`${activeModel === null ? "Load" : "Switch to"} ${selectedModel}`}
              >
                {modelOperation === "switching" ? "Loading…" : activeModel === null ? "Load" : "Switch"}
              </Button>
            )}
          </div>
        </div>

        {responseInProgress ? (
          <Button
            className={styles.primaryControl}
            variant="secondary"
            type="button"
            aria-label="Stop response"
            onClick={onStop}
          >
            <Square aria-hidden="true" /> Stop<span className={styles.visuallyHidden}> response</span>
          </Button>
        ) : (
          <Button
            className={styles.primaryControl}
            type="submit"
            aria-label="Send message"
            disabled={!canCompose || !input.trim()}
          >
            <Send aria-hidden="true" /> Send<span className={styles.visuallyHidden}> message</span>
          </Button>
        )}
      </div>
    </form>
  );
}
