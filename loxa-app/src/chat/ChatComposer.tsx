import type { KeyboardEvent, RefObject } from "react";
import { Paperclip, Send, Square } from "lucide-react";

import { Button, IconButton } from "../components/ui/button";
import { Tooltip } from "../components/ui/tooltip";
import styles from "./ChatComposer.module.css";

type ChatComposerProps = {
  input: string;
  inputRef: RefObject<HTMLTextAreaElement | null>;
  canCompose: boolean;
  responseInProgress: boolean;
  attachmentReason: string;
  onInput(value: string): void;
  onSend(): void;
  onStop(): void;
};

export function ChatComposer({
  input,
  inputRef,
  canCompose,
  responseInProgress,
  attachmentReason,
  onInput,
  onSend,
  onStop,
}: ChatComposerProps) {
  const submit = () => {
    if (responseInProgress) onStop();
    else onSend();
  };
  const keyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing || event.keyCode === 229) return;
    event.preventDefault();
    if (canCompose && input.trim()) onSend();
  };
  const attachmentButton = (
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
  );

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
        placeholder="Message the active local model"
      />

      <div className={styles.composerFooter}>
        <div className={styles.composerTools}>
          {attachmentReason ? (
            <Tooltip id="attachment-support-reason" side="top" content={attachmentReason}>
              {attachmentButton}
            </Tooltip>
          ) : (
            attachmentButton
          )}
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
