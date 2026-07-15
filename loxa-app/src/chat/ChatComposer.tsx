import type { KeyboardEvent, RefObject } from "react";
import { Paperclip, SendHorizontal, Square, Wrench } from "lucide-react";

import { Button, IconButton } from "../components/ui/button";
import { Tooltip } from "../components/ui/tooltip";
import styles from "./ChatComposer.module.css";

type ChatComposerProps = {
  input: string;
  inputRef: RefObject<HTMLTextAreaElement | null>;
  canCompose: boolean;
  responseInProgress: boolean;
  attachmentReason: string;
  toolsReason?: string;
  contextUsage?: number | null;
  idPrefix?: string;
  label?: string;
  placeholder?: string;
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
  toolsReason = "Tool use is not available for this model.",
  contextUsage = null,
  idPrefix = "",
  label = "Message composer",
  placeholder = "Message the active local model",
  onInput,
  onSend,
  onStop,
}: ChatComposerProps) {
  const messageId = idPrefix ? `${idPrefix}-message` : "message";
  const attachmentHelpId = idPrefix ? `${idPrefix}-attachment-support-reason` : "attachment-support-reason";
  const toolsHelpId = idPrefix ? `${idPrefix}-tools-support-reason` : "tools-support-reason";
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
      helpId={attachmentReason ? attachmentHelpId : undefined}
      aria-disabled="true"
      onClick={(event) => event.preventDefault()}
    >
      <Paperclip />
    </IconButton>
  );
  const toolsButton = (
    <IconButton
      className={styles.toolButton}
      variant="quiet"
      label="Tools"
      helpId={toolsHelpId}
      aria-disabled="true"
      onClick={(event) => event.preventDefault()}
    >
      <Wrench />
    </IconButton>
  );

  return (
    <form
      className={styles.composer}
      aria-label={label}
      onSubmit={(event) => {
        event.preventDefault();
        submit();
      }}
    >
      <label className={styles.messageLabel} htmlFor={messageId}>
        Message
      </label>
      <textarea
        ref={inputRef}
        id={messageId}
        className={styles.messageInput}
        rows={3}
        value={input}
        onChange={(event) => onInput(event.target.value)}
        onKeyDown={keyDown}
        disabled={!canCompose}
        placeholder={placeholder}
      />

      <div className={styles.composerFooter}>
        <div className={styles.composerTools}>
          {attachmentReason ? (
            <Tooltip id={attachmentHelpId} side="top" content={attachmentReason}>
              {attachmentButton}
            </Tooltip>
          ) : (
            attachmentButton
          )}
          <Tooltip id={toolsHelpId} side="top" content={toolsReason}>
            {toolsButton}
          </Tooltip>
          <span className={styles.contextUsage} aria-label="Context usage">
            {contextUsage === null ? "Context unavailable" : `${Math.round(contextUsage)}% context`}
          </span>
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
          <IconButton
            className={styles.primaryControl}
            variant="primary"
            label="Send message"
            type="submit"
            disabled={!canCompose || !input.trim()}
          >
            <SendHorizontal />
          </IconButton>
        )}
      </div>
    </form>
  );
}
