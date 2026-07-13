import { useEffect, useLayoutEffect, useRef, useState } from "react";

import { MarkdownMessage } from "./MarkdownMessage";
import styles from "./ChatScreen.module.css";

export type ChatTurnStatus = "queued" | "streaming" | "completed" | "cancelled" | "failed";

export type ChatTurn = {
  id: number | string;
  model: string;
  prompt: string;
  response: string;
  status: ChatTurnStatus;
  error: string;
};

export function ChatTranscript({
  turns,
  emptyMessage,
  copyText,
}: {
  turns: ChatTurn[];
  emptyMessage: string;
  copyText(text: string): Promise<void>;
}) {
  const transcript = useRef<HTMLDivElement>(null);
  const nearBottom = useRef(true);
  const latest = turns[turns.length - 1];

  useLayoutEffect(() => {
    const region = transcript.current;
    if (region && nearBottom.current) region.scrollTop = region.scrollHeight;
  }, [latest?.response, latest?.status, turns.length]);

  return (
    <div
      ref={transcript}
      className={styles.transcript}
      role="log"
      aria-label="Conversation"
      aria-live="off"
      aria-relevant="additions"
      tabIndex={0}
      onScroll={(event) => {
        const region = event.currentTarget;
        nearBottom.current = region.scrollHeight - region.scrollTop - region.clientHeight <= 64;
      }}
    >
      <div className={styles.transcriptColumn}>
        {turns.length === 0 ? (
          <div className={styles.emptyState}>
            <p>{emptyMessage}</p>
          </div>
        ) : turns.map((turn) => (
          <article className={styles.turn} key={turn.id} aria-label={`Chat turn using ${turn.model}`}>
            <div className={`${styles.message} ${styles.userMessage}`}>
              <p className={styles.messageLabel}>You</p>
              <p>{turn.prompt}</p>
            </div>
            <AssistantResponse turn={turn} copyText={copyText} />
          </article>
        ))}
      </div>
    </div>
  );
}

function AssistantResponse({ turn, copyText }: { turn: ChatTurn; copyText(text: string): Promise<void> }) {
  const [copyState, setCopyState] = useState<{
    phase: "idle" | "copying" | "copied" | "failed";
    turnId: number | string;
    status: ChatTurnStatus;
    source: string;
  }>({ phase: "idle", turnId: turn.id, status: turn.status, source: turn.response });
  const mounted = useRef(true);
  const copyGeneration = useRef(0);
  const currentTurn = useRef({ id: turn.id, status: turn.status, response: turn.response });
  const copyPhase = copyState.turnId === turn.id && copyState.status === turn.status && copyState.source === turn.response
    ? copyState.phase
    : "idle";
  const copyStatusId = `copy-response-status-${turn.id}`;
  const responseComplete = turn.status !== "queued" && turn.status !== "streaming" && turn.response.length > 0;
  const responseBusy = turn.status === "queued" || turn.status === "streaming";

  useEffect(() => () => {
    mounted.current = false;
    copyGeneration.current += 1;
  }, []);

  useLayoutEffect(() => {
    currentTurn.current = { id: turn.id, status: turn.status, response: turn.response };
    copyGeneration.current += 1;
  }, [turn.id, turn.response, turn.status]);

  const copy = async () => {
    if (!responseComplete || copyPhase === "copying") return;
    const source = turn.response;
    const turnId = turn.id;
    const status = turn.status;
    const generation = ++copyGeneration.current;
    setCopyState({ phase: "copying", turnId, status, source });
    try {
      await copyText(source);
      if (!mounted.current || copyGeneration.current !== generation || currentTurn.current.id !== turnId || currentTurn.current.status !== status || currentTurn.current.response !== source) return;
      setCopyState({ phase: "copied", turnId, status, source });
    } catch {
      if (!mounted.current || copyGeneration.current !== generation || currentTurn.current.id !== turnId || currentTurn.current.status !== status || currentTurn.current.response !== source) return;
      setCopyState({ phase: "failed", turnId, status, source });
    }
  };

  return (
    <div
      className={`${styles.message} ${styles.assistantMessage}`}
      role="region"
      aria-label={`Assistant response from ${turn.model}`}
      aria-live="off"
      aria-busy={responseBusy}
    >
      <div className={styles.messageHeading}>
        <div className={styles.messageIdentity}>
          <p className={styles.messageLabel}>Loxa</p>
          <span className="technical-value">{turn.model}</span>
        </div>
        <button
          className={`${styles.copyResponseButton} quiet-button interactive-target`}
          type="button"
          aria-label="Copy response"
          aria-describedby={copyPhase === "copied" || copyPhase === "failed" ? copyStatusId : undefined}
          disabled={!responseComplete || copyPhase === "copying"}
          onClick={() => void copy()}
        >
          {copyPhase === "copying" ? "Copying…" : "Copy response"}
        </button>
      </div>
      <MarkdownMessage content={turn.response || (turn.status === "queued" || turn.status === "streaming" ? "Waiting for the model…" : "No response was returned.")} />
      {(copyPhase === "copied" || copyPhase === "failed") && (
        <p id={copyStatusId} className={styles.copyStatus} role="status" aria-label="Copy response status">
          {copyPhase === "copied" ? "Response copied" : "Copy failed"}
        </p>
      )}
      <p className={`${styles.turnState} ${turn.status === "failed" ? styles.turnFailed : ""}`}>
        {turnStateLabel(turn.status)}{turn.error ? ` — ${turn.error}` : ""}
      </p>
    </div>
  );
}

function turnStateLabel(status: ChatTurnStatus): string {
  if (status === "failed") return "Turn failed";
  return `Turn ${status}`;
}
