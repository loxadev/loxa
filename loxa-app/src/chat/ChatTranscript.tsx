import { useLayoutEffect, useRef } from "react";

import styles from "./ChatScreen.module.css";

export type ChatTurnStatus = "queued" | "streaming" | "completed" | "cancelled" | "failed";

export type ChatTurn = {
  id: number;
  model: string;
  prompt: string;
  response: string;
  status: ChatTurnStatus;
  error: string;
};

export function ChatTranscript({ turns, emptyMessage }: { turns: ChatTurn[]; emptyMessage: string }) {
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
      aria-live="polite"
      aria-relevant="additions text"
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
            <div className={`${styles.message} ${styles.assistantMessage}`}>
              <div className={styles.messageHeading}>
                <p className={styles.messageLabel}>Loxa</p>
                <span className="technical-value">{turn.model}</span>
              </div>
              <p>{turn.response || (turn.status === "queued" || turn.status === "streaming" ? "Waiting for the model…" : "No response was returned.")}</p>
              <p className={`${styles.turnState} ${turn.status === "failed" ? styles.turnFailed : ""}`}>
                {turnStateLabel(turn.status)}{turn.error ? ` — ${turn.error}` : ""}
              </p>
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}

function turnStateLabel(status: ChatTurnStatus): string {
  if (status === "failed") return "Turn failed";
  return `Turn ${status}`;
}
