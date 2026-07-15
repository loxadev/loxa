import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Copy } from "lucide-react";

import { MarkdownMessage } from "./MarkdownMessage";
import mark from "../assets/brand/loxa-mark.svg?no-inline";
import { Button, IconButton } from "../components/ui/button";
import { ResponseMetrics } from "./ResponseMetrics";
import type { ChatTurnMetrics } from "./turnMetrics";
import styles from "./ChatTranscript.module.css";

export type ChatTurnStatus = "queued" | "streaming" | "completed" | "cancelled" | "failed";

export type ChatTurn = {
  id: number | string;
  model: string;
  prompt: string;
  response: string;
  status: ChatTurnStatus;
  error: string;
  metrics?: ChatTurnMetrics | null;
};

export function ChatTranscript({
  turns,
  emptyMessage,
  copyText,
  onBrowseModels,
}: {
  turns: ChatTurn[];
  emptyMessage: string;
  copyText(text: string): Promise<void>;
  onBrowseModels?: () => void;
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
            <img className={styles.emptyMark} src={mark} alt="Loxa" width="48" height="48" />
            <p>{emptyMessage}</p>
            {onBrowseModels && (
              <Button variant="secondary" onClick={onBrowseModels}>
                Browse models
              </Button>
            )}
          </div>
        ) : (
          turns.map((turn) => (
            <article className={styles.turn} key={turn.id} aria-label={`Chat turn using ${turn.model}`}>
              <div className={`${styles.message} ${styles.userMessage}`}>
                <p className={styles.messageLabel}>You</p>
                <p>{turn.prompt}</p>
              </div>
              <AssistantResponse turn={turn} copyText={copyText} />
            </article>
          ))
        )}
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
  const copyPhase =
    copyState.turnId === turn.id && copyState.status === turn.status && copyState.source === turn.response
      ? copyState.phase
      : "idle";
  const copyStatusId = `copy-response-status-${turn.id}`;
  const responseComplete = turn.status !== "queued" && turn.status !== "streaming" && turn.response.length > 0;
  const responseBusy = turn.status === "queued" || turn.status === "streaming";

  useEffect(
    () => () => {
      mounted.current = false;
      copyGeneration.current += 1;
    },
    [],
  );

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
      if (
        !mounted.current ||
        copyGeneration.current !== generation ||
        currentTurn.current.id !== turnId ||
        currentTurn.current.status !== status ||
        currentTurn.current.response !== source
      )
        return;
      setCopyState({ phase: "copied", turnId, status, source });
    } catch {
      if (
        !mounted.current ||
        copyGeneration.current !== generation ||
        currentTurn.current.id !== turnId ||
        currentTurn.current.status !== status ||
        currentTurn.current.response !== source
      )
        return;
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
      </div>
      <MarkdownMessage
        content={
          turn.response ||
          (turn.status === "queued" || turn.status === "streaming"
            ? "Waiting for the model…"
            : "No response was returned.")
        }
      />
      <div className={styles.responseFooter}>
        <ResponseMetrics metrics={turn.metrics} />
        <IconButton
          className={`${styles.copyResponseButton} interactive-target`}
          variant="quiet"
          label="Copy response"
          helpId={copyPhase === "copied" || copyPhase === "failed" ? copyStatusId : undefined}
          busy={copyPhase === "copying"}
          disabled={!responseComplete}
          onClick={() => void copy()}
        >
          <Copy />
        </IconButton>
      </div>
      {(copyPhase === "copied" || copyPhase === "failed") && (
        <p id={copyStatusId} className={styles.copyStatus} role="status" aria-label="Copy response status">
          {copyPhase === "copied" ? "Response copied" : "Copy failed"}
        </p>
      )}
      {turnStateLabel(turn.status) !== null && (
        <p className={`${styles.turnState} ${turn.status === "failed" ? styles.turnFailed : ""}`}>
          {turnStateLabel(turn.status)}
          {turn.error ? ` — ${turn.error}` : ""}
        </p>
      )}
    </div>
  );
}

function turnStateLabel(status: ChatTurnStatus): string | null {
  if (status === "completed") return null;
  if (status === "cancelled") return "Generation stopped";
  if (status === "failed") return "Turn failed";
  return `Turn ${status}`;
}
