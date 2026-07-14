import { Pencil, Trash2 } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from "react";

import { Button, IconButton } from "../components/ui/button";
import { Input } from "../components/ui/input";
import type { ConversationListItem } from "./ConversationList";
import styles from "./ConversationList.module.css";
import type { TurnState } from "./historyClient";

type Props = {
  conversation: ConversationListItem;
  selected: boolean;
  mutationsDisabled: boolean;
  mutationDisabledReasonId: string;
  onOpen(id: string): void;
  onOpenButton(node: HTMLButtonElement | null): void;
  onRename(id: string, title: string): void | Promise<void>;
  onDelete(id: string): Promise<string | null>;
  onDeleteSuccess(deletedId: string, replacementId: string | null): void;
  isLifecycleActive?(): boolean;
};

export function ConversationHistoryItem({
  conversation,
  selected,
  mutationsDisabled,
  mutationDisabledReasonId,
  onOpen,
  onOpenButton,
  onRename,
  onDelete,
  onDeleteSuccess,
  isLifecycleActive,
}: Props) {
  const [mode, setMode] = useState<"idle" | "rename" | "delete">("idle");
  const [renameValue, setRenameValue] = useState(conversation.title);
  const [pending, setPending] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const renameButton = useRef<HTMLButtonElement>(null);
  const renameInput = useRef<HTMLInputElement>(null);
  const deleteButton = useRef<HTMLButtonElement>(null);
  const deleteConfirmButton = useRef<HTMLButtonElement>(null);
  const mounted = useRef(true);
  const renameHelpId = `rename-help-${conversation.id}`;
  const deleteHelpId = `delete-help-${conversation.id}`;

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const actionIsActive = () => mounted.current && (isLifecycleActive?.() ?? true);
  const restoreFocus = (target: "rename" | "delete") => {
    requestAnimationFrame(() => (target === "rename" ? renameButton : deleteButton).current?.focus());
  };

  const cancelRename = () => {
    setMode("idle");
    restoreFocus("rename");
  };

  const submitRename = async (event: FormEvent) => {
    event.preventDefault();
    if (!validTitle(renameValue)) return;
    setPending(true);
    setActionError(null);
    try {
      await onRename(conversation.id, renameValue);
      if (!actionIsActive()) return;
      setMode("idle");
      restoreFocus("rename");
    } catch {
      if (!actionIsActive()) return;
      setActionError("Could not rename this conversation.");
      requestAnimationFrame(() => renameInput.current?.focus());
    } finally {
      if (actionIsActive()) setPending(false);
    }
  };

  const cancelDelete = () => {
    setMode("idle");
    restoreFocus("delete");
  };

  const confirmDelete = async () => {
    setPending(true);
    setActionError(null);
    try {
      const replacementId = await onDelete(conversation.id);
      onDeleteSuccess(conversation.id, replacementId);
      if (!actionIsActive()) return;
      setMode("idle");
    } catch {
      if (!actionIsActive()) return;
      setActionError("Could not delete this conversation.");
      requestAnimationFrame(() => deleteConfirmButton.current?.focus());
    } finally {
      if (actionIsActive()) setPending(false);
    }
  };

  const deleteKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape" && !pending) {
      event.preventDefault();
      event.stopPropagation();
      cancelDelete();
    }
  };

  return (
    <li className={styles.item}>
      {mode === "rename" ? (
        <form className={styles.renameForm} onSubmit={(event) => void submitRename(event)}>
          <label className={styles.srOnly} htmlFor={`rename-${conversation.id}`}>
            Conversation title
          </label>
          <Input
            ref={renameInput}
            autoFocus
            id={`rename-${conversation.id}`}
            maxLength={160}
            value={renameValue}
            onChange={(event) => setRenameValue(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                event.preventDefault();
                cancelRename();
              }
            }}
            disabled={pending}
          />
          <div className={styles.confirmActions}>
            <Button type="submit" disabled={!validTitle(renameValue) || pending} busy={pending}>
              Save title
            </Button>
            <Button type="button" variant="secondary" onClick={cancelRename} disabled={pending}>
              Cancel rename
            </Button>
          </div>
        </form>
      ) : (
        <>
          <button
            ref={onOpenButton}
            className={styles.conversationButton}
            type="button"
            aria-current={selected ? "page" : undefined}
            aria-label={`Open ${conversation.title}`}
            title={conversation.title}
            onClick={() => onOpen(conversation.id)}
            disabled={mode === "delete"}
          >
            <span className={styles.title}>{conversation.title}</span>
            <span className={styles.meta}>
              <time dateTime={new Date(conversation.updatedAtMs).toISOString()}>
                {formatTimestamp(conversation.updatedAtMs)}
              </time>
              {conversation.terminalState ? <span>{terminalLabel(conversation.terminalState)}</span> : null}
            </span>
          </button>
          <div className={styles.itemActions}>
            <IconButton
              ref={renameButton}
              label={`Rename ${conversation.title}`}
              helpId={mutationsDisabled ? mutationDisabledReasonId : renameHelpId}
              variant="quiet"
              onClick={() => {
                setActionError(null);
                setRenameValue(conversation.title);
                setMode("rename");
              }}
              disabled={mutationsDisabled || mode === "delete"}
            >
              <Pencil />
            </IconButton>
            <IconButton
              ref={deleteButton}
              label={`Delete ${conversation.title}`}
              helpId={mutationsDisabled ? mutationDisabledReasonId : deleteHelpId}
              variant="quiet"
              onClick={() => {
                setActionError(null);
                setMode("delete");
              }}
              disabled={mutationsDisabled || mode === "delete"}
            >
              <Trash2 />
            </IconButton>
            <span className={styles.srOnly} id={renameHelpId}>
              Rename this conversation.
            </span>
            <span className={styles.srOnly} id={deleteHelpId}>
              Delete this conversation after confirmation.
            </span>
          </div>
        </>
      )}

      {mode === "delete" ? (
        <div
          className={styles.deleteConfirm}
          role="group"
          aria-label={`Delete ${conversation.title}?`}
          aria-describedby={`delete-note-${conversation.id}`}
          onKeyDown={deleteKeyDown}
        >
          <p id={`delete-note-${conversation.id}`}>
            <strong>Delete {conversation.title}?</strong> This cannot be undone.
          </p>
          <div className={styles.confirmActions}>
            <Button type="button" variant="secondary" autoFocus onClick={cancelDelete} disabled={pending}>
              Cancel delete
            </Button>
            <Button
              ref={deleteConfirmButton}
              type="button"
              variant="danger"
              onClick={() => void confirmDelete()}
              disabled={mutationsDisabled || pending}
              busy={pending}
            >
              Delete conversation
            </Button>
          </div>
        </div>
      ) : null}
      {actionError ? (
        <p className={styles.itemError} role="alert">
          {actionError}
        </p>
      ) : null}
    </li>
  );
}

function validTitle(value: string): boolean {
  return value.trim().length > 0 && !value.includes("\0") && [...value].length <= 160;
}

function formatTimestamp(timestamp: number): string {
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" }).format(new Date(timestamp));
}

function terminalLabel(state: TurnState): string {
  switch (state) {
    case "queued":
      return "Queued";
    case "streaming":
      return "Streaming";
    case "completed":
      return "Completed";
    case "cancelled":
      return "Cancelled";
    case "failed":
      return "Failed";
  }
}
