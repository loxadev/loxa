import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from "react";
import styles from "./ConversationList.module.css";
import type { ChatSummary, TurnState } from "./historyClient";

export type ConversationListItem = ChatSummary & { terminalState?: TurnState };

type Props = {
  conversations: ConversationListItem[];
  selectedId: string | null;
  state: "loading" | "ready" | "error";
  errorMessage?: string;
  hasMore: boolean;
  onCreate(): void | Promise<void>;
  onSelect(id: string): void;
  onRename(id: string, title: string): void | Promise<void>;
  onDelete(id: string): void | Promise<void>;
  onLoadMore(): void | Promise<void>;
  onRetry?(): void | Promise<void>;
  isLifecycleActive?(): boolean;
  mutationsDisabled?: boolean;
};

export function ConversationList({
  conversations,
  selectedId,
  state,
  errorMessage,
  hasMore,
  onCreate,
  onSelect,
  onRename,
  onDelete,
  onLoadMore,
  onRetry,
  isLifecycleActive,
  mutationsDisabled = false,
}: Props) {
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [pendingListAction, setPendingListAction] = useState<"create" | "load-more" | "retry" | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const renameButtons = useRef(new Map<string, HTMLButtonElement>());
  const deleteButtons = useRef(new Map<string, HTMLButtonElement>());
  const deleteConfirmButtons = useRef(new Map<string, HTMLButtonElement>());
  const newChatButton = useRef<HTMLButtonElement>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const actionIsActive = () => mounted.current && (isLifecycleActive?.() ?? true);

  const startRename = (conversation: ConversationListItem) => {
    setDeletingId(null);
    setActionError(null);
    setRenameValue(conversation.title);
    setRenamingId(conversation.id);
  };

  const cancelRename = () => {
    const id = renamingId;
    setRenamingId(null);
    if (id !== null) requestAnimationFrame(() => renameButtons.current.get(id)?.focus());
  };

  const submitRename = async (event: FormEvent) => {
    event.preventDefault();
    if (renamingId === null || !validTitle(renameValue)) return;
    const id = renamingId;
    setPendingId(id);
    setActionError(null);
    try {
      await onRename(id, renameValue);
      if (!actionIsActive()) return;
      setRenamingId(null);
      requestAnimationFrame(() => renameButtons.current.get(id)?.focus());
    } catch {
      if (!actionIsActive()) return;
      setActionError("Could not rename this conversation.");
    } finally {
      if (actionIsActive()) setPendingId(null);
    }
  };

  const cancelDelete = () => {
    const id = deletingId;
    setDeletingId(null);
    if (id !== null) requestAnimationFrame(() => deleteButtons.current.get(id)?.focus());
  };

  const confirmDelete = async (id: string) => {
    setPendingId(id);
    setActionError(null);
    try {
      await onDelete(id);
      if (!actionIsActive()) return;
      setDeletingId(null);
      requestAnimationFrame(() => newChatButton.current?.focus());
    } catch {
      if (!actionIsActive()) return;
      setActionError("Could not delete this conversation.");
      requestAnimationFrame(() => deleteConfirmButtons.current.get(id)?.focus());
    } finally {
      if (actionIsActive()) setPendingId(null);
    }
  };

  const runListAction = async (kind: "create" | "load-more" | "retry", action: () => void | Promise<void>) => {
    if (pendingListAction !== null) return;
    setPendingListAction(kind);
    setActionError(null);
    try {
      await action();
    } catch {
      if (!actionIsActive()) return;
      setActionError(
        kind === "create"
          ? "Could not create a new conversation."
          : kind === "retry"
            ? "Could not retry conversation history."
            : "Could not load more conversations.",
      );
    } finally {
      if (actionIsActive()) setPendingListAction(null);
    }
  };

  const renameKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      cancelRename();
    }
  };

  const deleteDialogKeyDown = (event: KeyboardEvent<HTMLDivElement>, id: string) => {
    if (event.key === "Escape" && pendingId !== id) {
      event.preventDefault();
      event.stopPropagation();
      cancelDelete();
      return;
    }
  };

  return (
    <nav className={styles.rail} aria-label="Chat conversations">
      <div className={styles.header}>
        <h2 className={styles.heading}>Conversations</h2>
        <button
          ref={newChatButton}
          className={styles.newButton}
          type="button"
          onClick={() => void runListAction("create", onCreate)}
          disabled={mutationsDisabled || pendingListAction !== null}
        >
          <span aria-hidden="true">+</span>
          <span>New chat</span>
        </button>
      </div>

      <div className={styles.scrollRegion}>
        {state === "loading" && conversations.length === 0 ? (
          <p className={styles.notice} role="status">
            Loading conversations…
          </p>
        ) : null}
        {state === "error" ? (
          <div className={styles.notice} role="alert">
            <p>{errorMessage ?? "Conversation history is unavailable."}</p>
            {onRetry ? (
              <button
                type="button"
                onClick={() => void runListAction("retry", onRetry)}
                disabled={pendingListAction !== null}
              >
                Retry conversation history
              </button>
            ) : null}
          </div>
        ) : null}
        {state === "ready" && conversations.length === 0 ? (
          <p className={styles.notice}>No conversations yet.</p>
        ) : null}

        {conversations.length > 0 ? (
          <ol className={styles.list} aria-label="Recent conversations">
            {conversations.map((conversation) => {
              const isPending = pendingId === conversation.id;
              return (
                <li className={styles.item} key={conversation.id}>
                  {renamingId === conversation.id ? (
                    <form className={styles.renameForm} onSubmit={(event) => void submitRename(event)}>
                      <label className={styles.srOnly} htmlFor={`rename-${conversation.id}`}>
                        Conversation title
                      </label>
                      <input
                        autoFocus
                        className={styles.renameInput}
                        id={`rename-${conversation.id}`}
                        maxLength={160}
                        value={renameValue}
                        onChange={(event) => setRenameValue(event.target.value)}
                        onKeyDown={renameKeyDown}
                        disabled={isPending}
                      />
                      <div className={styles.confirmActions}>
                        <button type="submit" disabled={!validTitle(renameValue) || isPending}>
                          Save title
                        </button>
                        <button type="button" onClick={cancelRename} disabled={isPending}>
                          Cancel rename
                        </button>
                      </div>
                    </form>
                  ) : (
                    <>
                      <button
                        className={styles.conversationButton}
                        type="button"
                        aria-current={selectedId === conversation.id ? "page" : undefined}
                        aria-label={`Open ${conversation.title}`}
                        onClick={() => onSelect(conversation.id)}
                        disabled={deletingId === conversation.id}
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
                        <button
                          ref={(node) => setButtonRef(renameButtons.current, conversation.id, node)}
                          type="button"
                          aria-label={`Rename ${conversation.title}`}
                          onClick={() => startRename(conversation)}
                          disabled={deletingId === conversation.id}
                        >
                          Rename
                        </button>
                        <button
                          ref={(node) => setButtonRef(deleteButtons.current, conversation.id, node)}
                          type="button"
                          aria-label={`Delete ${conversation.title}`}
                          onClick={() => {
                            setRenamingId(null);
                            setActionError(null);
                            setDeletingId(conversation.id);
                          }}
                          disabled={mutationsDisabled || deletingId === conversation.id}
                        >
                          Delete
                        </button>
                      </div>
                    </>
                  )}

                  {deletingId === conversation.id ? (
                    <div
                      className={styles.deleteConfirm}
                      role="group"
                      aria-label={`Delete ${conversation.title}?`}
                      aria-describedby={`delete-note-${conversation.id}`}
                      onKeyDown={(event) => deleteDialogKeyDown(event, conversation.id)}
                    >
                      <p id={`delete-note-${conversation.id}`}>
                        <strong>Delete {conversation.title}?</strong> This cannot be undone.
                      </p>
                      <div className={styles.confirmActions}>
                        <button type="button" autoFocus onClick={cancelDelete} disabled={isPending}>
                          Cancel delete
                        </button>
                        <button
                          ref={(node) => setButtonRef(deleteConfirmButtons.current, conversation.id, node)}
                          type="button"
                          onClick={() => void confirmDelete(conversation.id)}
                          disabled={mutationsDisabled || isPending}
                        >
                          Delete conversation
                        </button>
                      </div>
                    </div>
                  ) : null}
                </li>
              );
            })}
          </ol>
        ) : null}

        {hasMore ? (
          <button
            className={styles.moreButton}
            type="button"
            onClick={() => void runListAction("load-more", onLoadMore)}
            disabled={state === "loading" || pendingListAction !== null}
          >
            Load more conversations
          </button>
        ) : null}
      </div>
      {actionError ? (
        <p className={styles.actionError} role="alert">
          {actionError}
        </p>
      ) : null}
    </nav>
  );
}

function setButtonRef(map: Map<string, HTMLButtonElement>, id: string, node: HTMLButtonElement | null): void {
  if (node === null) map.delete(id);
  else map.set(id, node);
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
