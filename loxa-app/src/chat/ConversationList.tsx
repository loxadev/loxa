import { Plus, Search } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import type { ConversationGroupLabel } from "./conversationHistory";
import { ConversationHistoryItem } from "./ConversationHistoryItem";
import styles from "./ConversationList.module.css";
import type { ChatSummary, TurnState } from "./historyClient";

export type ConversationListItem = ChatSummary & { terminalState?: TurnState };

type ConversationListGroup = {
  label: ConversationGroupLabel;
  conversations: ConversationListItem[];
};

type Props = {
  conversations: ConversationListItem[];
  groupedConversations: ConversationListGroup[];
  query: string;
  setQuery(query: string): void;
  selectedId: string | null;
  state: "loading" | "ready" | "error";
  errorMessage?: string;
  hasMore: boolean;
  onCreate(): void | Promise<void>;
  onSelect(id: string): void;
  onRename(id: string, title: string): void | Promise<void>;
  onDelete(id: string): Promise<string | null>;
  onLoadMore(): void | Promise<void>;
  onRetry?(): void | Promise<void>;
  isLifecycleActive?(): boolean;
  mutationsDisabled?: boolean;
};

const ACTIVE_TURN_REASON_ID = "conversation-mutation-disabled-reason";

export function ConversationList({
  conversations,
  groupedConversations,
  query,
  setQuery,
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
  const [pendingListAction, setPendingListAction] = useState<"create" | "load-more" | "retry" | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const openButtons = useRef(new Map<string, HTMLButtonElement>());
  const newChatButton = useRef<HTMLButtonElement>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const actionIsActive = () => mounted.current && (isLifecycleActive?.() ?? true);

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

  const conversationIds = groupedConversations.flatMap((group) => group.conversations.map(({ id }) => id));
  const focusAfterDelete = (deletedId: string, replacementId: string | null) => {
    const deletedIndex = conversationIds.indexOf(deletedId);
    const nearestId = conversationIds[deletedIndex + 1] ?? conversationIds[deletedIndex - 1] ?? null;
    const focusId = replacementId ?? nearestId;
    requestAnimationFrame(() => {
      const target = focusId === null ? undefined : openButtons.current.get(focusId);
      if (target) {
        target.focus();
        return;
      }
      newChatButton.current?.focus();
    });
  };

  return (
    <nav className={styles.rail} aria-label="Chat conversations">
      <div className={styles.header}>
        <h2 className={styles.heading}>Conversations</h2>
        <Button
          ref={newChatButton}
          className={styles.newButton}
          onClick={() => void runListAction("create", onCreate)}
          disabled={mutationsDisabled || pendingListAction !== null}
          aria-describedby={mutationsDisabled ? ACTIVE_TURN_REASON_ID : undefined}
          busy={pendingListAction === "create"}
        >
          <Plus aria-hidden="true" focusable="false" />
          <span>New chat</span>
        </Button>
        <label className={styles.searchField}>
          <span className={styles.srOnly}>Search conversations</span>
          <Search aria-hidden="true" focusable="false" />
          <Input
            type="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search conversations"
          />
        </label>
        {mutationsDisabled ? (
          <p className={styles.srOnly} id={ACTIVE_TURN_REASON_ID}>
            Unavailable while a response is active.
          </p>
        ) : null}
      </div>

      <div className={styles.scrollRegion}>
        {state === "loading" ? (
          <p className={styles.notice} role="status">
            {query.trim() || conversations.length > 0 ? "Searching conversations…" : "Loading conversations…"}
          </p>
        ) : null}
        {state === "error" ? (
          <div className={styles.notice} role="alert">
            <p>{errorMessage || "Conversation history is unavailable."}</p>
            {onRetry ? (
              <Button
                variant="secondary"
                onClick={() => void runListAction("retry", onRetry)}
                disabled={pendingListAction !== null}
                busy={pendingListAction === "retry"}
              >
                Retry conversation history
              </Button>
            ) : null}
          </div>
        ) : null}
        {state === "ready" && conversations.length === 0 ? (
          <p className={styles.notice}>{query.trim() ? "No matching conversations." : "No conversations yet."}</p>
        ) : null}

        {groupedConversations.map((group) => {
          const headingId = `conversation-group-${group.label.toLowerCase().replace(/\s+/g, "-")}`;
          return (
            <section className={styles.group} aria-labelledby={headingId} key={group.label}>
              <h3 className={styles.groupHeading} id={headingId}>
                {group.label}
              </h3>
              <ol className={styles.list} aria-label={`${group.label} conversations`}>
                {group.conversations.map((conversation) => (
                  <ConversationHistoryItem
                    key={conversation.id}
                    conversation={conversation}
                    selected={selectedId === conversation.id}
                    mutationsDisabled={mutationsDisabled}
                    mutationDisabledReasonId={ACTIVE_TURN_REASON_ID}
                    onOpen={onSelect}
                    onOpenButton={(node) => setButtonRef(openButtons.current, conversation.id, node)}
                    onRename={onRename}
                    onDelete={onDelete}
                    onDeleteSuccess={focusAfterDelete}
                    isLifecycleActive={isLifecycleActive}
                  />
                ))}
              </ol>
            </section>
          );
        })}

        {hasMore ? (
          <Button
            className={styles.moreButton}
            variant="secondary"
            onClick={() => void runListAction("load-more", onLoadMore)}
            disabled={state === "loading" || pendingListAction !== null}
            busy={pendingListAction === "load-more"}
          >
            Load more conversations
          </Button>
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
