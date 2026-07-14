import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  filterConversations,
  groupConversations,
  mergeBackendConversations,
  orderConversations,
  type ConversationHistoryController,
  type ConversationHistoryServices,
  type ConversationHistoryState,
  type ConversationSelection,
} from "./conversationHistory";
import {
  ConversationHistoryRequests,
  isConversationHistoryInvalidated,
  type ConversationHistoryPageResult,
} from "./conversationHistoryRequests";
import type { ChatPage, ChatSummary } from "./historyClient";

export type { ConversationHistoryController, ConversationHistoryServices } from "./conversationHistory";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Conversation history is unavailable.";
}

export function useConversationHistory({
  services,
  endpoint,
  enabled,
}: {
  services: ConversationHistoryServices;
  endpoint: string;
  enabled: boolean;
}): ConversationHistoryController {
  const [allConversations, setAllConversations] = useState<ChatSummary[]>([]);
  const [selection, setSelection] = useState<ConversationSelection>({ chatId: null, revision: 0 });
  const [state, setState] = useState<ConversationHistoryState>("loading");
  const [message, setMessage] = useState("");
  const [query, setQueryState] = useState("");
  const [nextBefore, setNextBefore] = useState<string | null>(null);
  const allRef = useRef<ChatSummary[]>([]);
  const selectionRef = useRef(selection);
  const queryRef = useRef("");
  const requests = useMemo(() => new ConversationHistoryRequests(services, endpoint), [endpoint, services]);

  useEffect(() => {
    selectionRef.current = selection;
  }, [selection]);

  const commitConversations = useCallback((conversations: ChatSummary[]) => {
    const ordered = orderConversations(conversations);
    allRef.current = ordered;
    setAllConversations(ordered);
    return ordered;
  }, []);

  const commitSelection = useCallback((chatId: string | null) => {
    setSelection((current) => {
      if (current.chatId === chatId) return current;
      const next = { chatId, revision: current.revision + 1 };
      selectionRef.current = next;
      return next;
    });
  }, []);

  const mergeBackendSummaries = useCallback(
    (summaries: readonly ChatSummary[]) => {
      return commitConversations(mergeBackendConversations(allRef.current, summaries));
    },
    [commitConversations],
  );

  const applyPage = useCallback(
    (page: ChatPage) => {
      const ordered = mergeBackendSummaries(page.chats);
      setNextBefore(requests.nextCursor);
      const selected = selectionRef.current.chatId;
      if (selected === null || !ordered.some(({ id }) => id === selected)) commitSelection(ordered[0]?.id ?? null);
    },
    [commitSelection, mergeBackendSummaries, requests],
  );

  const settlePageOutcome = useCallback(
    (outcome: ConversationHistoryPageResult) => {
      if (outcome.kind === "invalidated") return;
      applyPage(outcome.page);
      if (outcome.kind === "repeated-cursor") {
        setMessage("Conversation history returned a repeated cursor.");
        setState("error");
      } else {
        setMessage("");
        setState(queryRef.current.trim() !== "" && requests.hasMore ? "loading" : "ready");
      }
    },
    [applyPage, requests],
  );

  const reportRequestError = useCallback((error: unknown) => {
    if (isConversationHistoryInvalidated(error)) return;
    setMessage(errorMessage(error));
    setState("error");
  }, []);

  const loadAndSettle = useCallback(
    async (before: string | null) => {
      try {
        settlePageOutcome(await requests.loadPage(before));
      } catch (error) {
        reportRequestError(error);
        throw error;
      }
    },
    [reportRequestError, requests, settlePageOutcome],
  );

  const exhaustPages = useCallback(async () => {
    await Promise.resolve();
    setState("loading");
    try {
      const outcome = await requests.exhaust(applyPage);
      if (outcome.kind === "invalidated") return;
      if (outcome.kind === "repeated-cursor") {
        setMessage("Conversation history returned a repeated cursor.");
        setState("error");
      } else {
        setMessage("");
        setState("ready");
      }
    } catch (error) {
      reportRequestError(error);
      throw error;
    }
  }, [applyPage, reportRequestError, requests]);

  useEffect(() => {
    requests.invalidate();
    let disposed = false;
    const initialize = async () => {
      await Promise.resolve();
      if (disposed) return;
      allRef.current = [];
      setAllConversations([]);
      setNextBefore(null);
      setMessage("");
      setState("loading");
      commitSelection(null);
      if (enabled) await loadAndSettle(null);
    };
    void initialize().catch(() => undefined);

    return () => {
      disposed = true;
      requests.invalidate();
    };
  }, [commitSelection, enabled, loadAndSettle, requests]);

  useEffect(() => {
    if (!enabled || query.trim() === "" || nextBefore === null) return;
    let disposed = false;
    const beginSearch = async () => {
      await Promise.resolve();
      if (!disposed) await exhaustPages();
    };
    void beginSearch().catch(() => undefined);
    return () => {
      disposed = true;
    };
  }, [enabled, exhaustPages, nextBefore, query]);

  const runWithToken = useCallback(
    async <T>(operation: (token: string, signal: AbortSignal) => Promise<T>): Promise<T> => {
      try {
        return await requests.runAction(operation);
      } catch (error) {
        reportRequestError(error);
        throw error;
      }
    },
    [reportRequestError, requests],
  );

  const select = useCallback(
    (chatId: string | null) => {
      if (chatId === null || allRef.current.some(({ id }) => id === chatId)) commitSelection(chatId);
    },
    [commitSelection],
  );

  const create = useCallback(async () => {
    const created = await runWithToken((token, signal) => services.createChat(endpoint, token, { signal }));
    mergeBackendSummaries([created]);
    commitSelection(created.id);
    setMessage("");
    setState("ready");
    return created;
  }, [commitSelection, endpoint, mergeBackendSummaries, runWithToken, services]);

  const rename = useCallback(
    async (chatId: string, title: string) => {
      const renamed = await runWithToken((token, signal) =>
        services.renameChat(endpoint, token, chatId, title, { signal }),
      );
      mergeBackendSummaries([renamed]);
      setMessage("");
      setState("ready");
      return renamed;
    },
    [endpoint, mergeBackendSummaries, runWithToken, services],
  );

  const remove = useCallback(
    async (chatId: string) => {
      await runWithToken((token, signal) => services.deleteChat(endpoint, token, chatId, { signal }));
      const current = allRef.current;
      const index = current.findIndex(({ id }) => id === chatId);
      const wasSelected = selectionRef.current.chatId === chatId;
      const replacement = wasSelected ? (current[index + 1]?.id ?? current[index - 1]?.id ?? null) : null;
      commitConversations(current.filter(({ id }) => id !== chatId));
      if (wasSelected) commitSelection(replacement);
      setMessage("");
      setState("ready");
      return replacement;
    },
    [commitConversations, commitSelection, endpoint, runWithToken, services],
  );

  const loadMore = useCallback(async () => {
    if (requests.nextCursor === null) return;
    setState("loading");
    await loadAndSettle(requests.nextCursor);
  }, [loadAndSettle, requests]);

  const retry = useCallback(async () => {
    setMessage("");
    setState("loading");
    try {
      const outcome = await requests.retry();
      if (outcome) settlePageOutcome(outcome);
      else if (allRef.current.length === 0) await loadAndSettle(null);
      else setState("ready");
    } catch (error) {
      reportRequestError(error);
      throw error;
    }
  }, [loadAndSettle, reportRequestError, requests, settlePageOutcome]);

  const adoptCreatedChat = useCallback(
    (chat: ChatSummary) => {
      mergeBackendSummaries([chat]);
      commitSelection(chat.id);
    },
    [commitSelection, mergeBackendSummaries],
  );

  const reconcileSummary = useCallback(
    (chat: ChatSummary) => {
      mergeBackendSummaries([chat]);
    },
    [mergeBackendSummaries],
  );

  const clearAfterSettingsDelete = useCallback(() => {
    requests.invalidate();
    commitConversations([]);
    setNextBefore(null);
    commitSelection(null);
    setMessage("");
    setState("ready");
  }, [commitConversations, commitSelection, requests]);

  const setQuery = useCallback(
    (value: string) => {
      queryRef.current = value;
      setQueryState(value);
      if (value.trim() !== "" && requests.hasMore) setState("loading");
    },
    [requests],
  );
  const conversations = useMemo(() => filterConversations(allConversations, query), [allConversations, query]);
  const groupedConversations = useMemo(() => groupConversations(conversations), [conversations]);

  return {
    conversations,
    groupedConversations,
    selectedChatId: selection.chatId,
    selection,
    state,
    errorMessage: message,
    query,
    hasMore: nextBefore !== null,
    setQuery,
    select,
    create,
    rename,
    delete: remove,
    loadMore,
    retry,
    adoptCreatedChat,
    reconcileSummary,
    clearAfterSettingsDelete,
  };
}
