import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  CONVERSATION_HISTORY_PAGE_SIZE,
  filterConversations,
  groupConversations,
  mergeBackendConversations,
  orderConversations,
  type ConversationHistoryController,
  type ConversationHistoryServices,
  type ConversationHistoryState,
  type ConversationSelection,
} from "./conversationHistory";
import type { ChatSummary } from "./historyClient";

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
  const cursorRef = useRef<string | null>(null);
  const seenCursorsRef = useRef(new Set<string>());
  const pageFailedRef = useRef(false);
  const failedBeforeRef = useRef<string | null>(null);
  const controllersRef = useRef(new Set<AbortController>());
  const pageFlightRef = useRef<Promise<void> | null>(null);
  const generationRef = useRef(0);
  const enabledRef = useRef(enabled);

  useEffect(() => {
    enabledRef.current = enabled;
  }, [enabled]);

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

  const abortOwnedWork = useCallback(() => {
    for (const controller of controllersRef.current) controller.abort();
    controllersRef.current.clear();
    pageFlightRef.current = null;
  }, []);

  const ownAction = useCallback(() => {
    const controller = new AbortController();
    controllersRef.current.add(controller);
    return {
      controller,
      finish: () => controllersRef.current.delete(controller),
    };
  }, []);

  const mergeBackendSummaries = useCallback(
    (summaries: readonly ChatSummary[]) => {
      return commitConversations(mergeBackendConversations(allRef.current, summaries));
    },
    [commitConversations],
  );

  const loadPage = useCallback(
    (before: string | null): Promise<void> => {
      if (pageFlightRef.current) return pageFlightRef.current;
      const generation = generationRef.current;
      const owned = ownAction();
      const flight = (async () => {
        try {
          const token = await services.readControlToken(endpoint);
          if (owned.controller.signal.aborted) return;
          const page = await services.listChats(
            endpoint,
            token,
            before === null
              ? { limit: CONVERSATION_HISTORY_PAGE_SIZE }
              : { limit: CONVERSATION_HISTORY_PAGE_SIZE, before },
            { signal: owned.controller.signal },
          );
          if (owned.controller.signal.aborted || generation !== generationRef.current || !enabledRef.current) return;
          const ordered = mergeBackendSummaries(page.chats);
          cursorRef.current = page.nextBefore;
          setNextBefore(page.nextBefore);
          if (page.nextBefore !== null) {
            if (seenCursorsRef.current.has(page.nextBefore)) {
              cursorRef.current = null;
              setNextBefore(null);
              pageFailedRef.current = true;
              failedBeforeRef.current = before;
              setState("error");
              setMessage("Conversation history returned a repeated cursor.");
              return;
            }
            seenCursorsRef.current.add(page.nextBefore);
          }
          const selected = selectionRef.current.chatId;
          if (selected === null || !ordered.some(({ id }) => id === selected)) commitSelection(ordered[0]?.id ?? null);
          failedBeforeRef.current = null;
          setMessage("");
          setState("ready");
        } catch (error) {
          if (owned.controller.signal.aborted || generation !== generationRef.current || !enabledRef.current) return;
          pageFailedRef.current = true;
          failedBeforeRef.current = before;
          setMessage(errorMessage(error));
          setState("error");
          throw error;
        } finally {
          owned.finish();
        }
      })();
      pageFlightRef.current = flight;
      void flight.then(
        () => {
          if (pageFlightRef.current === flight) pageFlightRef.current = null;
        },
        () => {
          if (pageFlightRef.current === flight) pageFlightRef.current = null;
        },
      );
      return flight;
    },
    [commitSelection, endpoint, mergeBackendSummaries, ownAction, services],
  );

  const exhaustPages = useCallback(async () => {
    pageFailedRef.current = false;
    setState("loading");
    while (cursorRef.current !== null && enabledRef.current) {
      await loadPage(cursorRef.current);
      if (pageFlightRef.current) await pageFlightRef.current;
      if (pageFailedRef.current) return;
    }
    if (enabledRef.current) setState("ready");
  }, [loadPage]);

  useEffect(() => {
    generationRef.current += 1;
    const generation = generationRef.current;
    abortOwnedWork();
    const initialize = async () => {
      await Promise.resolve();
      if (generation !== generationRef.current) return;
      allRef.current = [];
      cursorRef.current = null;
      seenCursorsRef.current = new Set();
      pageFailedRef.current = false;
      failedBeforeRef.current = null;
      setAllConversations([]);
      setNextBefore(null);
      setMessage("");
      setState("loading");
      commitSelection(null);
      if (enabled) await loadPage(null);
    };
    void initialize().catch(() => undefined);

    return () => {
      if (generation === generationRef.current) generationRef.current += 1;
      abortOwnedWork();
    };
  }, [abortOwnedWork, commitSelection, enabled, endpoint, loadPage, services]);

  useEffect(() => {
    if (!enabled || query.trim() === "" || cursorRef.current === null) return;
    void exhaustPages().catch(() => undefined);
  }, [enabled, exhaustPages, nextBefore, query]);

  const runWithToken = useCallback(
    async <T>(operation: (token: string, signal: AbortSignal) => Promise<T>): Promise<T> => {
      const generation = generationRef.current;
      const owned = ownAction();
      try {
        const token = await services.readControlToken(endpoint);
        if (owned.controller.signal.aborted) throw new DOMException("Aborted", "AbortError");
        const result = await operation(token, owned.controller.signal);
        if (owned.controller.signal.aborted || generation !== generationRef.current || !enabledRef.current)
          throw new DOMException("Aborted", "AbortError");
        return result;
      } catch (error) {
        if (!owned.controller.signal.aborted && generation === generationRef.current && enabledRef.current) {
          setMessage(errorMessage(error));
          setState("error");
        }
        throw error;
      } finally {
        owned.finish();
      }
    },
    [endpoint, ownAction, services],
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
    if (cursorRef.current === null) return;
    pageFailedRef.current = false;
    setState("loading");
    await loadPage(cursorRef.current);
  }, [loadPage]);

  const retry = useCallback(async () => {
    const retryBefore = failedBeforeRef.current;
    const retryFailedPage = pageFailedRef.current;
    pageFailedRef.current = false;
    setMessage("");
    setState("loading");
    if (retryFailedPage) {
      if (retryBefore === null) seenCursorsRef.current = new Set();
      await loadPage(retryBefore);
    } else if (allRef.current.length === 0) {
      seenCursorsRef.current = new Set();
      await loadPage(null);
    } else if (cursorRef.current !== null) await loadPage(cursorRef.current);
    else setState("ready");
  }, [loadPage]);

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
    commitConversations([]);
    cursorRef.current = null;
    seenCursorsRef.current = new Set();
    pageFailedRef.current = false;
    failedBeforeRef.current = null;
    setNextBefore(null);
    commitSelection(null);
    setMessage("");
    setState("ready");
  }, [commitConversations, commitSelection]);

  const setQuery = useCallback((value: string) => setQueryState(value), []);
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
