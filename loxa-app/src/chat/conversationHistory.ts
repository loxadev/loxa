import type { ChatPage, ChatSummary } from "./historyClient";

export type ConversationGroupLabel = "Today" | "Yesterday" | "Previous 7 days" | "Older";
export type ConversationGroup = {
  label: ConversationGroupLabel;
  conversations: ChatSummary[];
};
export type ConversationSelection = { chatId: string | null; revision: number };
export type ConversationHistoryState = "loading" | "ready" | "error";
type HistoryRequestOptions = { signal?: AbortSignal };

export type ConversationHistoryServices = {
  readControlToken(endpoint: string): Promise<string>;
  listChats(
    endpoint: string,
    token: string,
    page?: { limit?: number; before?: string },
    options?: HistoryRequestOptions,
  ): Promise<ChatPage>;
  createChat(endpoint: string, token: string, options?: HistoryRequestOptions): Promise<ChatSummary>;
  getChat(endpoint: string, token: string, chatId: string, options?: HistoryRequestOptions): Promise<ChatSummary>;
  renameChat(
    endpoint: string,
    token: string,
    chatId: string,
    title: string,
    options?: HistoryRequestOptions,
  ): Promise<ChatSummary>;
  deleteChat(endpoint: string, token: string, chatId: string, options?: HistoryRequestOptions): Promise<void>;
};

export type ConversationHistoryController = {
  conversations: ChatSummary[];
  groupedConversations: ConversationGroup[];
  selectedChatId: string | null;
  selection: ConversationSelection;
  state: ConversationHistoryState;
  errorMessage: string;
  query: string;
  hasMore: boolean;
  setQuery(query: string): void;
  select(chatId: string | null): void;
  create(): Promise<ChatSummary>;
  rename(chatId: string, title: string): Promise<ChatSummary>;
  delete(chatId: string): Promise<string | null>;
  loadMore(): Promise<void>;
  retry(): Promise<void>;
  adoptCreatedChat(chat: ChatSummary): void;
  reconcileSummary(chat: ChatSummary): void;
  clearAfterSettingsDelete(): void;
};

export const CONVERSATION_HISTORY_PAGE_SIZE = 30;

export function orderConversations(conversations: readonly ChatSummary[]): ChatSummary[] {
  const byId = new Map<string, ChatSummary>();
  for (const conversation of conversations) {
    const existing = byId.get(conversation.id);
    if (!existing || conversation.updatedAtMs >= existing.updatedAtMs) byId.set(conversation.id, conversation);
  }
  return [...byId.values()].sort(
    (left, right) => right.updatedAtMs - left.updatedAtMs || right.id.localeCompare(left.id),
  );
}

export function mergeBackendConversations(
  current: readonly ChatSummary[],
  backend: readonly ChatSummary[],
): ChatSummary[] {
  const backendIds = new Set(backend.map(({ id }) => id));
  return orderConversations([...backend, ...current.filter(({ id }) => !backendIds.has(id))]);
}

export function filterConversations(conversations: readonly ChatSummary[], query: string): ChatSummary[] {
  const normalized = query.trim().toLowerCase();
  if (normalized === "") return [...conversations];
  return conversations.filter(({ title }) => title.toLowerCase().includes(normalized));
}

export function groupConversations(conversations: readonly ChatSummary[], now: Date = new Date()): ConversationGroup[] {
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1).getTime();
  const previousWeek = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 7).getTime();
  const groups = new Map<ConversationGroupLabel, ChatSummary[]>([
    ["Today", []],
    ["Yesterday", []],
    ["Previous 7 days", []],
    ["Older", []],
  ]);

  for (const conversation of orderConversations(conversations)) {
    const updated = conversation.updatedAtMs;
    const label =
      updated >= today
        ? "Today"
        : updated >= yesterday
          ? "Yesterday"
          : updated >= previousWeek
            ? "Previous 7 days"
            : "Older";
    groups.get(label)?.push(conversation);
  }

  return [...groups.entries()]
    .filter(([, entries]) => entries.length > 0)
    .map(([label, entries]) => ({ label, conversations: entries }));
}
