import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ChatPage, ChatSummary } from "./historyClient";
import { useConversationHistory, type ConversationHistoryServices } from "./useConversationHistory";

const endpoint = "http://127.0.0.1:8080";
const token = "secret-control-token";
const chat = (id: string, updatedAtMs: number, title = id): ChatSummary => ({
  id,
  title,
  createdAtMs: updatedAtMs - 1,
  updatedAtMs,
});

function services(overrides: Partial<ConversationHistoryServices> = {}): ConversationHistoryServices {
  return {
    readControlToken: vi.fn().mockResolvedValue(token),
    listChats: vi.fn().mockResolvedValue({ chats: [], nextBefore: null }),
    createChat: vi.fn(),
    getChat: vi.fn(),
    renameChat: vi.fn(),
    deleteChat: vi.fn(),
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("useConversationHistory", () => {
  it("loads only when enabled, reads one token, and selects the first ordered chat", async () => {
    const api = services({
      listChats: vi.fn().mockResolvedValue({ chats: [chat("a", 1), chat("b", 2)], nextBefore: null }),
    });
    const { result, rerender } = renderHook(
      ({ enabled }) => useConversationHistory({ services: api, endpoint, enabled }),
      { initialProps: { enabled: false } },
    );

    expect(api.readControlToken).not.toHaveBeenCalled();
    rerender({ enabled: true });

    await waitFor(() => expect(result.current.state).toBe("ready"));
    expect(result.current.conversations.map(({ id }) => id)).toEqual(["b", "a"]);
    expect(result.current.selection).toEqual({ chatId: "b", revision: 1 });
    expect(api.readControlToken).toHaveBeenCalledTimes(1);
    expect(api.listChats).toHaveBeenCalledWith(endpoint, token, { limit: 30 }, expect.anything());
  });

  it("reports safe errors and retries with a new owned action", async () => {
    const api = services({
      listChats: vi
        .fn()
        .mockRejectedValueOnce(new Error("history unavailable"))
        .mockResolvedValueOnce({ chats: [chat("a", 1)], nextBefore: null }),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));

    await waitFor(() => expect(result.current.state).toBe("error"));
    expect(result.current.errorMessage).toBe("history unavailable");
    await act(() => result.current.retry());
    expect(result.current.state).toBe("ready");
    expect(api.readControlToken).toHaveBeenCalledTimes(2);
  });

  it("paginates, deduplicates, and rejects a repeated cursor", async () => {
    const api = services({
      listChats: vi
        .fn()
        .mockResolvedValueOnce({ chats: [chat("a", 3)], nextBefore: "cursor" })
        .mockResolvedValueOnce({ chats: [chat("a", 4), chat("b", 2)], nextBefore: "cursor" }),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    await act(() => result.current.loadMore());

    expect(result.current.state).toBe("error");
    expect(result.current.errorMessage).toMatch(/repeated cursor/i);
    expect(result.current.conversations).toEqual([chat("a", 4), chat("b", 2)]);
    expect(api.readControlToken).toHaveBeenCalledTimes(2);
  });

  it("exhausts remaining pages for case-insensitive search without refetching page one", async () => {
    const api = services({
      listChats: vi
        .fn()
        .mockResolvedValueOnce({ chats: [chat("a", 3, "Alpha")], nextBefore: "two" })
        .mockResolvedValueOnce({ chats: [chat("b", 2, "Needle haystack")], nextBefore: "three" })
        .mockResolvedValueOnce({ chats: [chat("c", 1, "Other")], nextBefore: null }),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    act(() => result.current.setQuery("NEEDLE"));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    expect(result.current.conversations).toEqual([chat("b", 2, "Needle haystack")]);
    expect(api.listChats).toHaveBeenCalledTimes(3);
    expect(vi.mocked(api.listChats).mock.calls[1]?.[2]).toEqual({ limit: 30, before: "two" });
  });

  it("exhausts search when the query is entered before initial loading finishes", async () => {
    const first = deferred<ChatPage>();
    const api = services({
      listChats: vi
        .fn()
        .mockImplementationOnce(() => first.promise)
        .mockResolvedValueOnce({ chats: [chat("b", 1, "Needle")], nextBefore: null }),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));

    act(() => result.current.setQuery("needle"));
    first.resolve({ chats: [chat("a", 2, "Other")], nextBefore: "two" });

    await waitFor(() => expect(result.current.state).toBe("ready"));
    await waitFor(() => expect(api.listChats).toHaveBeenCalledTimes(2));
    expect(result.current.conversations).toEqual([chat("b", 1, "Needle")]);
  });

  it("fails closed when exhaustive search receives a repeated cursor", async () => {
    const api = services({
      listChats: vi
        .fn()
        .mockResolvedValueOnce({ chats: [chat("a", 2)], nextBefore: "repeat" })
        .mockResolvedValueOnce({ chats: [chat("b", 1)], nextBefore: "repeat" }),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    act(() => result.current.setQuery("missing"));

    await waitFor(() => expect(api.listChats).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.state).toBe("error"));
    expect(result.current.errorMessage).toMatch(/repeated cursor/i);
    expect(result.current.hasMore).toBe(false);
  });

  it("retries the page that failed closed on a repeated cursor", async () => {
    const api = services({
      listChats: vi
        .fn()
        .mockResolvedValueOnce({ chats: [chat("a", 2)], nextBefore: "repeat" })
        .mockResolvedValueOnce({ chats: [], nextBefore: "repeat" })
        .mockResolvedValueOnce({ chats: [chat("b", 1)], nextBefore: null }),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    await act(() => result.current.loadMore());
    expect(result.current.state).toBe("error");

    await act(() => result.current.retry());

    expect(api.listChats).toHaveBeenCalledTimes(3);
    expect(result.current.state).toBe("ready");
    expect(result.current.conversations.map(({ id }) => id)).toEqual(["a", "b"]);
  });

  it("single-flights concurrent load-more and search page requests", async () => {
    const page = deferred<ChatPage>();
    const api = services({
      listChats: vi
        .fn()
        .mockResolvedValueOnce({ chats: [chat("a", 2)], nextBefore: "two" })
        .mockImplementationOnce(() => page.promise),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    act(() => {
      void result.current.loadMore();
      result.current.setQuery("a");
    });
    await waitFor(() => expect(api.listChats).toHaveBeenCalledTimes(2));
    page.resolve({ chats: [], nextBefore: null });
    await waitFor(() => expect(result.current.state).toBe("ready"));

    expect(api.listChats).toHaveBeenCalledTimes(2);
    expect(api.readControlToken).toHaveBeenCalledTimes(2);
  });

  it("aborts owned work and ignores late results when disabled or unmounted", async () => {
    const page = deferred<ChatPage>();
    let signal: AbortSignal | undefined;
    const api = services({
      listChats: vi.fn((_endpoint, _token, _page, options) => {
        signal = options?.signal;
        return page.promise;
      }),
    });
    const { result, rerender, unmount } = renderHook(
      ({ enabled }) => useConversationHistory({ services: api, endpoint, enabled }),
      { initialProps: { enabled: true } },
    );
    await waitFor(() => expect(api.listChats).toHaveBeenCalled());
    rerender({ enabled: false });
    expect(signal?.aborted).toBe(true);
    page.resolve({ chats: [chat("late", 1)], nextBefore: null });
    await act(async () => Promise.resolve());
    expect(result.current.conversations).toEqual([]);
    unmount();

    const unmountPage = deferred<ChatPage>();
    let unmountSignal: AbortSignal | undefined;
    const unmountApi = services({
      listChats: vi.fn((_endpoint, _token, _page, options) => {
        unmountSignal = options?.signal;
        return unmountPage.promise;
      }),
    });
    const active = renderHook(() => useConversationHistory({ services: unmountApi, endpoint, enabled: true }));
    await waitFor(() => expect(unmountApi.listChats).toHaveBeenCalled());
    active.unmount();
    expect(unmountSignal?.aborted).toBe(true);
  });

  it("mutates create and rename state only from backend truth", async () => {
    const created = chat("new", 4, "Backend title");
    const renamed = chat("new", 5, "Canonical rename");
    const api = services({
      createChat: vi.fn().mockRejectedValueOnce(new Error("create failed")).mockResolvedValueOnce(created),
      renameChat: vi.fn().mockResolvedValue(renamed),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    await expect(act(() => result.current.create())).rejects.toThrow("create failed");
    expect(result.current.conversations).toEqual([]);
    expect(result.current.selection).toEqual({ chatId: null, revision: 0 });
    await act(() => result.current.create());
    await act(() => result.current.rename("new", "Client title"));

    expect(result.current.conversations).toEqual([renamed]);
    expect(result.current.selection).toEqual({ chatId: "new", revision: 1 });
    expect(api.readControlToken).toHaveBeenCalledTimes(4);
  });

  it("deletes truthfully and chooses next, then previous, while preserving nonselected selection", async () => {
    const api = services({
      listChats: vi.fn().mockResolvedValue({ chats: [chat("a", 3), chat("b", 2), chat("c", 1)], nextBefore: null }),
      deleteChat: vi.fn().mockResolvedValue(undefined),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    act(() => result.current.select("b"));
    const revision = result.current.selection.revision;

    await expect(act(() => result.current.delete("b"))).resolves.toBe("c");
    expect(result.current.selection).toEqual({ chatId: "c", revision: revision + 1 });
    const nextRevision = result.current.selection.revision;
    await expect(act(() => result.current.delete("a"))).resolves.toBeNull();
    expect(result.current.selection).toEqual({ chatId: "c", revision: nextRevision });
    await expect(act(() => result.current.delete("c"))).resolves.toBeNull();
    expect(result.current.selection).toEqual({ chatId: null, revision: nextRevision + 1 });
  });

  it("leaves list and selection unchanged when delete fails", async () => {
    const original = [chat("a", 2), chat("b", 1)];
    const api = services({
      listChats: vi.fn().mockResolvedValue({ chats: original, nextBefore: null }),
      deleteChat: vi.fn().mockRejectedValue(new Error("delete failed")),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    const selection = result.current.selection;

    await expect(act(() => result.current.delete("a"))).rejects.toThrow("delete failed");

    expect(result.current.conversations).toEqual(original);
    expect(result.current.selection).toEqual(selection);
  });

  it("preserves revision for query, pagination, rename, and reconciliation", async () => {
    const api = services({
      listChats: vi.fn().mockResolvedValue({ chats: [chat("a", 2)], nextBefore: null }),
      renameChat: vi.fn().mockResolvedValue(chat("a", 3, "Renamed")),
    });
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    const revision = result.current.selection.revision;

    act(() => result.current.setQuery("a"));
    await act(() => result.current.loadMore());
    await act(() => result.current.rename("a", "ignored"));
    act(() => result.current.reconcileSummary(chat("a", 4, "Reconciled")));

    expect(result.current.selection.revision).toBe(revision);
  });

  it("adopts existing backend chats, reconciles summaries, and clears exactly once", async () => {
    const api = services();
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    act(() => result.current.adoptCreatedChat(chat("a", 1)));
    act(() => result.current.reconcileSummary(chat("a", 2, "Fresh")));
    expect(result.current.conversations).toEqual([chat("a", 2, "Fresh")]);
    expect(api.createChat).not.toHaveBeenCalled();
    const revision = result.current.selection.revision;
    act(() => result.current.clearAfterSettingsDelete());
    expect(result.current.selection).toEqual({ chatId: null, revision: revision + 1 });
    act(() => result.current.clearAfterSettingsDelete());
    expect(result.current.selection.revision).toBe(revision + 1);
  });

  it("does not expose tokens, services, clients, DOM, portal, turn, or runtime fields", async () => {
    const api = services();
    const { result } = renderHook(() => useConversationHistory({ services: api, endpoint, enabled: true }));
    await waitFor(() => expect(result.current.state).toBe("ready"));

    const keys = Object.keys(result.current).join(" ").toLowerCase();
    expect(keys).not.toMatch(/token|service|client|dom|portal|turn|runtime/);
    expect(JSON.stringify(result.current)).not.toContain(token);
  });
});
