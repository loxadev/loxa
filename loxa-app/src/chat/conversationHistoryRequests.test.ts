import { describe, expect, it, vi } from "vitest";

import type { ChatPage } from "./historyClient";
import { ConversationHistoryRequests, isConversationHistoryInvalidated } from "./conversationHistoryRequests";

const endpoint = "http://127.0.0.1:8080";
const token = "secret-control-token";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function services(listChats: (before?: string, signal?: AbortSignal) => Promise<ChatPage>) {
  return {
    readControlToken: vi.fn().mockResolvedValue(token),
    listChats: vi.fn(
      (_endpoint: string, _token: string, page?: { before?: string }, options?: { signal?: AbortSignal }) =>
        listChats(page?.before, options?.signal),
    ),
  };
}

describe("ConversationHistoryRequests", () => {
  it("progresses cursors and exhausts pages with one token read per request", async () => {
    const api = services(async (before) =>
      before === undefined
        ? { chats: [], nextBefore: "two" }
        : before === "two"
          ? { chats: [], nextBefore: "three" }
          : { chats: [], nextBefore: null },
    );
    const requests = new ConversationHistoryRequests(api, endpoint);
    const initial = await requests.loadPage(null);
    const pages: ChatPage[] = [];

    const outcome = await requests.exhaust((page) => pages.push(page));

    expect(initial.kind).toBe("page");
    expect(outcome.kind).toBe("page");
    expect(pages.map(({ nextBefore }) => nextBefore)).toEqual(["three", null]);
    expect(requests.hasMore).toBe(false);
    expect(api.readControlToken).toHaveBeenCalledTimes(3);
  });

  it("fails closed on repeated cursors and retries the failed page", async () => {
    const api = services(
      vi
        .fn()
        .mockResolvedValueOnce({ chats: [], nextBefore: "repeat" })
        .mockResolvedValueOnce({ chats: [], nextBefore: "repeat" })
        .mockResolvedValueOnce({ chats: [], nextBefore: null }),
    );
    const requests = new ConversationHistoryRequests(api, endpoint);
    await requests.loadPage(null);

    const repeated = await requests.loadPage("repeat");
    const retried = await requests.retry();

    expect(repeated.kind).toBe("repeated-cursor");
    expect(requests.hasMore).toBe(false);
    expect(retried?.kind).toBe("page");
    expect(api.listChats).toHaveBeenCalledTimes(3);
  });

  it("single-flights concurrent page and exhaustive requests", async () => {
    const pending = deferred<ChatPage>();
    const api = services(
      vi.fn().mockResolvedValueOnce({ chats: [], nextBefore: "two" }).mockReturnValueOnce(pending.promise),
    );
    const requests = new ConversationHistoryRequests(api, endpoint);
    await requests.loadPage(null);

    const page = requests.loadPage("two");
    const exhaust = requests.exhaust(() => undefined);
    pending.resolve({ chats: [], nextBefore: null });
    await Promise.all([page, exhaust]);

    expect(api.listChats).toHaveBeenCalledTimes(2);
    expect(api.readControlToken).toHaveBeenCalledTimes(2);
  });

  it("invalidates pending pages and actions, aborts signals, and ignores late results", async () => {
    const page = deferred<ChatPage>();
    let pageSignal: AbortSignal | undefined;
    const api = services((_before, signal) => {
      pageSignal = signal;
      return page.promise;
    });
    const requests = new ConversationHistoryRequests(api, endpoint);
    const pendingPage = requests.loadPage(null);
    await vi.waitFor(() => expect(api.listChats).toHaveBeenCalled());

    const action = deferred<string>();
    let actionSignal: AbortSignal | undefined;
    const pendingAction = requests.runAction((_token, signal) => {
      actionSignal = signal;
      return action.promise;
    });
    await vi.waitFor(() => expect(api.readControlToken).toHaveBeenCalledTimes(2));
    requests.invalidate();

    expect(pageSignal?.aborted).toBe(true);
    expect(actionSignal?.aborted).toBe(true);
    page.resolve({ chats: [], nextBefore: "late" });
    action.resolve("late");

    await expect(pendingPage).resolves.toEqual({ kind: "invalidated" });
    await expect(pendingAction).rejects.toSatisfy(isConversationHistoryInvalidated);
    expect(requests.hasMore).toBe(false);
  });
});
