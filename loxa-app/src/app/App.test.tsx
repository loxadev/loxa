import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { App, GlobalConversationRail, type AppServices } from "./App";
import type { ConversationHistoryController } from "../chat/conversationHistory";
import type { ControlStreamCallbacks, ControlStreamTerminal, V2StreamCallbacks } from "../control/events";
import { decodeV2OperationAccepted, decodeV2ReconnectSnapshot, type OperationView } from "../control/contracts";
import { validV2Node, validV2Operation, validV2OperationAccepted, validV2Slot, v2Ids } from "../control/testSupport";
import type { BootstrapSnapshot } from "../node/NodeSession";
import type { NodeStatus } from "../node/contracts";
import { useWorkspaceStore } from "../stores/workspace-store";
import { controlSnapshot, modelFixture, servicesWithControl, testPeer } from "../node/testSupport";

function services(): AppServices {
  const api: AppServices = {
    ...servicesWithControl(),
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue({
        ownership: "none",
        endpoint: "http://127.0.0.1:8080",
        childRunning: false,
        error: null,
      }),
      start: vi.fn().mockResolvedValue({
        ownership: "owned",
        endpoint: "http://127.0.0.1:8080",
        childRunning: true,
        error: null,
      }),
      attach: vi.fn(),
      stop: vi.fn(),
    },
    getStatus: vi.fn().mockResolvedValue({
      node_id: "node-7",
      health: "unavailable",
      model: "loxa",
      engine: null,
      runtime_model: null,
      profile: null,
    }),
    getModels: vi.fn().mockResolvedValue({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] }),
    getCapabilities: vi
      .fn()
      .mockResolvedValue({ documentInput: false, documentInputReason: "Not supported.", textChat: true }),
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    getControlNode: vi
      .fn()
      .mockResolvedValue({ status: "unloaded", activeModelId: null, operationId: null, error: null }),
    getInventory: vi.fn().mockResolvedValue([]),
    loadModel: vi.fn(),
    getOperation: vi.fn(),
    createControlEventStream: vi.fn(() => ({
      cancel: vi.fn(),
      dispose: vi.fn(),
      finished: new Promise<ControlStreamTerminal>(() => undefined),
    })),
    createChatStream: vi.fn(),
    copyText: vi.fn(),
  };
  let revision = 11;
  const operationIds = new Map<string, string>();
  const operationId = (legacy: string) => {
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(legacy)) return legacy;
    const existing = operationIds.get(legacy);
    if (existing) return existing;
    const generated = `123e4567-e89b-42d3-9456-${(operationIds.size + 5000).toString(16).padStart(12, "0")}`;
    operationIds.set(legacy, generated);
    return generated;
  };
  api.openV2Events = vi.fn((_peer, _resume, callbacks: V2StreamCallbacks, signal) => {
    const publish = async (operations: OperationView[] = []) => {
      const status =
        (await api.getStatus("http://127.0.0.1:8080", { signal })) ??
        ({
          node_id: "node-7",
          health: "unavailable",
          model: "loxa",
          engine: null,
          runtime_model: null,
          profile: null,
        } satisfies NodeStatus);
      if (signal?.aborted) return;
      revision += 1;
      const active = operations.find(
        (operation) =>
          (operation.kind === "load" || operation.kind === "unload") &&
          (operation.status === "queued" || operation.status === "running"),
      );
      callbacks.onSnapshot(
        decodeV2ReconnectSnapshot({
          schema_version: 2,
          epoch: v2Ids.epoch,
          revision: String(revision),
          generated_at_unix_ms: String(Date.now()),
          stream: { epoch: v2Ids.epoch, cursor: String(revision), cursor_gap: false },
          nodes: [validV2Node],
          slots: [
            {
              ...validV2Slot,
              status: status.health === "ready" ? "ready" : active?.kind === "load" ? "loading" : "unloaded",
              model_id: status.runtime_model,
              operation_id: active ? operationId(active.id) : null,
            },
          ],
          operations: operations
            .map((operation) => ({
              operation_id: operationId(operation.id),
              node_id: v2Ids.node,
              kind: operation.kind,
              status: operation.status,
              slot_id: operation.kind === "download" ? null : v2Ids.slot,
              model_id: operation.modelId,
              progress:
                operation.progress === null
                  ? null
                  : {
                      completed_bytes: String(operation.progress.completedBytes),
                      total_bytes:
                        operation.progress.totalBytes === null ? null : String(operation.progress.totalBytes),
                    },
              error:
                operation.error === null
                  ? null
                  : {
                      code:
                        operation.kind === "download"
                          ? "download_failed"
                          : operation.kind === "load"
                            ? "load_failed"
                            : "unload_failed",
                      message: operation.error,
                    },
              created_revision: "1",
              updated_revision: String(revision),
              created_at_unix_ms: String(operation.createdAtUnixMs),
              updated_at_unix_ms: String(operation.updatedAtUnixMs),
            }))
            .sort((left, right) => left.operation_id.localeCompare(right.operation_id)),
          events: [],
        }),
      );
    };
    const legacy = api.createControlEventStream(
      "http://127.0.0.1:8080",
      "ab".repeat(32),
      0,
      {
        onSnapshot: (snapshot) => void publish(snapshot.operations),
        onEvent: (event) => void publish([event.operation]),
        onTerminal: (terminal) =>
          callbacks.onTerminal(
            terminal.kind === "cancelled"
              ? { kind: "cancelled", cursor: String(terminal.cursor) }
              : { kind: "error", cursor: String(terminal.cursor), message: terminal.message },
          ),
      },
      signal,
    );
    void publish();
    return {
      cancel: legacy.cancel,
      dispose: legacy.dispose,
      finished: legacy.finished.then((terminal) =>
        terminal.kind === "cancelled"
          ? { kind: "cancelled" as const, cursor: String(terminal.cursor) }
          : { kind: "error" as const, cursor: String(terminal.cursor), message: terminal.message },
      ),
    };
  });
  return api;
}

async function chooseChatModel(
  user: ReturnType<typeof userEvent.setup>,
  modelId: string,
  action: "Load" | "Switch to",
) {
  await user.click(await screen.findByRole("button", { name: "Choose model" }));
  await user.click(await screen.findByRole("option", { name: modelId }));
  await user.click(screen.getByRole("button", { name: `${action} ${modelId}` }));
}

describe("App", () => {
  beforeEach(() => {
    window.localStorage.clear();
    useWorkspaceStore.setState({
      activeRoute: "chat",
      activeSettingsPage: "overview",
      sidebarCollapsed: false,
      expandedSidebarWidth: 280,
    });
  });

  it("opens on Chat and keeps the primary navigation in product order", async () => {
    const user = userEvent.setup();
    render(<App services={services()} />);

    expect(await screen.findByRole("heading", { name: "New Chat" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Chat" })).toHaveAttribute("aria-current", "page");
    expect(screen.queryByRole("navigation", { name: "Chat conversations" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "New chat" })).not.toBeInTheDocument();

    const primary = screen.getByRole("navigation", { name: "Primary navigation" });
    expect(
      within(primary)
        .getAllByRole("link")
        .map((link) => link.getAttribute("aria-label")),
    ).toEqual(["Chat", "Models", "Node"]);

    await user.click(screen.getByRole("link", { name: "Models" }));
    expect(screen.getByRole("heading", { name: "Models" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Models" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "Node" }));
    expect(screen.getByRole("heading", { name: "Nodes" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Node" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "Settings" }));
    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("aria-current", "page");
  });

  it("routes the authoritative unloaded Chat action to Models", async () => {
    const user = userEvent.setup();
    const api = services();
    vi.mocked(api.getStatus).mockResolvedValue({
      node_id: "node-7",
      health: "unavailable",
      model: "loxa",
      engine: null,
      runtime_model: null,
      profile: null,
    });
    vi.mocked(api.getControlNode).mockResolvedValue({
      status: "unloaded",
      activeModelId: null,
      operationId: null,
      error: null,
    });
    vi.mocked(api.getInventory).mockResolvedValue([]);

    render(<App services={api} />);
    await user.click(await screen.findByRole("button", { name: "Open Models" }));

    expect(screen.getByRole("heading", { name: "Models" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Models" })).toHaveAttribute("aria-current", "page");
  });

  it("threads the Settings lifecycle signal through authenticated history clearing", async () => {
    const user = userEvent.setup();
    const api = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    const turnId = "1123456789abcdef0123456789abcdef";
    api.clearChats = vi.fn().mockResolvedValue({ deleted: 2 });
    api.listChats = vi.fn().mockResolvedValue({
      chats: [{ id: chatId, title: "Clear me", createdAtMs: 1, updatedAtMs: 2 }],
      nextBefore: null,
    });
    api.createChat = vi.fn();
    api.getChat = vi.fn();
    api.renameChat = vi.fn();
    api.deleteChat = vi.fn();
    api.listTurns = vi.fn().mockResolvedValue({
      turns: [
        {
          id: turnId,
          chatId,
          ordinal: 0,
          state: "completed",
          modelAlias: "loxa",
          recipeId: "gemma",
          engineName: "llama.cpp",
          engineVersion: null,
          errorCode: null,
          createdAtMs: 1,
          updatedAtMs: 2,
        },
      ],
      nextAfter: null,
    });
    api.listMessageSummaries = vi.fn().mockResolvedValue([
      {
        id: "2123456789abcdef0123456789abcdef",
        turnId,
        role: "user",
        contentBytes: 6,
        createdAtMs: 1,
        updatedAtMs: 1,
      },
      {
        id: "3123456789abcdef0123456789abcdef",
        turnId,
        role: "assistant",
        contentBytes: 8,
        createdAtMs: 2,
        updatedAtMs: 2,
      },
    ]);
    api.getMessageContent = vi.fn((_endpoint, _token, _chatId, _turnId, messageId) =>
      Promise.resolve(messageId.startsWith("2") ? "Prompt" : "Clear this transcript"),
    );
    render(<App services={api} />);
    await screen.findByRole("link", { name: "Node online. No active model" });
    expect(await screen.findByText("Clear this transcript")).toBeVisible();
    await user.click(screen.getByRole("link", { name: "Settings" }));
    await user.click(screen.getByRole("button", { name: "Clear chat history" }));
    await user.click(screen.getByRole("button", { name: "Confirm clear chat history" }));

    await waitFor(() =>
      expect(api.clearChats).toHaveBeenCalledWith("http://127.0.0.1:8080", "ab".repeat(32), {
        signal: expect.any(AbortSignal),
      }),
    );
    expect(await screen.findByRole("status")).toHaveTextContent("Deleted 2 conversations");
    expect(screen.queryByRole("button", { name: "Open Clear me" })).not.toBeInTheDocument();
    expect(screen.getByText("No conversations yet.")).toBeVisible();
    await user.click(screen.getByRole("link", { name: "Chat" }));
    expect(screen.queryByText("Clear this transcript")).not.toBeInTheDocument();
  });

  it("keeps Chat, Models, and Node primary while Settings remains in the footer", async () => {
    render(<App services={services()} />);
    await screen.findByRole("link", { name: "Node online. No active model" });

    const primary = screen.getByRole("navigation", { name: "Primary navigation" });
    const secondary = screen.getByRole("navigation", { name: "Application settings" });
    expect(primary).toContainElement(screen.getByRole("link", { name: "Chat" }));
    expect(primary).toContainElement(screen.getByRole("link", { name: "Node" }));
    expect(primary).toContainElement(screen.getByRole("link", { name: "Models" }));
    expect(secondary).toContainElement(screen.getByRole("link", { name: "Settings" }));
  });

  it("closes the observability inspector when the workspace becomes compact", async () => {
    const user = userEvent.setup();
    const compactListeners = new Set<(event: MediaQueryListEvent) => void>();
    let compact = false;
    const compactQuery = "(max-width: 760px)";
    const matchMedia = vi.fn((query: string) => {
      const listeners = query === compactQuery ? compactListeners : new Set<(event: MediaQueryListEvent) => void>();
      return {
        matches: query === compactQuery ? compact : false,
        media: query,
        onchange: null,
        addEventListener: (_type: string, listener: EventListenerOrEventListenerObject) =>
          listeners.add(listener as (event: MediaQueryListEvent) => void),
        removeEventListener: (_type: string, listener: EventListenerOrEventListenerObject) =>
          listeners.delete(listener as (event: MediaQueryListEvent) => void),
        addListener: () => undefined,
        removeListener: () => undefined,
        dispatchEvent: () => true,
      } as MediaQueryList;
    });
    vi.stubGlobal("matchMedia", matchMedia);

    render(<App services={services()} />);
    const trigger = await screen.findByRole("button", { name: "Node online" });
    await user.click(trigger);
    expect(trigger).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("complementary", { name: "Health and observability inspector" })).toBeInTheDocument();

    compact = true;
    act(() => {
      compactListeners.forEach((listener) => listener({ matches: true, media: compactQuery } as MediaQueryListEvent));
    });

    await waitFor(() => expect(trigger).toHaveAttribute("aria-expanded", "false"));
    expect(screen.queryByRole("complementary", { name: "Health and observability inspector" })).not.toBeInTheDocument();
    vi.unstubAllGlobals();
  });

  it("returns focus to the health trigger after closing observability", async () => {
    const user = userEvent.setup();
    render(<App services={services()} />);
    const trigger = await screen.findByRole("button", { name: "Node online" });
    await user.click(trigger);

    await user.click(screen.getByRole("button", { name: "Close observability" }));

    expect(trigger).toHaveFocus();
    expect(trigger).toHaveAttribute("aria-expanded", "false");
  });

  it("places Chat history in the global resizable rail and keeps route navigation", async () => {
    const user = userEvent.setup();
    const api = services();
    const chatId = "0123456789abcdef0123456789abcdef";
    api.listChats = vi.fn().mockResolvedValue({
      chats: [{ id: chatId, title: "Runtime notes", createdAtMs: 1, updatedAtMs: 2 }],
      nextBefore: null,
    });
    api.createChat = vi.fn();
    api.getChat = vi.fn();
    api.renameChat = vi.fn();
    api.deleteChat = vi.fn();
    api.listTurns = vi.fn().mockResolvedValue({ turns: [], nextAfter: null });
    api.listMessageSummaries = vi.fn();
    api.getMessageContent = vi.fn();
    const removeListener = vi.spyOn(window, "removeEventListener");
    const view = render(<App services={api} />);
    await screen.findByRole("link", { name: "Node online. No active model" });

    const globalRail = screen.getByRole("complementary", { name: "Primary" });
    expect(within(globalRail).getByRole("navigation", { name: "Chat conversations" })).toBeVisible();
    expect(await within(globalRail).findByRole("button", { name: "Open Runtime notes" })).toBeVisible();
    const historySearch = within(globalRail).getByRole("searchbox", { name: "Search conversations" });
    await user.type(historySearch, "missing");
    expect(within(globalRail).queryByRole("button", { name: "Open Runtime notes" })).not.toBeInTheDocument();
    expect(api.listChats).toHaveBeenCalledTimes(1);
    await user.clear(historySearch);
    expect(await within(globalRail).findByRole("button", { name: "Open Runtime notes" })).toBeVisible();
    expect(
      within(screen.getByRole("main")).queryByRole("navigation", { name: "Chat conversations" }),
    ).not.toBeInTheDocument();
    expect(within(globalRail).getByRole("link", { name: "Node" })).toBeVisible();
    expect(within(globalRail).getByRole("link", { name: "Settings" })).toBeVisible();

    for (const route of ["Models", "Node", "Settings", "Chat"]) {
      await user.click(within(globalRail).getByRole("link", { name: route }));
      expect(await within(globalRail).findByRole("button", { name: "Open Runtime notes" })).toBeVisible();
      expect(within(screen.getByRole("main")).queryByRole("navigation", { name: "Chat conversations" })).toBeNull();
    }
    expect(api.listChats).toHaveBeenCalledTimes(1);

    const separator = screen.getByRole("separator", { name: "Resize conversation rail" });
    const shell = screen.getByTestId("app-shell");
    expect(shell.style.getPropertyValue("--loxa-sidebar-width")).toBe("328px");
    expect(separator).toHaveAttribute("aria-valuemin", "240");
    expect(separator).toHaveAttribute("aria-valuemax", "400");
    fireEvent.keyDown(separator, { key: "End" });
    expect(separator).toHaveAttribute("aria-valuenow", "400");
    fireEvent.keyDown(separator, { key: "ArrowRight" });
    expect(separator).toHaveAttribute("aria-valuenow", "400");
    fireEvent.keyDown(separator, { key: "Home" });
    expect(separator).toHaveAttribute("aria-valuenow", "240");
    fireEvent.keyDown(separator, { key: "ArrowLeft" });
    expect(separator).toHaveAttribute("aria-valuenow", "240");
    fireEvent.doubleClick(separator);
    expect(shell.style.getPropertyValue("--loxa-sidebar-width")).toBe("328px");
    expect(separator).toHaveAttribute("aria-valuenow", "280");
    const setPointerCapture = vi.fn();
    const releasePointerCapture = vi.fn();
    Object.assign(separator, { setPointerCapture, releasePointerCapture, hasPointerCapture: () => true });
    fireEvent.pointerDown(separator, { pointerId: 7, button: 0, clientX: 328 });
    expect(setPointerCapture).toHaveBeenCalledWith(7);
    fireEvent.pointerMove(window, { pointerId: 8, clientX: 700 });
    expect(separator).toHaveAttribute("aria-valuenow", "280");
    fireEvent.pointerMove(window, { pointerId: 7, clientX: 700 });
    expect(separator).toHaveAttribute("aria-valuenow", "400");
    fireEvent.pointerMove(window, { pointerId: 7, clientX: -200 });
    expect(separator).toHaveAttribute("aria-valuenow", "240");
    expect(useWorkspaceStore.getState().sidebarCollapsed).toBe(false);
    fireEvent.pointerCancel(window, { pointerId: 7 });
    expect(releasePointerCapture).toHaveBeenCalledWith(7);

    await user.click(screen.getByRole("button", { name: "Hide conversations" }));
    expect(shell.style.getPropertyValue("--loxa-sidebar-width")).toBe("48px");
    expect(screen.queryByRole("separator", { name: "Resize conversation rail" })).not.toBeInTheDocument();
    for (const name of ["Chat", "Models", "Node", "Settings", "Node online. No active model"]) {
      expect(screen.getByRole("link", { name })).toBeVisible();
    }
    expect(screen.getByRole("button", { name: "Show conversations" })).toBeVisible();
    expect(document.querySelectorAll(".app-sidebar svg:not([aria-hidden='true'])")).toHaveLength(0);

    await user.click(screen.getByRole("button", { name: "Show conversations" }));
    expect(shell.style.getPropertyValue("--loxa-sidebar-width")).toBe("288px");
    await user.click(screen.getByRole("button", { name: "Hide conversations" }));

    view.unmount();
    render(<App services={api} />);
    expect(screen.getByTestId("app-shell").style.getPropertyValue("--loxa-sidebar-width")).toBe("48px");
    await user.click(screen.getByRole("button", { name: "Show conversations" }));
    expect(screen.getByTestId("app-shell").style.getPropertyValue("--loxa-sidebar-width")).toBe("288px");

    expect(removeListener).toHaveBeenCalledWith("pointermove", expect.any(Function));
    expect(removeListener).toHaveBeenCalledWith("pointerup", expect.any(Function));
    expect(removeListener).toHaveBeenCalledWith("pointercancel", expect.any(Function));
    removeListener.mockRestore();
  });

  it("selects history off-route, navigates to Chat, and restores the selection exactly once", async () => {
    useWorkspaceStore.setState({ activeRoute: "models" });
    const user = userEvent.setup();
    const api = services();
    const firstChat = "0123456789abcdef0123456789abcdef";
    const selectedChat = "1123456789abcdef0123456789abcdef";
    const turnId = "2123456789abcdef0123456789abcdef";
    const userMessage = "3123456789abcdef0123456789abcdef";
    const assistantMessage = "4123456789abcdef0123456789abcdef";
    api.listChats = vi.fn().mockResolvedValue({
      chats: [
        { id: firstChat, title: "First chat", createdAtMs: 1, updatedAtMs: 3 },
        { id: selectedChat, title: "Selected chat", createdAtMs: 1, updatedAtMs: 2 },
      ],
      nextBefore: null,
    });
    api.createChat = vi.fn();
    api.getChat = vi.fn();
    api.renameChat = vi.fn();
    api.deleteChat = vi.fn();
    api.listTurns = vi.fn().mockResolvedValue({
      turns: [
        {
          id: turnId,
          chatId: selectedChat,
          ordinal: 0,
          state: "completed",
          modelAlias: "loxa",
          recipeId: "gemma",
          engineName: "llama.cpp",
          engineVersion: null,
          errorCode: null,
          createdAtMs: 1,
          updatedAtMs: 2,
        },
      ],
      nextAfter: null,
    });
    api.listMessageSummaries = vi.fn().mockResolvedValue([
      { id: userMessage, turnId, role: "user", contentBytes: 6, createdAtMs: 1, updatedAtMs: 1 },
      { id: assistantMessage, turnId, role: "assistant", contentBytes: 8, createdAtMs: 2, updatedAtMs: 2 },
    ]);
    api.getMessageContent = vi.fn((_endpoint, _token, _chatId, _turnId, messageId) =>
      Promise.resolve(messageId === userMessage ? "Prompt" : "Restored once"),
    );

    render(<App services={api} />);
    await user.click(await screen.findByRole("button", { name: "Open Selected chat" }));

    expect(await screen.findByRole("heading", { name: "Selected chat" })).toBeVisible();
    expect(await screen.findByText("Restored once")).toBeVisible();
    expect(api.listTurns).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "Open Selected chat" }));
    expect(api.listTurns).toHaveBeenCalledTimes(1);
    expect(api.listChats).toHaveBeenCalledTimes(1);
  });

  it("reopens the selected active conversation off-route while keeping other selection blocked", async () => {
    const user = userEvent.setup();
    const selectedChat = "0123456789abcdef0123456789abcdef";
    const otherChat = "1123456789abcdef0123456789abcdef";
    const conversations = [
      { id: selectedChat, title: "Active response", createdAtMs: 1, updatedAtMs: 3 },
      { id: otherChat, title: "Other history", createdAtMs: 1, updatedAtMs: 2 },
    ];
    const select = vi.fn();
    const loadMore = vi.fn();
    const retry = vi.fn();
    const onOpenChat = vi.fn();
    const history: ConversationHistoryController = {
      conversations,
      groupedConversations: [{ label: "Older", conversations }],
      selectedChatId: selectedChat,
      selection: { chatId: selectedChat, revision: 1 },
      state: "ready",
      errorMessage: "",
      query: "",
      hasMore: false,
      setQuery: vi.fn(),
      select,
      create: vi.fn(),
      rename: vi.fn(),
      delete: vi.fn(),
      loadMore,
      retry,
      adoptCreatedChat: vi.fn(),
      reconcileSummary: vi.fn(),
      clearAfterSettingsDelete: vi.fn(),
    };

    render(<GlobalConversationRail history={history} interactionLocked onOpenChat={onOpenChat} />);

    const blockedConversation = screen.getByRole("button", { name: "Open Other history" });
    expect(blockedConversation).toBeDisabled();
    expect(blockedConversation).toHaveAccessibleDescription("Unavailable while a response is active.");
    blockedConversation.focus();
    await user.keyboard("{Enter}");
    expect(onOpenChat).not.toHaveBeenCalled();
    expect(select).not.toHaveBeenCalled();

    const selectedConversation = screen.getByRole("button", { name: "Open Active response" });
    expect(selectedConversation).toBeEnabled();
    selectedConversation.focus();
    await user.keyboard("{Enter}");
    expect(onOpenChat).toHaveBeenCalledOnce();
    expect(select).not.toHaveBeenCalled();
    expect(loadMore).not.toHaveBeenCalled();
    expect(retry).not.toHaveBeenCalled();
  });

  it("navigates only after New chat succeeds and preserves the current route after failure", async () => {
    useWorkspaceStore.setState({ activeRoute: "models" });
    const user = userEvent.setup();
    const api = services();
    const created = {
      id: "0123456789abcdef0123456789abcdef",
      title: "Created truth",
      createdAtMs: 1,
      updatedAtMs: 2,
    };
    api.listChats = vi.fn().mockResolvedValue({ chats: [], nextBefore: null });
    api.createChat = vi.fn().mockResolvedValueOnce(created).mockRejectedValueOnce(new Error("backend rejected"));
    api.getChat = vi.fn();
    api.renameChat = vi.fn();
    api.deleteChat = vi.fn();

    render(<App services={api} />);
    await screen.findByText("No conversations yet.");
    await user.click(screen.getByRole("button", { name: "New chat" }));
    expect(await screen.findByRole("heading", { name: "Created truth" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Open Created truth" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "Models" }));
    await user.click(screen.getByRole("button", { name: "New chat" }));
    expect(await screen.findByText("Could not create a new conversation.")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Models" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Open Created truth" })).toHaveAttribute("aria-current", "page");
  });

  it("shows authoritative node health and active model on every route and links recovery to Node", async () => {
    const api = services();
    api.getStatus = vi.fn().mockResolvedValue({
      node_id: "loxa-node-77",
      health: "ready",
      model: "loxa",
      engine: { name: "llama.cpp", version: "b777" },
      runtime_model: "gemma-ready",
      profile: "default",
    });
    const user = userEvent.setup();
    render(<App services={api} />);

    const health = await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" });
    expect(health).toHaveAttribute("href", "#node");
    for (const route of ["Models", "Chat", "Settings"]) {
      await user.click(screen.getByRole("link", { name: route }));
      expect(screen.getByRole("link", { name: "Node ready. Active model gemma-ready" })).toBeVisible();
    }

    await user.click(screen.getByRole("link", { name: "Node ready. Active model gemma-ready" }));
    expect(screen.getByRole("heading", { name: "Nodes" })).toBeInTheDocument();
  });

  it("reports an authenticated unloaded node without implying model readiness", async () => {
    render(<App services={services()} />);

    expect(await screen.findByRole("link", { name: "Node online. No active model" })).toBeVisible();
  });

  it("settles global model truth from a terminal snapshot after the initiating route unmounts", async () => {
    useWorkspaceStore.setState({ activeRoute: "node" });
    const api = services();
    const controlCallbacks: ControlStreamCallbacks[] = [];
    api.getStatus = vi
      .fn()
      .mockResolvedValueOnce({
        node_id: "node-7",
        health: "unavailable",
        model: "loxa",
        engine: null,
        runtime_model: null,
        profile: null,
      })
      .mockResolvedValueOnce({
        node_id: "node-7",
        health: "ready",
        model: "loxa",
        engine: { name: "llama.cpp", version: "b777" },
        runtime_model: "gemma-ready",
        profile: "default",
      });
    api.getInventory = vi.fn().mockResolvedValue([
      {
        id: "gemma-ready",
        repo: "loxa/gemma",
        revision: "rev",
        filename: "gemma.gguf",
        sha256: "ab".repeat(32),
        sizeBytes: 1,
        license: "Apache-2.0",
        params: "4B",
        quant: "Q4",
        minFreeMemoryGiB: 1,
        artifact: { kind: "downloaded" },
        compatibility: { compatible: true, reason: "Compatible" },
        engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
      },
    ]);
    api.getControlNode = vi
      .fn()
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockResolvedValueOnce({ status: "loading", activeModelId: null, operationId: v2Ids.operation, error: null })
      .mockResolvedValueOnce({ status: "ready", activeModelId: "gemma-ready", operationId: null, error: null });
    api.loadModel = vi.fn().mockResolvedValue({ operationId: "op-load" });
    api.getOperation = vi.fn().mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "queued",
      modelId: "gemma-ready",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    api.createControlEventStream = vi.fn((_endpoint, _token, _cursor, callbacks) => {
      controlCallbacks.push(callbacks);
      return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<ControlStreamTerminal>(() => undefined) };
    });
    const user = userEvent.setup();
    render(<App services={api} />);

    expect(await screen.findByRole("link", { name: "Node online. No active model" })).toBeVisible();
    await user.click(screen.getByRole("link", { name: "Models" }));
    await user.click(await screen.findByRole("button", { name: "Load gemma-ready" }));
    expect(screen.getByRole("link", { name: "Updating node. Model status unavailable" })).toBeVisible();

    await user.click(screen.getByRole("link", { name: "Node" }));
    const initialStreamCount = controlCallbacks.length;
    act(() =>
      controlCallbacks.forEach((callbacks) =>
        callbacks.onTerminal({
          kind: "error",
          cursor: 1,
          message: "Live model updates disconnected.",
        }),
      ),
    );
    await waitFor(() => expect(controlCallbacks.length).toBeGreaterThan(initialStreamCount));

    act(() =>
      controlCallbacks.slice(initialStreamCount).forEach((callbacks) =>
        callbacks.onSnapshot({
          cursor: 1,
          cursorGap: true,
          operations: [
            {
              id: v2Ids.operation,
              kind: "load",
              status: "succeeded",
              modelId: "gemma-ready",
              progress: null,
              error: null,
              createdAtUnixMs: 1,
              updatedAtUnixMs: 3,
            },
          ],
          events: [],
        }),
      ),
    );

    expect(await screen.findByRole("link", { name: "Node online. No active model" })).toBeVisible();
  });

  it("refreshes the shared node session after Chat loads a model without requiring navigation", async () => {
    const api = services();
    const controlCallbacks: ControlStreamCallbacks[] = [];
    api.getStatus = vi
      .fn()
      .mockResolvedValueOnce({
        node_id: "node-7",
        health: "unavailable",
        model: "loxa",
        engine: null,
        runtime_model: null,
        profile: null,
      })
      .mockResolvedValue({
        node_id: "node-7",
        health: "ready",
        model: "loxa",
        engine: { name: "llama.cpp", version: "b777" },
        runtime_model: "gemma-ready",
        profile: "default",
      });
    api.getInventory = vi.fn().mockResolvedValue([
      {
        id: "gemma-ready",
        repo: "loxa/gemma",
        revision: "rev",
        filename: "gemma.gguf",
        sha256: "ab".repeat(32),
        sizeBytes: 1,
        license: "Apache-2.0",
        params: "4B",
        quant: "Q4",
        minFreeMemoryGiB: 1,
        artifact: { kind: "downloaded" },
        compatibility: { compatible: true, reason: "Compatible" },
        engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
      },
    ]);
    api.getControlNode = vi
      .fn()
      .mockResolvedValueOnce({ status: "unloaded", activeModelId: null, operationId: null, error: null })
      .mockResolvedValue({ status: "ready", activeModelId: "gemma-ready", operationId: null, error: null });
    api.loadModel = vi.fn().mockResolvedValue({ operationId: "op-load" });
    api.getOperation = vi.fn().mockResolvedValue({
      id: "op-load",
      kind: "load",
      status: "succeeded",
      modelId: "gemma-ready",
      progress: null,
      error: null,
      createdAtUnixMs: 1,
      updatedAtUnixMs: 2,
    });
    api.createControlEventStream = vi.fn((_endpoint, _token, _cursor, callbacks) => {
      controlCallbacks.push(callbacks);
      return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<ControlStreamTerminal>(() => undefined) };
    });
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(await screen.findByRole("link", { name: "Chat" }));
    await chooseChatModel(user, "gemma-ready", "Load");
    act(() =>
      controlCallbacks.forEach((callbacks) =>
        callbacks.onEvent({
          sequence: 3,
          operation: {
            id: "op-load",
            kind: "load",
            status: "succeeded",
            modelId: "gemma-ready",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
        }),
      ),
    );

    await waitFor(() => expect(api.getStatus).toHaveBeenCalledTimes(3));
    expect(await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" })).toBeVisible();
    await waitFor(() => expect(screen.getByLabelText("Message")).toBeEnabled());
  });

  it("joins a Chat completion and terminal event into one authoritative status proof", async () => {
    const api = services();
    const controlCallbacks: ControlStreamCallbacks[] = [];
    const settlementSignals: AbortSignal[] = [];
    let statusCalls = 0;
    let resolveStatus!: (status: NodeStatus) => void;
    let resolveOperation!: (operation: Awaited<ReturnType<AppServices["getOperation"]>>) => void;
    api.getStatus = vi.fn((_endpoint, options) => {
      statusCalls += 1;
      if (statusCalls === 1)
        return Promise.resolve({
          node_id: "node-7",
          health: "unavailable",
          model: "loxa",
          engine: null,
          runtime_model: null,
          profile: null,
        } satisfies NodeStatus);
      if (statusCalls === 2 || statusCalls > 3)
        return Promise.resolve({
          node_id: "node-7",
          health: "ready",
          model: "loxa",
          engine: { name: "llama.cpp", version: "b777" },
          runtime_model: statusCalls === 2 ? "old-model" : "gemma-ready",
          profile: "default",
        } satisfies NodeStatus);
      if (options?.signal) settlementSignals.push(options.signal);
      return new Promise<NodeStatus>((resolve) => {
        resolveStatus = resolve;
      });
    });
    api.getInventory = vi.fn().mockResolvedValue([
      {
        id: "gemma-ready",
        repo: "loxa/gemma",
        revision: "rev",
        filename: "gemma.gguf",
        sha256: "ab".repeat(32),
        sizeBytes: 1,
        license: "Apache-2.0",
        params: "4B",
        quant: "Q4",
        minFreeMemoryGiB: 1,
        artifact: { kind: "downloaded" },
        compatibility: { compatible: true, reason: "Compatible" },
        engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
      },
    ]);
    api.getControlNode = vi
      .fn()
      .mockResolvedValueOnce({ status: "ready", activeModelId: "old-model", operationId: null, error: null })
      .mockResolvedValue({ status: "ready", activeModelId: "gemma-ready", operationId: null, error: null });
    api.loadModel = vi.fn().mockResolvedValue({ operationId: "op-load" });
    api.getOperation = vi.fn(
      () =>
        new Promise<Awaited<ReturnType<AppServices["getOperation"]>>>((resolve) => {
          resolveOperation = resolve;
        }),
    );
    api.createControlEventStream = vi.fn((_endpoint, _token, _cursor, callbacks) => {
      controlCallbacks.push(callbacks);
      return { cancel: vi.fn(), dispose: vi.fn(), finished: new Promise<ControlStreamTerminal>(() => undefined) };
    });
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(await screen.findByRole("link", { name: "Chat" }));
    await chooseChatModel(user, "gemma-ready", "Switch to");
    await waitFor(() => expect(api.getOperation).toHaveBeenCalledTimes(1));

    act(() =>
      controlCallbacks.forEach((callbacks) =>
        callbacks.onEvent({
          sequence: 7,
          operation: {
            id: "op-load",
            kind: "load",
            status: "succeeded",
            modelId: "gemma-ready",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
        }),
      ),
    );
    act(() =>
      resolveOperation({
        id: "op-load",
        kind: "load",
        status: "succeeded",
        modelId: "gemma-ready",
        progress: null,
        error: null,
        createdAtUnixMs: 1,
        updatedAtUnixMs: 2,
      }),
    );

    await waitFor(() => expect(api.getStatus).toHaveBeenCalledTimes(3));
    expect(settlementSignals).toHaveLength(1);
    expect(settlementSignals[0]?.aborted).toBe(false);

    act(() =>
      resolveStatus({
        node_id: "node-7",
        health: "ready",
        model: "loxa",
        engine: { name: "llama.cpp", version: "b777" },
        runtime_model: "gemma-ready",
        profile: "default",
      }),
    );
    expect(await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" })).toBeVisible();
    await waitFor(() => expect(screen.getByLabelText("Message")).toBeEnabled());
    expect(settlementSignals[0]?.aborted).toBe(false);
  });

  it("fails closed when the control token is unavailable and recovers through retry", async () => {
    useWorkspaceStore.setState({ activeRoute: "node" });
    const api = services();
    api.getStatus = vi
      .fn()
      .mockResolvedValueOnce({
        node_id: "node-7",
        health: "unavailable",
        model: "loxa",
        engine: null,
        runtime_model: null,
        profile: null,
      })
      .mockResolvedValue({
        node_id: "node-7",
        health: "ready",
        model: "loxa",
        engine: { name: "llama.cpp", version: "b777" },
        runtime_model: "gemma-ready",
        profile: "default",
      });
    api.readControlToken = vi
      .fn()
      .mockRejectedValueOnce(new Error("global stream token unavailable"))
      .mockResolvedValue("ab".repeat(32));
    const user = userEvent.setup();
    render(<App services={api} />);

    const retry = await screen.findByRole("button", { name: "Retry node startup" });
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalledTimes(1));
    expect(api.createControlEventStream).not.toHaveBeenCalled();
    await user.click(retry);

    expect(await screen.findByRole("link", { name: "Node online. No active model" })).toBeVisible();
    expect(api.readControlToken).toHaveBeenCalledTimes(2);
    expect(api.createControlEventStream).toHaveBeenCalled();
  });

  it("reconnects the session stream and settles after the initiating route unmounts", async () => {
    useWorkspaceStore.setState({ activeRoute: "node" });
    const api = services();
    const handles: Array<{ callbacks: ControlStreamCallbacks; dispose: ReturnType<typeof vi.fn> }> = [];
    const proofSignals: AbortSignal[] = [];
    api.getStatus = vi
      .fn()
      .mockResolvedValueOnce({
        node_id: "node-7",
        health: "unavailable",
        model: "loxa",
        engine: null,
        runtime_model: null,
        profile: null,
      })
      .mockImplementation((_endpoint, options) => {
        if (options?.signal) proofSignals.push(options.signal);
        return Promise.resolve({
          node_id: "node-7",
          health: "ready",
          model: "loxa",
          engine: { name: "llama.cpp", version: "b777" },
          runtime_model: "gemma-ready",
          profile: "default",
        });
      });
    api.getInventory = vi.fn().mockResolvedValue([
      {
        id: "gemma-ready",
        repo: "loxa/gemma",
        revision: "rev",
        filename: "gemma.gguf",
        sha256: "ab".repeat(32),
        sizeBytes: 1,
        license: "Apache-2.0",
        params: "4B",
        quant: "Q4",
        minFreeMemoryGiB: 1,
        artifact: { kind: "downloaded" },
        compatibility: { compatible: true, reason: "Compatible" },
        engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
      },
    ]);
    api.getControlNode = vi
      .fn()
      .mockResolvedValue({ status: "unloaded", activeModelId: null, operationId: null, error: null });
    api.loadModel = vi.fn().mockResolvedValue({ operationId: v2Ids.operation });
    api.getOperation = vi.fn(() => new Promise<never>(() => undefined));
    api.createControlEventStream = vi
      .fn()
      .mockImplementationOnce(() => {
        throw new Error("initial global stream unavailable");
      })
      .mockImplementation((_endpoint, _token, _cursor, callbacks) => {
        const dispose = vi.fn();
        handles.push({ callbacks, dispose });
        return { cancel: vi.fn(), dispose, finished: new Promise<ControlStreamTerminal>(() => undefined) };
      });
    const user = userEvent.setup();
    render(<App services={api} />);

    expect(await screen.findByRole("link", { name: "Node online. No active model" })).toBeVisible();
    await waitFor(() => expect(api.createControlEventStream).toHaveBeenCalled());
    await user.click(screen.getByRole("link", { name: "Models" }));
    await user.click(await screen.findByRole("button", { name: "Load gemma-ready" }));
    expect(screen.getByRole("link", { name: "Updating node. Model status unavailable" })).toBeVisible();
    await user.click(screen.getByRole("link", { name: "Node" }));

    await waitFor(() => expect(handles.some((handle) => !handle.dispose.mock.calls.length)).toBe(true));
    const sessionHandle = handles.find((handle) => !handle.dispose.mock.calls.length);
    expect(sessionHandle).toBeDefined();
    act(() =>
      sessionHandle?.callbacks.onSnapshot({
        cursor: 10,
        cursorGap: true,
        operations: [
          {
            id: "op-old-1",
            kind: "load",
            status: "succeeded",
            modelId: "old",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
          {
            id: v2Ids.operation,
            kind: "load",
            status: "succeeded",
            modelId: "gemma-ready",
            progress: null,
            error: null,
            createdAtUnixMs: 1,
            updatedAtUnixMs: 3,
          },
          {
            id: "op-old-2",
            kind: "unload",
            status: "failed",
            modelId: null,
            progress: null,
            error: "old",
            createdAtUnixMs: 1,
            updatedAtUnixMs: 2,
          },
        ],
        events: [],
      }),
    );

    expect(await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" })).toBeVisible();
    expect(api.getStatus).toHaveBeenCalledTimes(2);
    expect(proofSignals).toHaveLength(1);
    expect(proofSignals[0]?.aborted).toBe(false);
  });

  it("does not claim that no model is active before the node session is proven", async () => {
    const api = services();
    api.bootstrap.start = vi.fn(() => new Promise<BootstrapSnapshot>(() => undefined));
    render(<App services={api} />);

    expect(await screen.findByRole("link", { name: "Starting node. Model status unavailable" })).toBeVisible();
    expect(screen.queryByRole("link", { name: /Starting node\. No active model/ })).not.toBeInTheDocument();
  });

  it("keeps every route inside the same shell-owned page frame", async () => {
    const user = userEvent.setup();
    const { container } = render(<App services={services()} />);
    await screen.findByRole("link", { name: "Node online. No active model" });

    const canvas = container.querySelector(".workspace-canvas");
    const frame = container.querySelector(".workspace-frame");
    expect(canvas).toContainElement(frame as HTMLElement);
    expect(frame).toContainElement(screen.getByRole("heading", { name: "New Chat" }));

    for (const route of ["Models", "Node", "Settings"] as const) {
      await user.click(screen.getByRole("link", { name: route }));
      const heading = route === "Node" ? "Nodes" : route;
      expect(frame).toContainElement(screen.getByRole("heading", { name: heading }));
    }
  });

  it("keeps the complete Chat surface visible and inert while node identity is unproven", async () => {
    const api = services();
    api.bootstrap.start = vi.fn(() => new Promise<BootstrapSnapshot>(() => undefined));
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(screen.getByRole("link", { name: "Chat" }));

    expect(screen.getByRole("log", { name: "Conversation" })).toBeVisible();
    expect(screen.getByRole("form", { name: "Message composer" })).toBeVisible();
    expect(screen.getByLabelText("Message")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Choose model" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Attach document" })).toHaveAttribute("aria-disabled", "true");
    expect(
      screen.getByRole("tooltip", {
        name: /document input support cannot be checked until the node is connected/i,
      }),
    ).toBeInTheDocument();
    expect(api.getStatus).not.toHaveBeenCalled();
    expect(api.readControlToken).not.toHaveBeenCalled();
  });

  it("has a logical keyboard focus order and no unsupported controls", async () => {
    const user = userEvent.setup();
    render(<App services={services()} />);
    await screen.findByRole("link", { name: "Node online. No active model" });

    await user.tab();
    expect(screen.getByRole("link", { name: "Chat" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Models" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Node" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Node online. No active model" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("link", { name: "Settings" })).toHaveFocus();
    expect(screen.queryByRole("button", { name: /load|unload|switch|download/i })).not.toBeInTheDocument();
  });

  it("gates route clients until native bootstrap and the v2 peer proof succeed", async () => {
    const api = services();
    let resolveStart!: (snapshot: BootstrapSnapshot) => void;
    let resolvePeer!: (peer: typeof testPeer) => void;
    api.bootstrap.start = vi.fn(
      () =>
        new Promise<BootstrapSnapshot>((resolve) => {
          resolveStart = resolve;
        }),
    );
    api.proveV2ControlPeer = vi.fn(
      () =>
        new Promise<typeof testPeer>((resolve) => {
          resolvePeer = resolve;
        }),
    );
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(screen.getByRole("link", { name: "Models" }));
    expect(screen.getByRole("heading", { name: "Models" })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Starting the private Loxa node");
    expect(api.readControlToken).not.toHaveBeenCalled();

    await act(async () => {
      resolveStart({
        ownership: "owned",
        endpoint: "http://127.0.0.1:8080",
        childRunning: true,
        error: null,
      });
    });

    await waitFor(() => expect(api.readControlToken).toHaveBeenCalledOnce());
    expect(api.openV2Events).not.toHaveBeenCalled();

    await act(async () => {
      resolvePeer(testPeer);
    });

    await waitFor(() => expect(api.openV2Events).toHaveBeenCalledOnce());
  });

  it("closes route clients while an owned node is stopping and until retry proves it again", async () => {
    const api = services();
    let resolveStop!: (snapshot: BootstrapSnapshot) => void;
    api.bootstrap.stop = vi.fn(
      () =>
        new Promise<BootstrapSnapshot>((resolve) => {
          resolveStop = resolve;
        }),
    );
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(screen.getByRole("link", { name: "Node" }));
    const stop = await screen.findByRole("button", { name: "Stop node" });
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalled());
    vi.mocked(api.readControlToken).mockClear();
    await user.click(stop);
    await user.click(screen.getByRole("link", { name: "Models" }));
    expect(screen.getByRole("status")).toHaveTextContent(/stopping|node/i);
    expect(api.readControlToken).not.toHaveBeenCalled();

    await act(async () => {
      resolveStop({
        ownership: "none",
        endpoint: "http://127.0.0.1:8080",
        childRunning: false,
        error: null,
      });
    });
    expect(await screen.findByRole("button", { name: "Retry node startup" })).toBeEnabled();
    expect(api.readControlToken).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Retry node startup" }));
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalled());
  });

  it("keeps nested Settings navigation on the shared session and resets it from the sidebar", async () => {
    useWorkspaceStore.setState({ activeRoute: "node" });
    const api = services();
    api.getStatus = vi.fn().mockResolvedValue({
      node_id: "loxa-node-77",
      health: "ready",
      model: "loxa",
      engine: { name: "llama.cpp", version: "b777" },
      runtime_model: "gemma-ready",
      profile: "default",
    });
    const user = userEvent.setup();
    render(<App services={api} />);
    await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" });
    await user.click(screen.getByRole("link", { name: "Settings" }));
    expect(screen.getByRole("heading", { name: "Settings", level: 1 })).toBeVisible();
    expect(screen.queryByText(v2Ids.node)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Runtime/ }));
    expect(screen.getByRole("heading", { name: "Runtime", level: 1 })).toHaveFocus();
    expect(screen.getByRole("region", { name: "Local node/runtime" })).toHaveTextContent(v2Ids.node);
    expect(screen.getByText("gemma-ready")).toBeInTheDocument();

    await user.click(screen.getByRole("link", { name: "Node" }));
    await user.click(screen.getByRole("link", { name: "Settings" }));
    expect(screen.getByRole("heading", { name: "Settings", level: 1 })).toBeVisible();
    expect(screen.getByRole("button", { name: /Runtime/ })).toBeVisible();
    expect(screen.queryByText(v2Ids.node)).not.toBeInTheDocument();
    expect(api.bootstrap.start).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalledTimes(1));
    expect(api.createControlEventStream).toHaveBeenCalledTimes(1);
  });

  it("keeps v2 model truth settled when the terminal snapshot precedes the mutation response", async () => {
    useWorkspaceStore.setState({ activeRoute: "models" });
    const api = services();
    api.getInventory = vi.fn().mockResolvedValue([modelFixture()]);
    const accepted = decodeV2OperationAccepted(validV2OperationAccepted);
    let resolveLoad!: (value: typeof accepted) => void;
    api.loadV2Slot = vi.fn(
      () =>
        new Promise<typeof accepted>((resolve) => {
          resolveLoad = resolve;
        }),
    );
    const user = userEvent.setup();
    render(<App services={api} />);

    const load = await screen.findByRole("button", { name: "Load gemma-3-4b-it-q4" });
    await user.click(load);
    await waitFor(() => expect(api.loadV2Slot).toHaveBeenCalledOnce());
    const callbacks = vi.mocked(api.openV2Events!).mock.calls[0]?.[2];
    expect(callbacks).toBeDefined();
    act(() =>
      callbacks?.onSnapshot(
        controlSnapshot({
          revision: "12",
          cursor: "12",
          operations: [{ ...validV2Operation, status: "succeeded", updated_revision: "12" }],
        }),
      ),
    );
    await act(async () => resolveLoad(accepted));

    await waitFor(() => expect(load).toBeEnabled());
    expect(screen.getByRole("link", { name: "Node online. No active model" })).toBeVisible();
  });

  it("keeps the sidebar as the only live-region owner on Runtime during a session transition", async () => {
    const api = services();
    api.bootstrap.start = vi.fn(() => new Promise<BootstrapSnapshot>(() => undefined));
    const user = userEvent.setup();
    render(<App services={api} />);

    await user.click(screen.getByRole("link", { name: "Settings" }));
    await user.click(screen.getByRole("button", { name: /Runtime/ }));

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).getByText("Starting", { selector: '[data-slot="status-badge"]' })).toBeVisible();
    const liveRegions = document.querySelectorAll('[aria-live="polite"]');
    expect(liveRegions).toHaveLength(1);
    expect(liveRegions[0]).toBe(screen.getByRole("link", { name: "Starting node. Model status unavailable" }));
  });
});
