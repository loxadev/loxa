import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { App, GlobalConversationRail, type AppServices } from "./App";
import type { ConversationHistoryController } from "../chat/conversationHistory";
import type { ControlStreamCallbacks, ControlStreamTerminal } from "../control/events";
import type { BootstrapSnapshot } from "../node/NodeSession";
import type { NodeStatus } from "../node/contracts";
import { useWorkspaceStore } from "../stores/workspace-store";

function services(): AppServices {
  return {
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
    downloadModel: vi.fn(),
    loadModel: vi.fn(),
    unloadModel: vi.fn(),
    getOperation: vi.fn(),
    cancelOperation: vi.fn(),
    createControlEventStream: vi.fn(() => ({
      cancel: vi.fn(),
      dispose: vi.fn(),
      finished: new Promise<ControlStreamTerminal>(() => undefined),
    })),
    createChatStream: vi.fn(),
    copyText: vi.fn(),
  };
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

    expect(await screen.findByRole("heading", { name: "Chat" })).toBeInTheDocument();
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
    await user.click(await screen.findByRole("button", { name: "Browse models" }));

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

    const separator = screen.getByRole("separator", { name: "Resize navigation and conversation rail" });
    const shell = screen.getByTestId("app-shell");
    expect(shell.style.getPropertyValue("--loxa-sidebar-width")).toBe("280px");
    expect(separator).toHaveAttribute("aria-valuemin", "220");
    expect(separator).toHaveAttribute("aria-valuemax", "420");
    fireEvent.keyDown(separator, { key: "End" });
    expect(separator).toHaveAttribute("aria-valuenow", "420");
    fireEvent.keyDown(separator, { key: "ArrowRight" });
    expect(separator).toHaveAttribute("aria-valuenow", "420");
    fireEvent.keyDown(separator, { key: "Home" });
    expect(separator).toHaveAttribute("aria-valuenow", "220");
    fireEvent.keyDown(separator, { key: "ArrowLeft" });
    expect(separator).toHaveAttribute("aria-valuenow", "220");
    fireEvent.doubleClick(separator);
    expect(separator).toHaveAttribute("aria-valuenow", "280");
    const setPointerCapture = vi.fn();
    const releasePointerCapture = vi.fn();
    Object.assign(separator, { setPointerCapture, releasePointerCapture, hasPointerCapture: () => true });
    fireEvent.pointerDown(separator, { pointerId: 7, button: 0, clientX: 220 });
    expect(setPointerCapture).toHaveBeenCalledWith(7);
    fireEvent.pointerMove(window, { pointerId: 8, clientX: 700 });
    expect(separator).toHaveAttribute("aria-valuenow", "280");
    fireEvent.pointerMove(window, { pointerId: 7, clientX: 700 });
    expect(separator).toHaveAttribute("aria-valuenow", "420");
    fireEvent.pointerMove(window, { pointerId: 7, clientX: -200 });
    expect(separator).toHaveAttribute("aria-valuenow", "220");
    fireEvent.pointerCancel(window, { pointerId: 7 });
    expect(releasePointerCapture).toHaveBeenCalledWith(7);

    await user.click(screen.getByRole("button", { name: "Collapse sidebar" }));
    expect(shell.style.getPropertyValue("--loxa-sidebar-width")).toBe("56px");
    expect(
      screen.queryByRole("separator", { name: "Resize navigation and conversation rail" }),
    ).not.toBeInTheDocument();
    for (const name of ["Expand sidebar", "Chat", "Models", "Node", "Settings", "Node online. No active model"]) {
      expect(screen.getByRole(name === "Expand sidebar" ? "button" : "link", { name })).toBeVisible();
    }
    expect(document.querySelectorAll(".app-sidebar svg:not([aria-hidden='true'])")).toHaveLength(0);

    view.unmount();
    render(<App services={api} />);
    expect(screen.getByTestId("app-shell").style.getPropertyValue("--loxa-sidebar-width")).toBe("56px");
    await user.click(screen.getByRole("button", { name: "Expand sidebar" }));
    expect(screen.getByTestId("app-shell").style.getPropertyValue("--loxa-sidebar-width")).toBe("220px");

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

    expect(await screen.findByRole("heading", { name: "Chat" })).toBeVisible();
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
    expect(await screen.findByRole("heading", { name: "Chat" })).toBeVisible();
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
      .mockResolvedValueOnce({ status: "loading", activeModelId: null, operationId: "op-load", error: null })
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
              id: "op-load",
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

    expect(await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" })).toBeVisible();
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
    await user.click(await screen.findByRole("button", { name: "Load gemma-ready" }));
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
    expect(screen.getByLabelText("Message")).toBeEnabled();
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
    await user.selectOptions(await screen.findByRole("combobox", { name: "Choose model" }), "gemma-ready");
    await user.click(screen.getByRole("button", { name: "Switch to gemma-ready" }));
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
    await waitFor(() => expect(screen.getByLabelText("Message")).toBeDisabled());
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

  it("settles from the local Chat terminal when the global stream initially cannot connect", async () => {
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
    const user = userEvent.setup();
    render(<App services={api} />);

    expect(await screen.findByRole("link", { name: "Node online. No active model" })).toBeVisible();
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalledTimes(1));
    expect(api.createControlEventStream).not.toHaveBeenCalled();
    await user.click(screen.getByRole("link", { name: "Chat" }));
    await user.click(await screen.findByRole("button", { name: "Load gemma-ready" }));

    expect(await screen.findByRole("link", { name: "Node ready. Active model gemma-ready" })).toBeVisible();
    await waitFor(() => expect(screen.getByLabelText("Message")).toBeEnabled());
    expect(api.getStatus).toHaveBeenCalledTimes(3);
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
    api.loadModel = vi.fn().mockResolvedValue({ operationId: "op-load" });
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
    await waitFor(() => expect(api.createControlEventStream).toHaveBeenCalledTimes(1));
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
            id: "op-load",
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
    expect(frame).toContainElement(screen.getByRole("heading", { name: "Chat" }));

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
    expect(screen.getByRole("combobox", { name: "Choose model" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Attach document" })).toBeDisabled();
    expect(screen.getByText(/document input support cannot be checked until the node is connected/i)).toBeVisible();
    expect(api.getStatus).not.toHaveBeenCalled();
    expect(api.readControlToken).not.toHaveBeenCalled();
  });

  it("has a logical keyboard focus order and no unsupported controls", async () => {
    const user = userEvent.setup();
    render(<App services={services()} />);
    await screen.findByRole("link", { name: "Node online. No active model" });

    await user.tab();
    expect(screen.getByRole("button", { name: "Collapse sidebar" })).toHaveFocus();
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

  it("gates route clients until native bootstrap and the public status probe succeed", async () => {
    const api = services();
    let resolveStart!: (snapshot: BootstrapSnapshot) => void;
    let resolveStatus!: (status: NodeStatus) => void;
    api.bootstrap.start = vi.fn(
      () =>
        new Promise<BootstrapSnapshot>((resolve) => {
          resolveStart = resolve;
        }),
    );
    api.getStatus = vi.fn(
      () =>
        new Promise<NodeStatus>((resolve) => {
          resolveStatus = resolve;
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

    expect(api.readControlToken).not.toHaveBeenCalled();

    await act(async () => {
      resolveStatus({
        node_id: "node-7",
        health: "unavailable",
        model: "loxa",
        engine: null,
        runtime_model: null,
        profile: null,
      });
    });

    await waitFor(() => expect(api.readControlToken).toHaveBeenCalled());
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
    expect(screen.queryByText("loxa-node-77")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Runtime/ }));
    expect(screen.getByRole("heading", { name: "Runtime", level: 1 })).toHaveFocus();
    expect(screen.getByRole("region", { name: "Local node/runtime" })).toHaveTextContent("loxa-node-77");
    expect(screen.getByText("gemma-ready")).toBeInTheDocument();

    await user.click(screen.getByRole("link", { name: "Node" }));
    await user.click(screen.getByRole("link", { name: "Settings" }));
    expect(screen.getByRole("heading", { name: "Settings", level: 1 })).toBeVisible();
    expect(screen.getByRole("button", { name: /Runtime/ })).toBeVisible();
    expect(screen.queryByText("loxa-node-77")).not.toBeInTheDocument();
    expect(api.bootstrap.start).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(api.readControlToken).toHaveBeenCalledTimes(1));
    expect(api.createControlEventStream).toHaveBeenCalledTimes(1);
  });
});
