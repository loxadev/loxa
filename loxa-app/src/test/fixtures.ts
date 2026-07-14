import type { AppServices } from "@/app/App";
import type { PersistentTurnTerminal } from "@/chat/historyClient";
import type { StreamTerminal } from "@/chat/streamChat";
import type { ControlStreamTerminal } from "@/control/events";

const ENDPOINT = "http://127.0.0.1:8080";
const TOKEN = "ab".repeat(32);
const CHAT_ID = "01".repeat(16);

const pending = <T>() => new Promise<T>(() => undefined);

export function createAppServicesFixture(overrides: Partial<AppServices> = {}): AppServices {
  const chat = { id: CHAT_ID, title: "New chat", createdAtMs: 1_700_000_000_000, updatedAtMs: 1_700_000_000_000 };
  const services: AppServices = {
    bootstrap: {
      snapshot: async () => ({ ownership: "owned", endpoint: ENDPOINT, childRunning: true, error: null }),
      start: async () => ({ ownership: "owned", endpoint: ENDPOINT, childRunning: true, error: null }),
      attach: async (endpoint) => ({ ownership: "attached", endpoint, childRunning: false, error: null }),
      stop: async () => ({ ownership: "none", endpoint: ENDPOINT, childRunning: false, error: null }),
    },
    getStatus: async () => ({
      node_id: "loxa-browser-fixture",
      health: "unavailable",
      model: "loxa",
      engine: null,
      runtime_model: null,
      profile: null,
    }),
    getModels: async () => ({ object: "list", data: [{ id: "loxa", object: "model", owned_by: "loxa" }] }),
    getCapabilities: async () => ({
      documentInput: false,
      documentInputReason: "Document input is unavailable in the browser fixture.",
      textChat: true,
    }),
    readControlToken: async () => TOKEN,
    getControlNode: async () => ({ status: "unloaded", activeModelId: null, operationId: null, error: null }),
    getInventory: async () => [],
    downloadModel: async () => ({ operationId: "browser-download" }),
    loadModel: async () => ({ operationId: "browser-load" }),
    unloadModel: async () => ({ operationId: "browser-unload" }),
    getOperation: async () => ({
      id: "browser-operation",
      kind: "load",
      status: "queued",
      modelId: "browser-model",
      progress: null,
      error: null,
      createdAtUnixMs: 1_700_000_000_000,
      updatedAtUnixMs: 1_700_000_000_000,
    }),
    cancelOperation: async () => ({
      id: "browser-operation",
      kind: "load",
      status: "cancelled",
      modelId: "browser-model",
      progress: null,
      error: null,
      createdAtUnixMs: 1_700_000_000_000,
      updatedAtUnixMs: 1_700_000_000_001,
    }),
    createControlEventStream: () => ({
      cancel: () => undefined,
      dispose: () => undefined,
      finished: pending<ControlStreamTerminal>(),
    }),
    createChatStream: () => ({
      cancel: () => undefined,
      dispose: () => undefined,
      finished: pending<StreamTerminal>(),
    }),
    listChats: async () => ({ chats: [], nextBefore: null }),
    createChat: async () => chat,
    getChat: async () => chat,
    listTurns: async () => ({ turns: [], nextAfter: null }),
    listMessageSummaries: async () => [],
    getMessageContent: async () => "",
    renameChat: async (_endpoint, _token, _chatId, title) => ({ ...chat, title }),
    deleteChat: async () => undefined,
    clearChats: async () => ({ deleted: 0 }),
    createPersistentTurn: () => ({
      cancel: () => undefined,
      dispose: () => undefined,
      finished: pending<PersistentTurnTerminal>(),
    }),
    copyText: async () => undefined,
  };

  return { ...services, ...overrides };
}
