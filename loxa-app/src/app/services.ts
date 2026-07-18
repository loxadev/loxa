import { invoke } from "@tauri-apps/api/core";

import { streamChat } from "../chat/streamChat";
import {
  clearChats,
  createChat,
  deleteChat,
  getChat,
  getMessageContent,
  listChats,
  listMessageSummaries,
  listTurns,
  renameChat,
  streamPersistentTurn,
} from "../chat/historyClient";
import {
  cancelV2Operation,
  downloadV2Model,
  getCapabilities,
  getControlNode,
  getInventory,
  getOperation,
  loadModel,
  loadV2Slot,
  proveV2ControlPeer,
  unloadV2Slot,
} from "../control/client";
import { openV2Events, streamControlEvents } from "../control/events";
import { getModels, getStatus } from "../node/client";
import type { BootstrapSnapshot, StartNodeRequest } from "../node/NodeSession";
import type { AppServices } from "./App";

export const DEFAULT_ENDPOINT = "http://127.0.0.1:8080";
export function desktopRuntimeUnavailableMessage(development: boolean) {
  return development ? "Desktop runtime is unavailable in browser preview." : "Desktop runtime is unavailable.";
}

export const DESKTOP_RUNTIME_UNAVAILABLE_MESSAGE = desktopRuntimeUnavailableMessage(import.meta.env.DEV);

function invokeDesktop<T>(command: string, args?: Record<string, unknown>) {
  if (!("__TAURI_INTERNALS__" in window)) {
    return Promise.reject(new Error(DESKTOP_RUNTIME_UNAVAILABLE_MESSAGE));
  }
  return invoke<T>(command, args);
}

export const appServices: AppServices = {
  bootstrap: {
    snapshot: () => invokeDesktop<BootstrapSnapshot>("bootstrap_snapshot"),
    start: (request: StartNodeRequest) => invokeDesktop<BootstrapSnapshot>("start_node", { request }),
    attach: (endpoint: string) => invokeDesktop<BootstrapSnapshot>("attach_node", { endpoint }),
    stop: () => invokeDesktop<BootstrapSnapshot>("stop_owned_node"),
  },
  getStatus,
  getModels,
  getCapabilities,
  readControlToken: (endpoint: string) => invokeDesktop<string>("read_control_token", { endpoint }),
  getControlNode,
  getInventory,
  proveV2ControlPeer,
  openV2Events,
  downloadV2Model,
  loadV2Slot,
  unloadV2Slot,
  cancelV2Operation,
  loadModel,
  getOperation,
  createControlEventStream: streamControlEvents,
  createChatStream: streamChat,
  listChats,
  createChat,
  getChat,
  listTurns,
  listMessageSummaries,
  getMessageContent,
  renameChat,
  deleteChat,
  createPersistentTurn: streamPersistentTurn,
  clearChats,
  copyText: (text: string) => navigator.clipboard.writeText(text),
};
