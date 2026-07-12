import { invoke } from "@tauri-apps/api/core";

import { streamChat } from "../chat/streamChat";
import {
  cancelOperation,
  downloadModel,
  getCapabilities,
  getControlNode,
  getInventory,
  getOperation,
  loadModel,
  unloadModel,
} from "../control/client";
import { streamControlEvents } from "../control/events";
import { getModels, getStatus } from "../node/client";
import type { BootstrapSnapshot, StartNodeRequest } from "../node/NodeScreen";
import type { AppServices } from "./App";

export const DEFAULT_ENDPOINT = "http://127.0.0.1:8080";

export const appServices: AppServices = {
  bootstrap: {
    snapshot: () => invoke<BootstrapSnapshot>("bootstrap_snapshot"),
    start: (request: StartNodeRequest) =>
      invoke<BootstrapSnapshot>("start_node", { request }),
    attach: (endpoint: string) =>
      invoke<BootstrapSnapshot>("attach_node", { endpoint }),
    stop: () => invoke<BootstrapSnapshot>("stop_owned_node"),
  },
  getStatus,
  getModels,
  getCapabilities,
  readControlToken: (endpoint: string) => invoke<string>("read_control_token", { endpoint }),
  getControlNode,
  getInventory,
  downloadModel,
  loadModel,
  unloadModel,
  getOperation,
  cancelOperation,
  createControlEventStream: streamControlEvents,
  createChatStream: streamChat,
  copyText: (text: string) => navigator.clipboard.writeText(text),
};
