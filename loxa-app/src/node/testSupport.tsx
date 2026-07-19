/* eslint-disable react-refresh/only-export-components -- shared test fixtures are intentionally colocated with the provider harness */
import { type ReactNode } from "react";
import { vi } from "vitest";

import type { ProvenControlPeer } from "../control/client";
import { decodeV2ReconnectSnapshot, type ModelInventoryEntry, type V2ReconnectSnapshot } from "../control/contracts";
import type { V2StreamCallbacks, V2StreamTerminal } from "../control/events";
import {
  validV2Node,
  validV2OperationAccepted,
  validV2ReconnectSnapshot,
  validV2Slot,
  v2Ids,
} from "../control/testSupport";
import { NodeSessionProvider, type BootstrapSnapshot, type NodeSessionServices } from "./NodeSession";
import type { ModelsScreenServices } from "../models/useModelsController";

export const testEndpoint = "http://127.0.0.1:8080";
export const testPeer = Object.freeze({}) as ProvenControlPeer;

export function controlSnapshot(
  overrides: {
    epoch?: string;
    cursor?: string;
    revision?: string;
    slot?: Record<string, unknown>;
    operations?: unknown[];
    cursorGap?: boolean;
  } = {},
): V2ReconnectSnapshot {
  const epoch = overrides.epoch ?? v2Ids.epoch;
  const revision = overrides.revision ?? "11";
  return decodeV2ReconnectSnapshot({
    ...validV2ReconnectSnapshot,
    epoch,
    revision,
    stream: { epoch, cursor: overrides.cursor ?? revision, cursor_gap: overrides.cursorGap ?? false },
    nodes: [validV2Node],
    slots: [{ ...validV2Slot, status: "unloaded", model_id: null, operation_id: null, ...overrides.slot }],
    operations: overrides.operations ?? [],
    events: [],
  });
}

export function scriptedV2Control(initial = controlSnapshot()) {
  let callbacks: V2StreamCallbacks | undefined;
  const openV2Events = vi.fn((_peer, _resume, next: V2StreamCallbacks) => {
    callbacks = next;
    queueMicrotask(() => next.onSnapshot(initial));
    const terminal: V2StreamTerminal = { kind: "cancelled", cursor: initial.stream.cursor };
    return { dispose: vi.fn(), cancel: vi.fn(), finished: Promise.resolve(terminal) };
  });
  return {
    openV2Events,
    emitReplacement(snapshot: V2ReconnectSnapshot) {
      callbacks?.onSnapshot(snapshot);
    },
    emitEvent(event: Parameters<V2StreamCallbacks["onEvent"]>[0]) {
      callbacks?.onEvent(event);
    },
    terminate(terminal: V2StreamTerminal) {
      callbacks?.onTerminal(terminal);
    },
  };
}

export function servicesWithControl(
  control = scriptedV2Control(),
  overrides: Partial<NodeSessionServices & ModelsScreenServices> = {},
): NodeSessionServices & ModelsScreenServices {
  const bootstrap: BootstrapSnapshot = {
    ownership: "owned",
    endpoint: testEndpoint,
    childRunning: true,
    error: null,
  };
  return {
    bootstrap: {
      snapshot: vi.fn().mockResolvedValue(bootstrap),
      start: vi.fn().mockResolvedValue(bootstrap),
      attach: vi.fn().mockResolvedValue({ ...bootstrap, ownership: "attached" }),
      stop: vi.fn().mockResolvedValue({ ...bootstrap, ownership: "none", childRunning: false }),
    },
    readControlToken: vi.fn().mockResolvedValue("ab".repeat(32)),
    proveV2ControlPeer: vi.fn().mockResolvedValue(testPeer),
    openV2Events: control.openV2Events,
    getInventory: vi.fn().mockResolvedValue([]),
    confirmGlobalDownloadCancel: vi.fn().mockReturnValue(false),
    downloadV2Model: vi.fn().mockResolvedValue(validV2OperationAccepted),
    loadV2Slot: vi.fn().mockResolvedValue(validV2OperationAccepted),
    unloadV2Slot: vi.fn().mockResolvedValue(validV2OperationAccepted),
    cancelV2Operation: vi.fn().mockResolvedValue(validV2OperationAccepted),
    getStatus: vi.fn(),
    createControlEventStream: vi.fn(),
    ...overrides,
  };
}

export function modelFixture(id = "gemma-3-4b-it-q4"): ModelInventoryEntry {
  return {
    id,
    repo: `loxa/${id}`,
    revision: "0123456789abcdef",
    filename: `${id}.gguf`,
    sha256: "ab".repeat(32),
    sizeBytes: 1024,
    license: "Apache-2.0",
    params: "4B",
    quant: "Q4_K_M",
    minFreeMemoryGiB: 6,
    artifact: { kind: "downloaded" },
    compatibility: { compatible: true, reason: "Eligible" },
    engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
  };
}

export function SessionHarness({ services, children }: { services: NodeSessionServices; children: ReactNode }) {
  return (
    <NodeSessionProvider services={services} endpoint={testEndpoint}>
      {children}
    </NodeSessionProvider>
  );
}
