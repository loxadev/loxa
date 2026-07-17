import { SseDecodeError, SseDecoder } from "../chat/sse";
import {
  assertControlToken,
  assertProvenControlIdentity,
  controlUrl,
  fetchFromProvenControlPeer,
  v2ControlHttpError,
  type ControlFetch,
  type ProvenControlPeer,
} from "./client";
import {
  ControlContractError,
  decodeControlEvent,
  decodeReconnectSnapshot,
  decodeV2ControlEventJson,
  decodeV2ReconnectSnapshotJson,
  type ControlEvent,
  type ReconnectSnapshot,
  type V2ControlEvent,
  type V2Node,
  type V2Operation,
  type V2ReconnectSnapshot,
  type V2Slot,
} from "./contracts";

export type ControlStreamTerminal =
  { kind: "cancelled"; cursor: number } | { kind: "error"; cursor: number; message: string };

export type ControlStreamCallbacks = {
  onSnapshot(snapshot: ReconnectSnapshot): void;
  onEvent(event: ControlEvent): void;
  onTerminal(terminal: ControlStreamTerminal): void;
};

export type ControlStreamHandle = {
  dispose(): void;
  cancel(): void;
  finished: Promise<ControlStreamTerminal>;
};

const EVENT_CONNECT_TIMEOUT_MS = 5_000;
const MAX_V2_SNAPSHOT_BYTES = 2 * 1024 * 1024;
const MAX_V2_SSE_FRAME_BYTES = MAX_V2_SNAPSHOT_BYTES + 1024;
const MAX_V2_EVENT_FRAME_BYTES = 16 * 1024 + 512;
const MAX_U64 = 18_446_744_073_709_551_615n;
const V2_UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const DECIMAL_PATTERN = /^(0|[1-9][0-9]*)$/;

export type ResumeCursor = { epoch: string; cursor: string };
export type V2ControlState = {
  epoch: string;
  cursor: string;
  revision: string;
  nodes: V2Node[];
  slots: V2Slot[];
  operations: V2Operation[];
};

type AppliedEvent = { eventId: string; sequence: string; fingerprint: string };
type AppliedEvents = { bySequence: Map<string, AppliedEvent>; byId: Map<string, AppliedEvent> };
const appliedEvents = new WeakMap<V2ControlState, AppliedEvents>();

function canonical(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonical(record[key])}`)
    .join(",")}}`;
}

function immutableCopy<T>(value: T): T {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return Object.freeze(value.map((item) => immutableCopy(item))) as T;
  const copy = Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, immutableCopy(item)]),
  );
  return Object.freeze(copy) as T;
}

function trackerFrom(events: readonly V2ControlEvent[]): AppliedEvents {
  const tracker: AppliedEvents = { bySequence: new Map(), byId: new Map() };
  for (const event of events) {
    const applied = { eventId: event.event_id, sequence: event.sequence, fingerprint: canonical(event) };
    tracker.bySequence.set(event.sequence, applied);
    tracker.byId.set(event.event_id, applied);
  }
  return tracker;
}

function exactDuplicate(tracker: AppliedEvents, event: V2ControlEvent): boolean {
  const fingerprint = canonical(event);
  const sameSequence = tracker.bySequence.get(event.sequence);
  const sameId = tracker.byId.get(event.event_id);
  if (!sameSequence && !sameId) return false;
  if (
    sameSequence?.eventId === event.event_id &&
    sameSequence.fingerprint === fingerprint &&
    sameId?.sequence === event.sequence &&
    sameId.fingerprint === fingerprint
  ) {
    return true;
  }
  throw new ControlContractError("v2 live event duplicate identity conflict");
}

function rememberEvent(tracker: AppliedEvents, event: V2ControlEvent): void {
  const applied = { eventId: event.event_id, sequence: event.sequence, fingerprint: canonical(event) };
  tracker.bySequence.set(event.sequence, applied);
  tracker.byId.set(event.event_id, applied);
  while (tracker.bySequence.size > 1024) {
    const oldestSequence = tracker.bySequence.keys().next().value as string | undefined;
    if (oldestSequence === undefined) break;
    const oldest = tracker.bySequence.get(oldestSequence);
    tracker.bySequence.delete(oldestSequence);
    if (oldest) tracker.byId.delete(oldest.eventId);
  }
}

export function applyV2Snapshot(_previous: V2ControlState | undefined, snapshot: V2ReconnectSnapshot): V2ControlState {
  const state = immutableCopy<V2ControlState>({
    epoch: snapshot.epoch,
    cursor: snapshot.stream.cursor,
    revision: snapshot.revision,
    nodes: [...snapshot.nodes],
    slots: [...snapshot.slots],
    operations: [...snapshot.operations],
  });
  appliedEvents.set(state, trackerFrom(snapshot.events));
  return state;
}

function isImmediateSuccessor(previous: string, next: string): boolean {
  return BigInt(previous) + 1n === BigInt(next);
}

function updateOperations(operations: V2Operation[], changed: V2Operation): V2Operation[] {
  const next = operations.filter((operation) => operation.operation_id !== changed.operation_id);
  next.push(changed);
  next.sort((left, right) => {
    const revisionOrder = BigInt(left.created_revision) - BigInt(right.created_revision);
    if (revisionOrder < 0n) return -1;
    if (revisionOrder > 0n) return 1;
    return left.operation_id.localeCompare(right.operation_id);
  });
  while (next.length > 256) {
    const terminalIndex = next.findIndex(
      (operation) =>
        operation.status === "succeeded" || operation.status === "failed" || operation.status === "cancelled",
    );
    if (terminalIndex < 0) throw new ControlContractError("v2 operation retention");
    next.splice(terminalIndex, 1);
  }
  return next;
}

export function applyV2Event(state: V2ControlState, event: V2ControlEvent): V2ControlState {
  if (event.epoch !== state.epoch) return state;
  const tracker = appliedEvents.get(state) ?? trackerFrom([]);
  if (exactDuplicate(tracker, event)) return state;
  const sequenceOrder = BigInt(event.sequence) - BigInt(state.cursor);
  if (sequenceOrder <= 0n) {
    throw new ControlContractError("v2 live event regressed or changed duplicate identity");
  }
  if (!isImmediateSuccessor(state.cursor, event.sequence) || !isImmediateSuccessor(state.revision, event.revision)) {
    throw new ControlContractError("v2 live event cursor gap");
  }
  const node = state.nodes[0];
  const slot = state.slots[0];
  const existingOperation = state.operations.find((operation) => operation.operation_id === event.operation_id);
  if (
    !node ||
    !slot ||
    event.node_id !== node.node_id ||
    event.node_instance_id !== node.node_instance_id ||
    (event.slot_id !== null && event.slot_id !== slot.slot_id) ||
    (event.node !== null &&
      (event.node.node_id !== node.node_id || event.node.node_instance_id !== node.node_instance_id)) ||
    (event.slot !== null && (event.slot.node_id !== node.node_id || event.slot.slot_id !== slot.slot_id)) ||
    (event.operation !== null &&
      (event.operation.node_id !== node.node_id ||
        (event.operation.slot_id !== null && event.operation.slot_id !== slot.slot_id) ||
        (existingOperation !== undefined &&
          (existingOperation.node_id !== event.operation.node_id ||
            existingOperation.slot_id !== event.operation.slot_id ||
            existingOperation.kind !== event.operation.kind ||
            existingOperation.created_revision !== event.operation.created_revision ||
            existingOperation.created_at_unix_ms !== event.operation.created_at_unix_ms))))
  ) {
    throw new ControlContractError("v2 live event ownership correlation");
  }
  const next = immutableCopy<V2ControlState>({
    epoch: state.epoch,
    cursor: event.sequence,
    revision: event.revision,
    nodes: event.node === null ? state.nodes : [event.node],
    slots: event.slot === null ? state.slots : [event.slot],
    operations: event.operation === null ? state.operations : updateOperations(state.operations, event.operation),
  });
  const nextTracker: AppliedEvents = { bySequence: new Map(tracker.bySequence), byId: new Map(tracker.byId) };
  rememberEvent(nextTracker, event);
  appliedEvents.set(next, nextTracker);
  return next;
}

export type V2StreamTerminal =
  { kind: "cancelled"; cursor: string } | { kind: "error"; cursor: string; message: string };

export type V2StreamCallbacks = {
  onSnapshot(snapshot: V2ReconnectSnapshot): void;
  onRetainedEvent(event: V2ControlEvent): void;
  onEvent(event: V2ControlEvent): void;
  onTerminal(terminal: V2StreamTerminal): void;
};

export type V2EventStream = {
  dispose(): void;
  cancel(): void;
  finished: Promise<V2StreamTerminal>;
};

type V2SseFrame = { data: string; event?: string; id?: string };

class V2SseDecoder {
  private readonly buffer = new Uint8Array(MAX_V2_SSE_FRAME_BYTES);
  private length = 0;
  private limit = MAX_V2_SSE_FRAME_BYTES;
  private finished = false;

  push(chunk: Uint8Array, consume: (frame: V2SseFrame) => void): void {
    if (this.finished) throw new SseDecodeError("The v2 SSE decoder is already finished.");
    for (const byte of chunk) {
      if (byte !== 10) {
        const pendingBoundary = this.boundaryEndingInCarriageReturn();
        if (pendingBoundary > 0) this.emit(pendingBoundary, consume);
      }
      if (this.length >= this.limit) throw new SseDecodeError("The v2 SSE frame exceeds its bound.");
      this.buffer[this.length] = byte;
      this.length += 1;
      if (byte === 10) {
        const boundary = this.boundaryEndingInLineFeed();
        if (boundary > 0) this.emit(boundary, consume);
      }
    }
  }

  finish(consume: (frame: V2SseFrame) => void): void {
    if (this.finished) return;
    this.finished = true;
    const boundary = this.boundaryEndingInCarriageReturn();
    if (boundary > 0) this.emit(boundary, consume);
    if (this.length > 0) throw new SseDecodeError("The v2 SSE stream ended with an incomplete frame.");
  }

  expectLiveEvents(): void {
    this.limit = MAX_V2_EVENT_FRAME_BYTES;
  }

  private boundaryEndingInLineFeed(): number {
    const suffixes = [
      [13, 10, 13, 10],
      [13, 10, 10],
      [10, 13, 10],
      [13, 13, 10],
      [10, 10],
    ];
    return this.matchingSuffix(suffixes);
  }

  private boundaryEndingInCarriageReturn(): number {
    return this.matchingSuffix([
      [13, 10, 13],
      [10, 13],
      [13, 13],
    ]);
  }

  private matchingSuffix(suffixes: number[][]): number {
    for (const suffix of suffixes) {
      if (
        suffix.length <= this.length &&
        suffix.every((byte, index) => this.buffer[this.length - suffix.length + index] === byte)
      ) {
        return suffix.length;
      }
    }
    return 0;
  }

  private emit(boundary: number, consume: (frame: V2SseFrame) => void): void {
    const raw = this.buffer.slice(0, this.length - boundary);
    this.length = 0;
    const frame = this.parseBytes(raw);
    if (frame) consume(frame);
  }

  private parseBytes(raw: Uint8Array): V2SseFrame | null {
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(raw);
    } catch {
      throw new SseDecodeError("The v2 SSE stream contains invalid UTF-8.");
    }
    return this.parse(text);
  }

  private parse(raw: string): V2SseFrame | null {
    const data: string[] = [];
    let event: string | undefined;
    let id: string | undefined;
    for (const line of raw.split(/\r\n|\n|\r/)) {
      if (line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon < 0 ? line : line.slice(0, colon);
      let value = colon < 0 ? "" : line.slice(colon + 1);
      if (value.startsWith(" ")) value = value.slice(1);
      if (field === "data") data.push(value);
      else if (field === "event") event = value;
      else if (field === "id" && !value.includes("\0")) id = value;
    }
    if (data.length === 0) return null;
    return {
      ...(event === undefined ? {} : { event }),
      ...(id === undefined ? {} : { id }),
      data: data.join("\n"),
    };
  }
}

function validateResume(resume: ResumeCursor | undefined): void {
  if (resume === undefined) return;
  if (!V2_UUID_PATTERN.test(resume.epoch) || !DECIMAL_PATTERN.test(resume.cursor) || BigInt(resume.cursor) > MAX_U64) {
    throw new ControlContractError("v2 resume cursor");
  }
}

export function openV2Events(
  peer: ProvenControlPeer,
  resume: ResumeCursor | undefined,
  callbacks: V2StreamCallbacks,
  signal?: AbortSignal,
): V2EventStream {
  validateResume(resume);
  const query = resume === undefined ? "" : `?epoch=${resume.epoch}&cursor=${resume.cursor}`;
  const controller = new AbortController();
  let cursor = resume?.cursor ?? "0";
  let revision = "0";
  let epoch: string | null = resume?.epoch ?? null;
  let nodeId: string | null = null;
  let nodeInstanceId: string | null = null;
  let streamEvents = trackerFrom([]);
  let abortCause: "cancel" | "dispose" | "timeout" | null = null;
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let readerCancelled = false;
  let terminalNotified = false;
  const cancelReader = async () => {
    if (reader === null || readerCancelled) return;
    readerCancelled = true;
    await Promise.resolve(reader.cancel()).catch(() => undefined);
  };
  const abort = (cause: "cancel" | "dispose" | "timeout") => {
    if (abortCause !== null) return;
    abortCause = cause;
    controller.abort();
    void cancelReader();
  };
  const callerAbort = () => abort("cancel");
  if (signal?.aborted) abort("cancel");
  else signal?.addEventListener("abort", callerAbort, { once: true });
  const terminal = (value: V2StreamTerminal): V2StreamTerminal => {
    if (terminalNotified) return value;
    terminalNotified = true;
    if (abortCause !== "dispose") {
      try {
        callbacks.onTerminal(value);
      } catch {
        // A consumer exception cannot create a second terminal callback.
      }
    }
    return value;
  };
  const cancelled = () => terminal({ kind: "cancelled", cursor });
  const failed = (message: string) => terminal({ kind: "error", cursor, message });
  const connectTimeout = setTimeout(() => abort("timeout"), EVENT_CONNECT_TIMEOUT_MS);

  const finished = (async (): Promise<V2StreamTerminal> => {
    if (abortCause !== null) return cancelled();
    let sawSnapshot = false;
    try {
      const response = await fetchFromProvenControlPeer(peer, `/loxa/v2/events${query}`, {
        method: "GET",
        headers: { accept: "text/event-stream" },
        signal: controller.signal,
      });
      clearTimeout(connectTimeout);
      if (abortCause === "timeout") return failed("Connecting to durable Loxa updates timed out.");
      if (abortCause !== null) return cancelled();
      if (!response.ok) return failed((await v2ControlHttpError(response)).message);
      if (response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "text/event-stream") {
        return failed("The Loxa durable event service returned an invalid media type.");
      }
      if (!response.body) return failed("The Loxa durable event service returned no response body.");
      reader = response.body.getReader();
      const decoder = new V2SseDecoder();
      const consume = (frame: V2SseFrame): void => {
        if (abortCause !== null) return;
        if (frame.event === "snapshot") {
          if (sawSnapshot || frame.id !== undefined) throw new ControlContractError("duplicate v2 reconnect snapshot");
          const snapshot = decodeV2ReconnectSnapshotJson(frame.data);
          const snapshotNode = snapshot.nodes[0];
          if (!snapshotNode) throw new ControlContractError("v2 snapshot node");
          assertProvenControlIdentity(peer, snapshotNode.node_id, snapshotNode.node_instance_id);
          nodeId = snapshotNode.node_id;
          nodeInstanceId = snapshotNode.node_instance_id;
          epoch = snapshot.epoch;
          cursor = snapshot.stream.cursor;
          revision = snapshot.revision;
          sawSnapshot = true;
          streamEvents = trackerFrom(snapshot.events);
          decoder.expectLiveEvents();
          callbacks.onSnapshot(snapshot);
          for (const retained of snapshot.events) callbacks.onRetainedEvent(retained);
        } else if (frame.event === "state") {
          if (!sawSnapshot) throw new ControlContractError("v2 event before reconnect snapshot");
          const event = decodeV2ControlEventJson(frame.data);
          if (event.epoch !== epoch) return;
          if (
            event.node_id !== nodeId ||
            event.node_instance_id === null ||
            event.node_instance_id !== nodeInstanceId
          ) {
            throw new ControlContractError("v2 live event node identity");
          }
          if (exactDuplicate(streamEvents, event)) return;
          const sequenceOrder = BigInt(event.sequence) - BigInt(cursor);
          if (sequenceOrder <= 0n) {
            throw new ControlContractError("regressing v2 control event");
          }
          if (
            frame.id !== event.sequence ||
            !isImmediateSuccessor(cursor, event.sequence) ||
            !isImmediateSuccessor(revision, event.revision)
          ) {
            throw new ControlContractError("v2 control event cursor gap");
          }
          cursor = event.sequence;
          revision = event.revision;
          rememberEvent(streamEvents, event);
          callbacks.onEvent(event);
        } else {
          throw new ControlContractError("v2 control event type");
        }
      };
      while (true) {
        const result = await reader.read();
        if (abortCause !== null) return cancelled();
        if (result.done) {
          decoder.finish(consume);
          await cancelReader();
          return failed("Durable Loxa updates disconnected.");
        }
        decoder.push(result.value, consume);
      }
    } catch (error) {
      if (abortCause === "timeout") return failed("Connecting to durable Loxa updates timed out.");
      if (abortCause !== null || controller.signal.aborted) return cancelled();
      await cancelReader();
      if (error instanceof ControlContractError || error instanceof SseDecodeError || error instanceof SyntaxError) {
        return failed("The Loxa node returned an invalid durable update stream.");
      }
      return failed(
        reader === null ? "Could not connect to durable Loxa updates." : "Durable Loxa updates failed while reading.",
      );
    } finally {
      clearTimeout(connectTimeout);
      signal?.removeEventListener("abort", callerAbort);
      reader?.releaseLock();
    }
  })();

  return { cancel: () => abort("cancel"), dispose: () => abort("dispose"), finished };
}

export function streamControlEvents(
  endpoint: string,
  token: string,
  initialCursor: number,
  callbacks: ControlStreamCallbacks,
  signal?: AbortSignal,
  fetch: ControlFetch = globalThis.fetch,
): ControlStreamHandle {
  assertControlToken(token);
  if (!Number.isSafeInteger(initialCursor) || initialCursor < 0) {
    throw new ControlContractError("event cursor");
  }
  const url = controlUrl(endpoint, `/loxa/v1/events?cursor=${initialCursor}`);
  const controller = new AbortController();
  let cursor = initialCursor;
  let abortCause: "cancel" | "dispose" | "timeout" | null = null;
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let readerCancelled = false;
  let terminalNotified = false;

  const cancelReader = async () => {
    if (reader === null || readerCancelled) return;
    readerCancelled = true;
    await Promise.resolve(reader.cancel()).catch(() => undefined);
  };
  const abort = (cause: "cancel" | "dispose" | "timeout") => {
    if (abortCause !== null) return;
    abortCause = cause;
    controller.abort();
    void cancelReader();
  };
  const callerAbort = () => abort("cancel");
  if (signal?.aborted) abort("cancel");
  else signal?.addEventListener("abort", callerAbort, { once: true });

  const terminal = (value: ControlStreamTerminal): ControlStreamTerminal => {
    if (terminalNotified) return value;
    terminalNotified = true;
    if (abortCause !== "dispose") {
      try {
        callbacks.onTerminal(value);
      } catch {
        // A consumer exception cannot create a second terminal callback.
      }
    }
    return value;
  };
  const cancelled = () => terminal({ kind: "cancelled", cursor });
  const failed = (message: string) => terminal({ kind: "error", cursor, message });
  const connectTimeout = setTimeout(() => abort("timeout"), EVENT_CONNECT_TIMEOUT_MS);

  const finished = (async (): Promise<ControlStreamTerminal> => {
    if (abortCause !== null) return cancelled();
    let sawSnapshot = false;
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          accept: "text/event-stream",
          authorization: `Bearer ${token}`,
        },
        signal: controller.signal,
      });
      clearTimeout(connectTimeout);
      if (abortCause === "timeout") return failed("Connecting to live model updates timed out.");
      if (abortCause !== null) return cancelled();
      if (!response.ok) return failed(`The Loxa event service returned HTTP ${response.status}.`);
      if (!response.body) return failed("The Loxa event service returned no response body.");
      reader = response.body.getReader();
      if (abortCause !== null) {
        await cancelReader();
        return cancelled();
      }
      const decoder = new SseDecoder();
      while (true) {
        const result = await reader.read();
        if (abortCause !== null) return cancelled();
        if (result.done) {
          if (decoder.hasPendingFrame()) {
            throw new ControlContractError("unterminated final control event");
          }
          if (decoder.finish().length > 0) {
            throw new ControlContractError("unterminated final control event");
          }
          await cancelReader();
          return failed("Live model updates disconnected.");
        }
        for (const frame of decoder.push(result.value)) {
          if (abortCause !== null) return cancelled();
          const payload = JSON.parse(frame.data) as unknown;
          if (frame.event === "snapshot") {
            if (sawSnapshot) throw new ControlContractError("duplicate reconnect snapshot");
            const snapshot = decodeReconnectSnapshot(payload);
            if (snapshot.cursor < cursor) throw new ControlContractError("regressing reconnect snapshot");
            cursor = snapshot.cursor;
            sawSnapshot = true;
            callbacks.onSnapshot(snapshot);
          } else if (frame.event === "operation") {
            if (!sawSnapshot) throw new ControlContractError("event before reconnect snapshot");
            const event = decodeControlEvent(payload);
            if (event.sequence <= cursor || frame.id !== String(event.sequence)) {
              throw new ControlContractError("regressing control event");
            }
            cursor = event.sequence;
            callbacks.onEvent(event);
          } else {
            throw new ControlContractError("control event type");
          }
        }
      }
    } catch (error) {
      if (abortCause === "timeout") return failed("Connecting to live model updates timed out.");
      if (abortCause !== null || controller.signal.aborted) return cancelled();
      await cancelReader();
      if (error instanceof ControlContractError || error instanceof SseDecodeError || error instanceof SyntaxError) {
        return failed("The Loxa node returned an invalid model update stream.");
      }
      return failed(
        reader === null ? "Could not connect to live model updates." : "Live model updates failed while reading.",
      );
    } finally {
      clearTimeout(connectTimeout);
      signal?.removeEventListener("abort", callerAbort);
      reader?.releaseLock();
    }
  })();

  return {
    cancel: () => abort("cancel"),
    dispose: () => abort("dispose"),
    finished,
  };
}
