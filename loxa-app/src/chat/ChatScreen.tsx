import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";

import type {
  getCapabilities as defaultGetCapabilities,
  getControlNode as defaultGetControlNode,
  getInventory as defaultGetInventory,
  getOperation as defaultGetOperation,
  loadModel as defaultLoadModel,
} from "../control/client";
import type { ModelInventoryEntry, NodeControlStatus } from "../control/contracts";
import type { ControlStreamHandle, streamControlEvents as defaultStreamControlEvents } from "../control/events";
import type { getModels as defaultGetModels, getStatus as defaultGetStatus } from "../node/client";
import type { NodeSessionPhase } from "../node/NodeSession";
import { ChatComposer } from "./ChatComposer";
import type { ConversationHistoryController } from "./conversationHistory";
import type { ChatSummary, PersistentTurnCallbacks, PersistentTurnHandle } from "./historyClient";
import styles from "./ChatScreen.module.css";
import { ChatTranscript, type ChatTurn } from "./ChatTranscript";
import type { StreamCallbacks, StreamHandle, StreamTerminal } from "./streamChat";

export type ChatScreenServices = {
  getStatus: typeof defaultGetStatus;
  getModels: typeof defaultGetModels;
  readControlToken(endpoint: string): Promise<string>;
  getCapabilities: typeof defaultGetCapabilities;
  getControlNode: typeof defaultGetControlNode;
  getInventory: typeof defaultGetInventory;
  getOperation: typeof defaultGetOperation;
  loadModel: typeof defaultLoadModel;
  createControlEventStream: typeof defaultStreamControlEvents;
  createChatStream(endpoint: string, request: unknown, callbacks: StreamCallbacks): StreamHandle;
  getChat?(endpoint: string, token: string, chatId: string, options?: { signal?: AbortSignal }): Promise<ChatSummary>;
  listTurns?(
    endpoint: string,
    token: string,
    chatId: string,
    page?: { limit?: number; after?: string },
    options?: { signal?: AbortSignal },
  ): Promise<{ turns: import("./historyClient").HistoryTurn[]; nextAfter: string | null }>;
  listMessageSummaries?(
    endpoint: string,
    token: string,
    chatId: string,
    turnId: string,
    options?: { signal?: AbortSignal },
  ): Promise<import("./historyClient").MessageSummary[]>;
  getMessageContent?(
    endpoint: string,
    token: string,
    chatId: string,
    turnId: string,
    messageId: string,
    options?: { signal?: AbortSignal },
  ): Promise<string>;
  createPersistentTurn?(
    endpoint: string,
    token: string,
    chatId: string,
    content: string,
    callbacks: PersistentTurnCallbacks,
    signal?: AbortSignal,
  ): PersistentTurnHandle;
  copyText(text: string): Promise<void>;
};

type ConnectionState = "checking" | "disconnected" | "ready";
type CapabilityState = "checking" | "supported" | "unsupported" | "unavailable";
type ControlStreamState = "live" | "reconnecting" | "unavailable";
export type ChatNodeAvailability = {
  phase: NodeSessionPhase;
  proven: boolean;
  error: string | null;
};
export type ChatScreenHistory = Pick<ConversationHistoryController, "selection" | "create" | "reconcileSummary">;

export function ChatScreen({
  services,
  endpoint,
  nodeAvailability,
  onModelMutationStart,
  onModelMutationSettled,
  history,
  onInteractionLockChange,
  onNavigateModels,
}: {
  services: ChatScreenServices;
  endpoint: string;
  nodeAvailability?: ChatNodeAvailability;
  onModelMutationStart?: (operationId?: string) => void;
  onModelMutationSettled?: (operationId: string) => void | Promise<void>;
  history?: ChatScreenHistory | null;
  onInteractionLockChange?: (locked: boolean) => void;
  onNavigateModels?: () => void;
}) {
  const availabilityPhase = nodeAvailability?.phase;
  const availabilityProven = nodeAvailability?.proven;
  const availabilityError = nodeAvailability?.error;
  const [connection, setConnection] = useState<ConnectionState>("checking");
  const [requestModel, setRequestModel] = useState<string | null>(null);
  const [activeModel, setActiveModel] = useState<string | null>(null);
  const [selectedModel, setSelectedModel] = useState("");
  const [eligibleModels, setEligibleModels] = useState<ModelInventoryEntry[]>([]);
  const [modelOperation, setModelOperation] = useState<"idle" | "switching">("idle");
  const [controlBusy, setControlBusy] = useState(false);
  const [controlStreamState, setControlStreamState] = useState<ControlStreamState>("live");
  const [input, setInput] = useState("");
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [restoreError, setRestoreError] = useState("");
  const [omittedTurns, setOmittedTurns] = useState(0);
  const [connectionError, setConnectionError] = useState("");
  const [chatCapability, setChatCapability] = useState<CapabilityState>("checking");
  const [attachmentReason, setAttachmentReason] = useState("Checking document input support.");
  const availabilityBlocked =
    availabilityProven === false || (availabilityPhase === "reconciling" && modelOperation === "idle");
  const availabilityBlockedReason =
    availabilityBlocked && availabilityPhase !== undefined
      ? nodeSessionUnavailableReason({
          phase: availabilityPhase,
          proven: availabilityProven ?? false,
          error: availabilityError ?? null,
        })
      : "";
  const handle = useRef<StreamHandle | null>(null);
  const persistentHandle = useRef<PersistentTurnHandle | null>(null);
  const persistentController = useRef<AbortController | null>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const focusAfterTerminal = useRef(false);
  const stopRequested = useRef(false);
  const lifecycleController = useRef<AbortController | null>(null);
  const controlStream = useRef<ControlStreamHandle | null>(null);
  const operations = useRef(new Map<string, { status: string }>());
  const activeTurnId = useRef<number | null>(null);
  const nextTurnId = useRef(1);
  const mounted = useRef(true);
  const recoveryRequired = useRef(false);
  const truthVersion = useRef(0);
  const displayBuffer = useRef<{ turnId: number; response: string } | null>(null);
  const displayFrame = useRef<number | null>(null);
  const onModelMutationStartRef = useRef(onModelMutationStart);
  const onModelMutationSettledRef = useRef(onModelMutationSettled);
  onModelMutationStartRef.current = onModelMutationStart;
  onModelMutationSettledRef.current = onModelMutationSettled;
  const selectedChatIdRef = useRef(history?.selection.chatId ?? null);
  const handledSelection = useRef<string | null>(null);
  const suppressNextSelection = useRef(false);
  const historyControllers = useRef(new Set<AbortController>());
  const restoreController = useRef<AbortController | null>(null);
  const restoreGeneration = useRef(0);

  useLayoutEffect(() => {
    if (!availabilityBlocked) return;
    const turnId = activeTurnId.current;
    if (turnId === null) return;
    if (displayFrame.current !== null) {
      cancelScheduledFrame(displayFrame.current);
      displayFrame.current = null;
    }
    const bufferedResponse = displayBuffer.current?.turnId === turnId ? displayBuffer.current.response : null;
    displayBuffer.current = null;
    activeTurnId.current = null;
    stopRequested.current = false;
    focusAfterTerminal.current = true;
    setTurns((current) =>
      current.map((turn) =>
        turn.id === turnId
          ? terminalTurn(bufferedResponse === null ? turn : { ...turn, response: bufferedResponse }, {
              kind: "cancelled",
            })
          : turn,
      ),
    );
  }, [availabilityBlocked]);

  const ownHistoryAction = useCallback((parent?: AbortSignal) => {
    const controller = new AbortController();
    const abortFromParent = () => controller.abort();
    if (parent?.aborted) controller.abort();
    else parent?.addEventListener("abort", abortFromParent, { once: true });
    historyControllers.current.add(controller);
    return {
      controller,
      finish: () => {
        parent?.removeEventListener("abort", abortFromParent);
        historyControllers.current.delete(controller);
      },
    };
  }, []);

  const abortHistoryActions = useCallback(() => {
    restoreGeneration.current += 1;
    restoreController.current?.abort();
    restoreController.current = null;
    for (const historyController of historyControllers.current) historyController.abort();
    historyControllers.current.clear();
  }, []);

  const restoreConversation = useCallback(
    async (chatId: string, signal: AbortSignal, generation: number) => {
      const { listTurns, listMessageSummaries, getMessageContent } = services;
      if (!mounted.current || signal.aborted || generation !== restoreGeneration.current) return;
      setTurns([]);
      setRestoreError("");
      if (!listTurns || !listMessageSummaries || !getMessageContent) return;
      try {
        const token = await services.readControlToken(endpoint);
        if (!mounted.current || signal.aborted || generation !== restoreGeneration.current) return;
        const restored: ChatTurn[] = [];
        let after: string | undefined;
        let lastOrdinal = -1;
        let pageCount = 0;
        const seenCursors = new Set<string>();
        const seenTurnIds = new Set<string>();
        do {
          pageCount += 1;
          if (pageCount > 100) throw new Error("Invalid chat history pagination: too many pages.");
          const page = await listTurns(endpoint, token, chatId, { limit: 30, ...(after ? { after } : {}) }, { signal });
          if (!mounted.current || signal.aborted || generation !== restoreGeneration.current) return;
          for (const turn of page.turns) {
            if (seenTurnIds.has(turn.id) || turn.ordinal <= lastOrdinal) {
              throw new Error("Invalid chat history pagination: turns are duplicated or out of order.");
            }
            seenTurnIds.add(turn.id);
            lastOrdinal = turn.ordinal;
            const summaries = await listMessageSummaries(endpoint, token, chatId, turn.id, { signal });
            if (!mounted.current || signal.aborted || generation !== restoreGeneration.current) return;
            const user = summaries.find(({ role }) => role === "user");
            const assistant = summaries.find(({ role }) => role === "assistant");
            if (!user || !assistant) continue;
            const [prompt, response] = await Promise.all([
              getMessageContent(endpoint, token, chatId, turn.id, user.id, { signal }),
              getMessageContent(endpoint, token, chatId, turn.id, assistant.id, { signal }),
            ]);
            if (!mounted.current || signal.aborted || generation !== restoreGeneration.current) return;
            restored.push({
              id: turn.id,
              model: turn.recipeId,
              prompt,
              response,
              status: turn.state,
              error: turn.errorCode ? turn.errorCode.replace(/_/g, " ") : "",
            });
          }
          const next = page.nextAfter ?? undefined;
          if (next && seenCursors.has(next))
            throw new Error("Invalid chat history pagination: the cursor did not advance.");
          if (next) seenCursors.add(next);
          after = next;
        } while (after);
        if (
          mounted.current &&
          !signal.aborted &&
          generation === restoreGeneration.current &&
          selectedChatIdRef.current === chatId
        ) {
          setTurns(restored);
          setOmittedTurns(0);
        }
      } catch (reason) {
        if (mounted.current && !signal.aborted && generation === restoreGeneration.current) {
          setRestoreError(message(reason));
        }
      }
    },
    [endpoint, services],
  );

  const runRestore = useCallback(
    async (chatId: string, parent?: AbortSignal) => {
      restoreController.current?.abort();
      const owned = ownHistoryAction(parent);
      restoreController.current = owned.controller;
      const generation = ++restoreGeneration.current;
      try {
        await restoreConversation(chatId, owned.controller.signal, generation);
      } finally {
        if (restoreController.current === owned.controller) restoreController.current = null;
        owned.finish();
      }
    },
    [ownHistoryAction, restoreConversation],
  );

  useEffect(() => {
    const controller = new AbortController();
    let disposed = false;
    mounted.current = true;
    recoveryRequired.current = false;

    if (availabilityBlocked) {
      setConnection("disconnected");
      setConnectionError(availabilityBlockedReason);
      setRequestModel(null);
      setActiveModel(null);
      setSelectedModel("");
      setEligibleModels([]);
      setControlBusy(false);
      setControlStreamState("unavailable");
      setChatCapability("unavailable");
      setAttachmentReason("Document input support cannot be checked until the node is connected.");
      return () => {
        disposed = true;
        mounted.current = false;
        controller.abort();
        abortHistoryActions();
      };
    }

    setConnection("checking");
    setConnectionError("");
    setRequestModel(null);
    setActiveModel(null);
    setSelectedModel("");
    setEligibleModels([]);
    setControlBusy(false);
    setControlStreamState("live");
    setChatCapability("checking");
    setAttachmentReason("Checking document input support.");
    void Promise.all([
      services.getStatus(endpoint, { signal: controller.signal }),
      services.getModels(endpoint, { signal: controller.signal }),
    ])
      .then(([status, models]) => {
        if (disposed) return;
        if (status.health !== "ready") {
          setConnection("disconnected");
          return;
        }
        setRequestModel(models.data[0].id);
        setConnection(recoveryRequired.current ? "disconnected" : "ready");
      })
      .catch((reason: unknown) => {
        if (disposed || controller.signal.aborted) return;
        setConnectionError(message(reason));
        setConnection("disconnected");
      });

    void (async () => {
      if (disposed) return;
      const [capabilities, inventory, controlNode] = await Promise.all([
        services
          .readControlToken(endpoint)
          .then((token) => services.getCapabilities(endpoint, token, { signal: controller.signal })),
        services
          .readControlToken(endpoint)
          .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
        services
          .readControlToken(endpoint)
          .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
      ]);
      if (disposed) return;
      setChatCapability(capabilities.textChat ? "supported" : "unsupported");
      setAttachmentReason(
        capabilities.documentInput
          ? "Document input transport is not available in this desktop build."
          : capabilities.documentInputReason,
      );
      const eligible = inventory.filter(
        (entry) => entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible,
      );
      setEligibleModels(eligible);
      setActiveModel(controlNode.activeModelId);
      setSelectedModel(controlNode.activeModelId ?? eligible[0]?.id ?? "");
      setControlBusy(controlNode.operationId !== null);
      if (controlNode.status !== "ready") {
        setActiveModel(null);
        setConnectionError(nodeUnavailableReason(controlNode.status));
        setConnection("disconnected");
      }
      const connectControlStream = async (cursor: number, reconcile: boolean) => {
        if (reconcile) {
          const [node, nextInventory] = await Promise.all([
            services
              .readControlToken(endpoint)
              .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
            services
              .readControlToken(endpoint)
              .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
          ]);
          if (disposed) return;
          const eligibleNext = nextInventory.filter(
            (entry) => entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible,
          );
          setEligibleModels(eligibleNext);
          if (node.status === "ready" && node.activeModelId !== null) {
            setActiveModel(node.activeModelId);
            setSelectedModel(node.activeModelId);
            setConnectionError("");
            setConnection("ready");
          } else {
            setActiveModel(null);
            setConnectionError(nodeUnavailableReason(node.status));
            setConnection("disconnected");
          }
        }
        const streamToken = await services.readControlToken(endpoint);
        if (disposed) return;
        const previousStream = controlStream.current;
        controlStream.current = services.createControlEventStream(
          endpoint,
          streamToken,
          cursor,
          {
            onSnapshot: (snapshot) => {
              if (disposed) return;
              operations.current = new Map(snapshot.operations.map((operation) => [operation.id, operation]));
              snapshot.operations
                .filter(isActiveLifecycleOperation)
                .forEach((operation) => onModelMutationStartRef.current?.(operation.id));
              snapshot.operations.filter(isTerminalLifecycleOperation).forEach((operation) => {
                void onModelMutationSettledRef.current?.(operation.id);
              });
              setControlStreamState("live");
              setControlBusy(
                snapshot.operations.some(
                  (operation) => operation.status === "queued" || operation.status === "running",
                ),
              );
            },
            onEvent: (event) => {
              if (disposed) return;
              setControlStreamState("live");
              operations.current.set(event.operation.id, event.operation);
              if (isActiveLifecycleOperation(event.operation)) onModelMutationStartRef.current?.(event.operation.id);
              setControlBusy(
                [...operations.current.values()].some(
                  (operation) => operation.status === "queued" || operation.status === "running",
                ),
              );
              if (
                (event.operation.kind === "load" || event.operation.kind === "unload") &&
                isTerminalOperation(event.operation.status)
              ) {
                setControlBusy(true);
                const version = ++truthVersion.current;
                void Promise.all([
                  services
                    .readControlToken(endpoint)
                    .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
                  services
                    .readControlToken(endpoint)
                    .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
                ])
                  .then(([node, nextInventory]) => {
                    if (disposed || version !== truthVersion.current) return;
                    const eligibleNext = nextInventory.filter(
                      (entry) =>
                        entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible,
                    );
                    setEligibleModels(eligibleNext);
                    setControlBusy(
                      node.operationId !== null ||
                        [...operations.current.values()].some(
                          (operation) => operation.status === "queued" || operation.status === "running",
                        ),
                    );
                    if (node.status === "ready" && node.activeModelId !== null) {
                      setActiveModel(node.activeModelId);
                      setSelectedModel(node.activeModelId);
                      setConnectionError("");
                      setConnection("ready");
                    } else {
                      setActiveModel(null);
                      setConnectionError(nodeUnavailableReason(node.status));
                      setConnection("disconnected");
                    }
                  })
                  .catch(() => {
                    if (!disposed && version === truthVersion.current) setControlBusy(true);
                  })
                  .finally(() => {
                    if (!disposed) void onModelMutationSettledRef.current?.(event.operation.id);
                  });
              }
            },
            onTerminal: (terminal) => {
              if (disposed) return;
              setControlStreamState("reconnecting");
              setControlBusy(true);
              void connectControlStream(terminal.cursor, true).catch(() => {
                if (disposed || controller.signal.aborted) return;
                setControlStreamState("unavailable");
                setControlBusy(true);
              });
            },
          },
          controller.signal,
        );
        previousStream?.dispose();
      };
      await connectControlStream(0, false);
      if (controlNode.status === "recovery_required") {
        recoveryRequired.current = true;
        setConnectionError("Recovery required. Restart the node safely before using chat.");
        setConnection("disconnected");
      }
    })().catch(() => {
      if (disposed || controller.signal.aborted) return;
      setChatCapability("unavailable");
      setAttachmentReason("Document input support could not be verified for this model and backend.");
    });

    const disposeWork = () => {
      if (disposed) return;
      disposed = true;
      mounted.current = false;
      controller.abort();
      abortHistoryActions();
      lifecycleController.current?.abort();
      lifecycleController.current = null;
      persistentController.current?.abort();
      persistentController.current = null;
      controlStream.current?.dispose();
      controlStream.current = null;
      activeTurnId.current = null;
      if (displayFrame.current !== null) {
        cancelScheduledFrame(displayFrame.current);
        displayFrame.current = null;
      }
      displayBuffer.current = null;
      handle.current?.dispose();
      handle.current = null;
      persistentHandle.current?.dispose();
      persistentHandle.current = null;
    };
    window.addEventListener("beforeunload", disposeWork);
    return () => {
      window.removeEventListener("beforeunload", disposeWork);
      disposeWork();
    };
  }, [abortHistoryActions, availabilityBlocked, availabilityBlockedReason, endpoint, services]);

  useEffect(() => {
    if (!history) return;
    const { chatId, revision } = history.selection;
    const key = `${chatId ?? "new"}:${revision}`;
    if (handledSelection.current === key) return;
    handledSelection.current = key;
    selectedChatIdRef.current = chatId;
    if (suppressNextSelection.current && chatId !== null) {
      suppressNextSelection.current = false;
      return;
    }
    restoreController.current?.abort();
    restoreGeneration.current += 1;
    if (chatId === null) {
      setTurns([]);
      setOmittedTurns(0);
      setRestoreError("");
      return;
    }
    void runRestore(chatId);
  }, [history, history?.selection.chatId, history?.selection.revision, runRestore]);

  const latestTurn = turns[turns.length - 1];
  const responseInProgress = latestTurn?.status === "queued" || latestTurn?.status === "streaming";
  const canCompose =
    connection === "ready" &&
    chatCapability === "supported" &&
    requestModel !== null &&
    activeModel !== null &&
    !responseInProgress &&
    modelOperation === "idle" &&
    !controlBusy;

  useEffect(() => {
    onInteractionLockChange?.(responseInProgress);
  }, [onInteractionLockChange, responseInProgress]);

  useEffect(() => () => onInteractionLockChange?.(false), [onInteractionLockChange]);

  useEffect(() => {
    if (!focusAfterTerminal.current || responseInProgress || !canCompose) return;
    focusAfterTerminal.current = false;
    inputRef.current?.focus();
  }, [canCompose, latestTurn?.status, responseInProgress]);

  const updateTurn = (id: number | string, update: (current: ChatTurn) => ChatTurn) => {
    setTurns((current) => current.map((turn) => (turn.id === id ? update(turn) : turn)));
  };

  const sendPersistent = async (content: string) => {
    const createPersistentTurn = services.createPersistentTurn;
    if (!createPersistentTurn) return;
    const id = nextTurnId.current++;
    activeTurnId.current = id;
    displayBuffer.current = { turnId: id, response: "" };
    stopRequested.current = false;
    setConnectionError("");
    setInput("");
    setTurns((current) => [
      ...current,
      { id, model: activeModel ?? "loxa", prompt: content, response: "", status: "queued", error: "" },
    ]);
    const controller = new AbortController();
    persistentController.current = controller;
    try {
      let chatId = selectedChatIdRef.current;
      if (chatId === null) {
        if (!history) throw new Error("Chat history is unavailable.");
        suppressNextSelection.current = true;
        let chat: ChatSummary;
        try {
          chat = await history.create();
        } catch (error) {
          suppressNextSelection.current = false;
          throw error;
        }
        if (!mounted.current || controller.signal.aborted) return;
        chatId = chat.id;
        selectedChatIdRef.current = chat.id;
      }
      const token = await services.readControlToken(endpoint);
      if (!mounted.current || controller.signal.aborted) return;
      const stream = createPersistentTurn(
        endpoint,
        token,
        chatId,
        content,
        {
          onStarted: (_turnId, omitted) => {
            if (!mounted.current || controller.signal.aborted || activeTurnId.current !== id) return;
            setOmittedTurns(omitted);
            updateTurn(id, (turn) => ({ ...turn, status: "streaming" }));
          },
          onDelta: (text) => {
            if (!mounted.current || activeTurnId.current !== id) return;
            const buffered = displayBuffer.current;
            if (!buffered || buffered.turnId !== id) return;
            buffered.response += text;
            if (displayFrame.current !== null) return;
            displayFrame.current = scheduleFrame(() => {
              displayFrame.current = null;
              const latest = displayBuffer.current;
              if (mounted.current && activeTurnId.current === id && latest?.turnId === id) {
                updateTurn(id, (turn) => ({ ...turn, response: latest.response, status: "streaming" }));
              }
            });
          },
          onTerminal: (terminal) => {
            if (!mounted.current || activeTurnId.current !== id) return;
            if (displayFrame.current !== null) cancelScheduledFrame(displayFrame.current);
            displayFrame.current = null;
            const response = displayBuffer.current?.turnId === id ? displayBuffer.current.response : "";
            displayBuffer.current = null;
            activeTurnId.current = null;
            persistentHandle.current = null;
            persistentController.current = null;
            stopRequested.current = false;
            focusAfterTerminal.current = true;
            updateTurn(id, (turn) => terminalTurn({ ...turn, response: response || turn.response }, terminal));
            if (services.getChat && history) {
              const owned = ownHistoryAction();
              void services
                .readControlToken(endpoint)
                .then((token) => {
                  if (!mounted.current || owned.controller.signal.aborted) return undefined;
                  return services.getChat?.(endpoint, token, chatId, { signal: owned.controller.signal });
                })
                .then((summary) => {
                  if (!summary || !mounted.current || owned.controller.signal.aborted) return;
                  history.reconcileSummary(summary);
                })
                .catch(() => undefined)
                .finally(owned.finish);
            }
          },
        },
        controller.signal,
      );
      persistentHandle.current = stream;
      if (stopRequested.current) stream.cancel();
    } catch (reason) {
      if (!mounted.current || controller.signal.aborted) return;
      displayBuffer.current = null;
      activeTurnId.current = null;
      persistentHandle.current = null;
      persistentController.current = null;
      focusAfterTerminal.current = true;
      updateTurn(id, (turn) => ({ ...turn, status: "failed", error: message(reason) }));
    }
  };

  const send = () => {
    const content = input.trim();
    if (!canCompose || !requestModel || !activeModel || !content) return;
    if (services.createPersistentTurn && history) {
      void sendPersistent(content);
      return;
    }
    const id = nextTurnId.current++;
    activeTurnId.current = id;
    displayBuffer.current = { turnId: id, response: "" };
    stopRequested.current = false;
    setConnectionError("");
    setInput("");
    setTurns((current) => [
      ...current,
      {
        id,
        model: activeModel,
        prompt: content,
        response: "",
        status: "queued",
        error: "",
      },
    ]);
    try {
      const stream = services.createChatStream(
        endpoint,
        {
          model: requestModel,
          messages: [{ role: "user", content }],
        },
        {
          onDelta: (text) => {
            if (!mounted.current || activeTurnId.current !== id) return;
            const buffered = displayBuffer.current;
            if (buffered === null || buffered.turnId !== id) return;
            buffered.response += text;
            if (displayFrame.current !== null) return;
            displayFrame.current = scheduleFrame(() => {
              displayFrame.current = null;
              const latest = displayBuffer.current;
              if (!mounted.current || activeTurnId.current !== id || latest === null || latest.turnId !== id) return;
              updateTurn(id, (turn) => ({ ...turn, response: latest.response, status: "streaming" }));
            });
          },
          onTerminal: (terminal) => {
            if (!mounted.current || activeTurnId.current !== id) return;
            if (displayFrame.current !== null) {
              cancelScheduledFrame(displayFrame.current);
              displayFrame.current = null;
            }
            const bufferedResponse = displayBuffer.current?.turnId === id ? displayBuffer.current.response : null;
            displayBuffer.current = null;
            activeTurnId.current = null;
            handle.current = null;
            stopRequested.current = false;
            focusAfterTerminal.current = true;
            updateTurn(id, (turn) =>
              terminalTurn(bufferedResponse === null ? turn : { ...turn, response: bufferedResponse }, terminal),
            );
          },
        },
      );
      handle.current = stream;
    } catch (reason) {
      if (displayFrame.current !== null) {
        cancelScheduledFrame(displayFrame.current);
        displayFrame.current = null;
      }
      displayBuffer.current = null;
      activeTurnId.current = null;
      handle.current = null;
      focusAfterTerminal.current = true;
      updateTurn(id, (turn) => ({ ...turn, status: "failed", error: message(reason) }));
    }
  };

  const stop = () => {
    if (stopRequested.current || !responseInProgress) return;
    stopRequested.current = true;
    if (persistentHandle.current) persistentHandle.current.cancel();
    else handle.current?.cancel();
  };

  const switchModel = async () => {
    if (!selectedModel || selectedModel === activeModel || modelOperation !== "idle" || controlBusy) return;
    const controller = new AbortController();
    lifecycleController.current = controller;
    const close = () => controller.abort();
    window.addEventListener("beforeunload", close, { once: true });
    setModelOperation("switching");
    setControlBusy(true);
    setConnectionError("");
    let reconciledBusy = true;
    let publishReconciledBusy = false;
    try {
      const loadToken = await services.readControlToken(endpoint);
      const accepted = await services.loadModel(endpoint, loadToken, selectedModel, { signal: controller.signal });
      if (mounted.current && !controller.signal.aborted) setRequestModel(null);
      onModelMutationStart?.(accepted.operationId);
      let operationToken = await services.readControlToken(endpoint);
      let terminal = await services.getOperation(endpoint, operationToken, accepted.operationId, {
        signal: controller.signal,
      });
      while (terminal.status === "queued" || terminal.status === "running") {
        await delay(1_000, controller.signal);
        operationToken = await services.readControlToken(endpoint);
        terminal = await services.getOperation(endpoint, operationToken, accepted.operationId, {
          signal: controller.signal,
        });
      }
      operations.current.set(terminal.id, terminal);
      await onModelMutationSettled?.(terminal.id);
      if (terminal.status !== "succeeded") throw new Error(terminal.error || `Model switch ${terminal.status}.`);
      const nodeToken = await services.readControlToken(endpoint);
      const version = ++truthVersion.current;
      const [node, models] = await Promise.all([
        services.getControlNode(endpoint, nodeToken, { signal: controller.signal }),
        services.getModels(endpoint, { signal: controller.signal }),
      ]);
      if (version !== truthVersion.current) return;
      publishReconciledBusy = true;
      if (node.status !== "ready" || node.activeModelId !== selectedModel)
        throw new Error("The node did not confirm the selected model as ready.");
      const reconciledRequestModel = models.data.find(({ id }) => id === "loxa")?.id ?? models.data[0]?.id ?? null;
      if (reconciledRequestModel === null)
        throw new Error("The node did not publish a chat request model after loading.");
      reconciledBusy =
        node.operationId !== null ||
        [...operations.current.values()].some(
          (operation) => operation.status === "queued" || operation.status === "running",
        );
      if (mounted.current) {
        setActiveModel(node.activeModelId);
        setRequestModel(reconciledRequestModel);
        setSelectedModel(node.activeModelId);
        setConnectionError("");
        setConnection("ready");
      }
    } catch (reason) {
      if (mounted.current && !controller.signal.aborted) {
        setConnectionError(message(reason));
        try {
          const version = ++truthVersion.current;
          const [node, inventory] = await Promise.all([
            services
              .readControlToken(endpoint)
              .then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
            services
              .readControlToken(endpoint)
              .then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
          ]);
          if (version === truthVersion.current) {
            publishReconciledBusy = true;
            const eligible = inventory.filter(
              (entry) =>
                entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible,
            );
            setEligibleModels(eligible);
            if (node.status === "ready" && node.activeModelId !== null) {
              setActiveModel(node.activeModelId);
            } else {
              setActiveModel(null);
              setConnectionError(nodeUnavailableReason(node.status));
              setConnection("disconnected");
            }
            reconciledBusy =
              node.operationId !== null ||
              [...operations.current.values()].some(
                (operation) => operation.status === "queued" || operation.status === "running",
              );
          }
        } catch {
          reconciledBusy = true;
          publishReconciledBusy = true;
        }
      }
    } finally {
      window.removeEventListener("beforeunload", close);
      if (lifecycleController.current === controller) lifecycleController.current = null;
      if (mounted.current && !controller.signal.aborted) {
        setModelOperation("idle");
        if (publishReconciledBusy) setControlBusy(reconciledBusy);
      }
    }
  };

  const statusLabel = connectionLabel(
    connection,
    connectionError,
    chatCapability,
    controlStreamState,
    controlBusy,
    modelOperation,
    latestTurn,
  );
  const chatSupportReason =
    chatCapability === "unsupported"
      ? "Text chat is not supported by this node."
      : chatCapability === "unavailable"
        ? connectionError || "Text chat support could not be verified. Start or attach the node from Node first."
        : chatCapability === "checking"
          ? "Checking text chat support."
          : controlStreamState === "reconnecting"
            ? "Reconnecting to live model updates. Chat will unlock after a fresh node snapshot."
            : controlStreamState === "unavailable"
              ? "Live model updates are unavailable. Return to Node or reopen Chat to retry."
              : controlBusy
                ? "A model operation is in progress. Chat will unlock after the node confirms completion."
                : activeModel === null && connection === "ready"
                  ? "No active runtime model is available for chat."
                  : "";
  const emptyMessage = emptyChatMessage(
    connection,
    chatCapability,
    connectionError,
    activeModel,
    eligibleModels.length,
    modelOperation,
    controlStreamState,
    controlBusy,
  );
  const canBrowseModels =
    connection === "disconnected" &&
    connectionError === nodeUnavailableReason("unloaded") &&
    controlStreamState === "live" &&
    modelOperation === "idle" &&
    !controlBusy;

  return (
    <section className={styles.screen} aria-labelledby="chat-heading">
      <header className={styles.header}>
        <h1 id="chat-heading">Chat</h1>
        <div className={styles.headerStatus}>
          <p className={styles.liveStatus} role="status" aria-live="polite" aria-atomic="true">
            {statusLabel}
          </p>
          <p className={styles.activeModelStatus}>
            {activeModel === null ? "No active model" : `Active model: ${activeModel}`}
          </p>
        </div>
      </header>

      <div className={styles.chatMain}>
        <div className={styles.contextNotice}>
          {restoreError ||
            (omittedTurns > 0
              ? `${omittedTurns} earlier ${omittedTurns === 1 ? "turn was" : "turns were"} omitted from the model context.`
              : "")}
        </div>
        <ChatTranscript
          turns={turns}
          emptyMessage={emptyMessage}
          copyText={services.copyText}
          onBrowseModels={canBrowseModels ? onNavigateModels : undefined}
        />

        <ChatComposer
          input={input}
          inputRef={inputRef}
          canCompose={canCompose}
          responseInProgress={responseInProgress}
          supportReason={chatSupportReason}
          attachmentReason={attachmentReason}
          activeModel={activeModel}
          selectedModel={selectedModel}
          eligibleModels={eligibleModels}
          modelBusy={controlBusy}
          modelOperation={modelOperation}
          modelControlsAvailable={!availabilityBlocked}
          onInput={setInput}
          onSelectedModel={setSelectedModel}
          onSwitchModel={() => void switchModel()}
          onSend={send}
          onStop={stop}
        />
      </div>
    </section>
  );
}

function nodeSessionUnavailableReason(availability: ChatNodeAvailability): string {
  if (availability.phase === "checking" || availability.phase === "starting") {
    return "Starting the private Loxa node. Chat will be available after identity is proven.";
  }
  if (availability.phase === "stopping") return "The app-owned node is stopping. Chat is unavailable.";
  if (availability.phase === "recovery-required") return "Recovery required. Resolve the node before using chat.";
  if (availability.phase === "reconciling") return "Refreshing authoritative model status before enabling chat.";
  return availability.error || "The Loxa node is disconnected. Retry from Node to use chat.";
}

function terminalTurn(turn: ChatTurn, terminal: StreamTerminal): ChatTurn {
  if (terminal.kind === "error") return { ...turn, status: "failed", error: terminal.message };
  return { ...turn, status: terminal.kind, error: "" };
}

function connectionLabel(
  connection: ConnectionState,
  error: string,
  capability: CapabilityState,
  controlStreamState: ControlStreamState,
  controlBusy: boolean,
  operation: "idle" | "switching",
  latest?: ChatTurn,
): string {
  if (connection === "checking") return "Checking node";
  if (connection === "disconnected") return error || "Disconnected";
  if (error) return error;
  if (capability === "unsupported" || capability === "unavailable") return "Chat unavailable";
  if (controlStreamState === "reconnecting") return "Reconnecting to live model updates";
  if (controlStreamState === "unavailable") return "Live model updates unavailable";
  if (operation === "switching") return "Loading selected model";
  if (controlBusy) return "Model operation in progress";
  if (latest?.status === "failed") return latest.error;
  if (latest) return latest.status[0].toUpperCase() + latest.status.slice(1);
  return "Ready";
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function emptyChatMessage(
  connection: ConnectionState,
  capability: CapabilityState,
  error: string,
  activeModel: string | null,
  eligibleModelCount: number,
  operation: "idle" | "switching",
  controlStreamState: ControlStreamState,
  controlBusy: boolean,
): string {
  if (connection === "checking") return "Preparing the local chat session…";
  if (error) return error;
  if (capability === "checking") return "Checking text chat support…";
  if (capability === "unsupported") return "Text chat is not supported by this node.";
  if (capability === "unavailable") return "Text chat support could not be verified. Retry from Node.";
  if (controlStreamState === "reconnecting")
    return "Reconnecting to live model updates. Chat will unlock after a fresh node snapshot.";
  if (controlStreamState === "unavailable")
    return "Live model updates are unavailable. Return to Node or reopen Chat to retry.";
  if (operation === "switching")
    return "Loading the selected model. Chat will unlock after the node confirms readiness.";
  if (controlBusy) return "A model operation is in progress. Chat will unlock after the node confirms completion.";
  if (eligibleModelCount === 0) return "No downloaded compatible model is available. Download one from Models.";
  if (activeModel === null) return "No model is loaded. Choose a downloaded model below or open Models.";
  return "Start a new conversation or continue one from your local history.";
}

function isTerminalOperation(status: string): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

function isActiveLifecycleOperation(operation: { kind: string; status: string }): boolean {
  return (
    (operation.kind === "load" || operation.kind === "unload") &&
    (operation.status === "queued" || operation.status === "running")
  );
}

function isTerminalLifecycleOperation(operation: { kind: string; status: string }): boolean {
  return (operation.kind === "load" || operation.kind === "unload") && isTerminalOperation(operation.status);
}

function nodeUnavailableReason(status: NodeControlStatus): string {
  if (status === "recovery_required") return "Recovery required. Restart the node safely before using chat.";
  if (status === "unloaded") return "No model is loaded. Load a verified model from Models before using chat.";
  if (status === "loading") return "The node is loading a model. Chat will be available after readiness is confirmed.";
  if (status === "unloading") return "The node is unloading the active model. Chat is unavailable.";
  if (status === "error") return "The node reported an error. Resolve it from Node before using chat.";
  return "Chat is unavailable until the node confirms a ready model.";
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", abort, { once: true });
  });
}

function scheduleFrame(callback: FrameRequestCallback): number {
  if (typeof requestAnimationFrame === "function") return requestAnimationFrame(callback);
  return window.setTimeout(() => callback(performance.now()), 16);
}

function cancelScheduledFrame(frame: number): void {
  if (typeof cancelAnimationFrame === "function") cancelAnimationFrame(frame);
  else window.clearTimeout(frame);
}
