import { useEffect, useRef, useState } from "react";

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
import { ChatComposer } from "./ChatComposer";
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
};

type ConnectionState = "checking" | "disconnected" | "ready";
type CapabilityState = "checking" | "supported" | "unsupported" | "unavailable";
type ControlStreamState = "live" | "reconnecting" | "unavailable";
export function ChatScreen({ services, endpoint }: { services: ChatScreenServices; endpoint: string }) {
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
  const [connectionError, setConnectionError] = useState("");
  const [chatCapability, setChatCapability] = useState<CapabilityState>("checking");
  const [attachmentReason, setAttachmentReason] = useState("Checking document input support.");
  const handle = useRef<StreamHandle | null>(null);
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

  useEffect(() => {
    const controller = new AbortController();
    let disposed = false;
    mounted.current = true;
    recoveryRequired.current = false;

    void Promise.all([
      services.getStatus(endpoint, { signal: controller.signal }),
      services.getModels(endpoint, { signal: controller.signal }),
    ]).then(([status, models]) => {
      if (disposed) return;
      if (status.health !== "ready") {
        setConnection("disconnected");
        return;
      }
      setRequestModel(models.data[0].id);
      setConnection(recoveryRequired.current ? "disconnected" : "ready");
    }).catch((reason: unknown) => {
      if (disposed || controller.signal.aborted) return;
      setConnectionError(message(reason));
      setConnection("disconnected");
    });

    void (async () => {
      if (disposed) return;
      const [capabilities, inventory, controlNode] = await Promise.all([
        services.readControlToken(endpoint).then((token) => services.getCapabilities(endpoint, token, { signal: controller.signal })),
        services.readControlToken(endpoint).then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
        services.readControlToken(endpoint).then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
      ]);
      if (disposed) return;
      setChatCapability(capabilities.textChat ? "supported" : "unsupported");
      setAttachmentReason(capabilities.documentInput
        ? "Document input transport is not available in this desktop build."
        : capabilities.documentInputReason);
      const eligible = inventory.filter((entry) => entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible);
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
            services.readControlToken(endpoint).then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
            services.readControlToken(endpoint).then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
          ]);
          if (disposed) return;
          const eligibleNext = nextInventory.filter((entry) => entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible);
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
        controlStream.current = services.createControlEventStream(endpoint, streamToken, cursor, {
        onSnapshot: (snapshot) => {
          if (disposed) return;
          operations.current = new Map(snapshot.operations.map((operation) => [operation.id, operation]));
          setControlStreamState("live");
          setControlBusy(snapshot.operations.some((operation) => operation.status === "queued" || operation.status === "running"));
        },
        onEvent: (event) => {
          if (disposed) return;
          setControlStreamState("live");
          operations.current.set(event.operation.id, event.operation);
          setControlBusy([...operations.current.values()].some((operation) => operation.status === "queued" || operation.status === "running"));
          if ((event.operation.kind === "load" || event.operation.kind === "unload") && isTerminalOperation(event.operation.status)) {
            setControlBusy(true);
            const version = ++truthVersion.current;
            void Promise.all([
              services.readControlToken(endpoint).then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
              services.readControlToken(endpoint).then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
            ]).then(([node, nextInventory]) => {
              if (disposed || version !== truthVersion.current) return;
              const eligibleNext = nextInventory.filter((entry) => entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible);
              setEligibleModels(eligibleNext);
              setControlBusy(node.operationId !== null || [...operations.current.values()].some((operation) => operation.status === "queued" || operation.status === "running"));
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
            }).catch(() => {
              if (!disposed && version === truthVersion.current) setControlBusy(true);
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
      }, controller.signal);
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
      lifecycleController.current?.abort();
      lifecycleController.current = null;
      controlStream.current?.dispose();
      controlStream.current = null;
      activeTurnId.current = null;
      handle.current?.dispose();
      handle.current = null;
    };
    window.addEventListener("beforeunload", disposeWork);
    return () => {
      window.removeEventListener("beforeunload", disposeWork);
      disposeWork();
    };
  }, [endpoint, services]);

  const latestTurn = turns[turns.length - 1];
  const responseInProgress = latestTurn?.status === "queued" || latestTurn?.status === "streaming";
  const canCompose = connection === "ready" && chatCapability === "supported" &&
    requestModel !== null && activeModel !== null && !responseInProgress && modelOperation === "idle" && !controlBusy;

  useEffect(() => {
    if (!focusAfterTerminal.current || responseInProgress) return;
    focusAfterTerminal.current = false;
    inputRef.current?.focus();
  }, [latestTurn?.status, responseInProgress]);

  const updateTurn = (id: number, update: (current: ChatTurn) => ChatTurn) => {
    setTurns((current) => current.map((turn) => turn.id === id ? update(turn) : turn));
  };

  const send = () => {
    const content = input.trim();
    if (!canCompose || !requestModel || !activeModel || !content) return;
    const id = nextTurnId.current++;
    activeTurnId.current = id;
    stopRequested.current = false;
    setConnectionError("");
    setInput("");
    setTurns((current) => [...current, {
      id,
      model: activeModel,
      prompt: content,
      response: "",
      status: "queued",
      error: "",
    }]);
    try {
      const stream = services.createChatStream(endpoint, {
        model: requestModel,
        messages: [{ role: "user", content }],
      }, {
        onDelta: (text) => {
          if (!mounted.current || activeTurnId.current !== id) return;
          updateTurn(id, (turn) => ({ ...turn, response: turn.response + text, status: "streaming" }));
        },
        onTerminal: (terminal) => {
          if (!mounted.current || activeTurnId.current !== id) return;
          activeTurnId.current = null;
          handle.current = null;
          stopRequested.current = false;
          focusAfterTerminal.current = true;
          updateTurn(id, (turn) => terminalTurn(turn, terminal));
        },
      });
      handle.current = stream;
    } catch (reason) {
      activeTurnId.current = null;
      handle.current = null;
      focusAfterTerminal.current = true;
      updateTurn(id, (turn) => ({ ...turn, status: "failed", error: message(reason) }));
    }
  };

  const stop = () => {
    if (stopRequested.current || !responseInProgress) return;
    stopRequested.current = true;
    handle.current?.cancel();
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
      let operationToken = await services.readControlToken(endpoint);
      let terminal = await services.getOperation(endpoint, operationToken, accepted.operationId, { signal: controller.signal });
      while (terminal.status === "queued" || terminal.status === "running") {
        await delay(1_000, controller.signal);
        operationToken = await services.readControlToken(endpoint);
        terminal = await services.getOperation(endpoint, operationToken, accepted.operationId, { signal: controller.signal });
      }
      operations.current.set(terminal.id, terminal);
      if (terminal.status !== "succeeded") throw new Error(terminal.error || `Model switch ${terminal.status}.`);
      const nodeToken = await services.readControlToken(endpoint);
      const version = ++truthVersion.current;
      const node = await services.getControlNode(endpoint, nodeToken, { signal: controller.signal });
      if (version !== truthVersion.current) return;
      publishReconciledBusy = true;
      if (node.status !== "ready" || node.activeModelId !== selectedModel) throw new Error("The node did not confirm the selected model as ready.");
      reconciledBusy = node.operationId !== null || [...operations.current.values()].some((operation) => operation.status === "queued" || operation.status === "running");
      if (mounted.current) {
        setActiveModel(node.activeModelId);
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
            services.readControlToken(endpoint).then((token) => services.getControlNode(endpoint, token, { signal: controller.signal })),
            services.readControlToken(endpoint).then((token) => services.getInventory(endpoint, token, { signal: controller.signal })),
          ]);
          if (version === truthVersion.current) {
            publishReconciledBusy = true;
            const eligible = inventory.filter((entry) => entry.artifact.kind === "downloaded" && entry.compatibility.compatible && entry.engine.eligible);
            setEligibleModels(eligible);
            if (node.status === "ready" && node.activeModelId !== null) {
              setActiveModel(node.activeModelId);
            } else {
              setActiveModel(null);
              setConnectionError(nodeUnavailableReason(node.status));
              setConnection("disconnected");
            }
            reconciledBusy = node.operationId !== null || [...operations.current.values()].some((operation) => operation.status === "queued" || operation.status === "running");
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

  const statusLabel = connectionLabel(connection, connectionError, chatCapability, controlStreamState, controlBusy, modelOperation, latestTurn);
  const chatSupportReason = chatCapability === "unsupported"
    ? "Text chat is not supported by this node."
    : chatCapability === "unavailable"
      ? "Text chat support could not be verified. Start or attach the node from Node first."
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
  const emptyMessage = emptyChatMessage(connection, chatCapability, connectionError, activeModel, eligibleModels.length, modelOperation, controlStreamState, controlBusy);

  return (
    <section className={styles.screen} aria-labelledby="chat-heading">
      <header className="screen-header">
        <div><p className="eyebrow">Operational tool</p><h1 id="chat-heading">Chat</h1></div>
        <p className="status-badge" role="status" aria-live="polite">{statusLabel}</p>
      </header>

      <ChatTranscript turns={turns} emptyMessage={emptyMessage} />

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
        onInput={setInput}
        onSelectedModel={setSelectedModel}
        onSwitchModel={() => void switchModel()}
        onSend={send}
        onStop={stop}
      />
    </section>
  );
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
  if (controlStreamState === "reconnecting") return "Reconnecting to live model updates. Chat will unlock after a fresh node snapshot.";
  if (controlStreamState === "unavailable") return "Live model updates are unavailable. Return to Node or reopen Chat to retry.";
  if (operation === "switching") return "Loading the selected model. Chat will unlock after the node confirms readiness.";
  if (controlBusy) return "A model operation is in progress. Chat will unlock after the node confirms completion.";
  if (eligibleModelCount === 0) return "No downloaded compatible model is available. Download one from Models.";
  if (activeModel === null) return "No model is loaded. Choose a downloaded model below or open Models.";
  return "Ask the active local model. Responses stay in this session only.";
}

function isTerminalOperation(status: string): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
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
