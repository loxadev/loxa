import { useEffect, useRef, useState } from "react";

import type { getCapabilities as defaultGetCapabilities } from "../control/client";
import type { getModels as defaultGetModels, getStatus as defaultGetStatus } from "../node/client";
import type { StreamCallbacks, StreamHandle, StreamTerminal } from "./streamChat";

export type ChatScreenServices = {
  getStatus: typeof defaultGetStatus;
  getModels: typeof defaultGetModels;
  readControlToken(endpoint: string): Promise<string>;
  getCapabilities: typeof defaultGetCapabilities;
  createChatStream(endpoint: string, request: unknown, callbacks: StreamCallbacks): StreamHandle;
};

type ConnectionState = "checking" | "disconnected" | "ready";
type CapabilityState = "checking" | "supported" | "unsupported" | "unavailable";
type ChatTurnStatus = "queued" | "streaming" | "completed" | "cancelled" | "failed";

type ChatTurn = {
  id: number;
  model: string;
  prompt: string;
  response: string;
  status: ChatTurnStatus;
  error: string;
};

export function ChatScreen({ services, endpoint }: { services: ChatScreenServices; endpoint: string }) {
  const [connection, setConnection] = useState<ConnectionState>("checking");
  const [requestModel, setRequestModel] = useState<string | null>(null);
  const [activeModel, setActiveModel] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [connectionError, setConnectionError] = useState("");
  const [chatCapability, setChatCapability] = useState<CapabilityState>("checking");
  const [attachmentReason, setAttachmentReason] = useState("Checking document input support.");
  const handle = useRef<StreamHandle | null>(null);
  const activeTurnId = useRef<number | null>(null);
  const nextTurnId = useRef(1);
  const mounted = useRef(true);

  useEffect(() => {
    const controller = new AbortController();
    let disposed = false;
    mounted.current = true;

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
      setActiveModel(status.runtime_model);
      setConnection("ready");
    }).catch((reason: unknown) => {
      if (disposed || controller.signal.aborted) return;
      setConnectionError(message(reason));
      setConnection("disconnected");
    });

    void (async () => {
      const token = await services.readControlToken(endpoint);
      if (disposed) return;
      const capabilities = await services.getCapabilities(endpoint, token, { signal: controller.signal });
      if (disposed) return;
      setChatCapability(capabilities.textChat ? "supported" : "unsupported");
      setAttachmentReason(capabilities.documentInput
        ? "Document input transport is not available in this desktop build."
        : capabilities.documentInputReason);
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
    requestModel !== null && activeModel !== null && !responseInProgress;

  const updateTurn = (id: number, update: (current: ChatTurn) => ChatTurn) => {
    setTurns((current) => current.map((turn) => turn.id === id ? update(turn) : turn));
  };

  const send = () => {
    const content = input.trim();
    if (!canCompose || !requestModel || !activeModel || !content) return;
    const id = nextTurnId.current++;
    activeTurnId.current = id;
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
          updateTurn(id, (turn) => terminalTurn(turn, terminal));
        },
      });
      handle.current = stream;
    } catch (reason) {
      activeTurnId.current = null;
      handle.current = null;
      updateTurn(id, (turn) => ({ ...turn, status: "failed", error: message(reason) }));
    }
  };

  const statusLabel = connectionLabel(connection, connectionError, chatCapability, latestTurn);
  const chatSupportReason = chatCapability === "unsupported"
    ? "Text chat is not supported by this node."
    : chatCapability === "unavailable"
      ? "Text chat support could not be verified. Start or attach the node from Node first."
      : chatCapability === "checking"
        ? "Checking text chat support."
        : activeModel === null && connection === "ready"
          ? "No active runtime model is available for chat."
          : "";

  return (
    <section className="chat-screen" aria-labelledby="chat-heading">
      <header className="screen-header">
        <div><p className="eyebrow">Operational tool</p><h1 id="chat-heading">Chat</h1></div>
        <p className="status-badge" role="status" aria-live="polite">{statusLabel}</p>
      </header>

      <p className="model-line">Public API model alias <span className="technical-value">{requestModel ?? "Unavailable"}</span></p>

      <div className="chat-output" aria-label="Conversation" aria-live="polite">
        {turns.length === 0 ? <p className="empty-state">Responses appear here.</p> : turns.map((turn) => (
          <article className="chat-turn" key={turn.id} aria-label={`Chat turn using ${turn.model}`}>
            <div className="chat-message chat-message-user"><p className="message-label">You</p><p>{turn.prompt}</p></div>
            <div className="chat-message chat-message-assistant">
              <div className="message-heading"><p className="message-label">Loxa</p><span className="technical-value">{turn.model}</span></div>
              <p>{turn.response || (turn.status === "queued" || turn.status === "streaming" ? "Waiting for the model…" : "No response was returned.")}</p>
              <p className={`turn-state turn-${turn.status}`}>{turnStateLabel(turn.status)}{turn.error ? ` — ${turn.error}` : ""}</p>
            </div>
          </article>
        ))}
      </div>

      <form className="composer" onSubmit={(event) => { event.preventDefault(); send(); }}>
        <label htmlFor="message">Message</label>
        <textarea
          id="message"
          value={input}
          onChange={(event) => setInput(event.target.value)}
          disabled={!canCompose}
          aria-describedby={chatSupportReason ? "chat-support-reason" : undefined}
        />
        {chatSupportReason && <p id="chat-support-reason" className="composer-reason">{chatSupportReason}</p>}
        <div className="composer-footer">
          <div className="composer-tools">
            <button
              className="quiet-button interactive-target attachment-button"
              type="button"
              aria-label="Attach document"
              aria-describedby="attachment-support-reason"
              disabled
            >+</button>
            <div className="composer-model-control">
              <label htmlFor="active-chat-model">Active model</label>
              <select
                id="active-chat-model"
                value={activeModel ?? ""}
                disabled
                aria-describedby="model-control-reason"
              >
                <option value="">No active model</option>
                {activeModel && <option value={activeModel}>{activeModel}</option>}
              </select>
              <span id="model-control-reason" className="attachment-reason">
                Model load and switch controls are not available yet; use Models for verified downloads.
              </span>
            </div>
          </div>
          <div className="action-row">
            <button className="primary-button interactive-target" type="submit" disabled={!canCompose || !input.trim()}>Send message</button>
            {responseInProgress && <button className="secondary-button interactive-target" type="button" onClick={() => handle.current?.cancel()}>Cancel response</button>}
          </div>
        </div>
        <p id="attachment-support-reason" className="attachment-reason">{attachmentReason}</p>
      </form>
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
  latest?: ChatTurn,
): string {
  if (connection === "checking") return "Checking node";
  if (connection === "disconnected") return error ? `Disconnected. ${error}` : "Disconnected";
  if (capability === "unsupported" || capability === "unavailable") return "Chat unavailable";
  if (latest?.status === "failed") return latest.error;
  if (latest) return latest.status[0].toUpperCase() + latest.status.slice(1);
  return "Ready";
}

function turnStateLabel(status: ChatTurnStatus): string {
  if (status === "failed") return "Turn failed";
  return `Turn ${status}`;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
