import { useEffect, useRef, useState } from "react";

import type { getModels as defaultGetModels, getStatus as defaultGetStatus } from "../node/client";
import type { StreamCallbacks, StreamHandle } from "./streamChat";

export type ChatScreenServices = {
  getStatus: typeof defaultGetStatus;
  getModels: typeof defaultGetModels;
  createChatStream(endpoint: string, request: unknown, callbacks: StreamCallbacks): StreamHandle;
};

type ChatState = "checking" | "disconnected" | "ready" | "streaming" | "cancelled" | "completed" | "error";

export function ChatScreen({ services, endpoint }: { services: ChatScreenServices; endpoint: string }) {
  const [state, setState] = useState<ChatState>("checking");
  const [model, setModel] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [output, setOutput] = useState("");
  const [error, setError] = useState("");
  const handle = useRef<StreamHandle | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    const controller = new AbortController();
    void Promise.all([
      services.getStatus(endpoint, { signal: controller.signal }),
      services.getModels(endpoint, { signal: controller.signal }),
    ]).then(([status, models]) => {
      if (!mounted.current) return;
      if (status.health !== "ready") { setState("disconnected"); return; }
      setModel(models.data[0].id);
      setState("ready");
    }).catch((reason) => {
      if (!mounted.current || controller.signal.aborted) return;
      setError(reason instanceof Error ? reason.message : String(reason));
      setState("disconnected");
    });
    const beforeUnload = () => handle.current?.dispose();
    window.addEventListener("beforeunload", beforeUnload);
    return () => {
      mounted.current = false;
      controller.abort();
      handle.current?.dispose();
      window.removeEventListener("beforeunload", beforeUnload);
    };
  }, [endpoint, services]);

  const send = () => {
    const content = input.trim();
    if (state !== "ready" || !model || !content) return;
    setOutput("");
    setError("");
    setState("streaming");
    handle.current = services.createChatStream(endpoint, {
      model,
      messages: [{ role: "user", content }],
    }, {
      onDelta: (text) => { if (mounted.current) setOutput((current) => current + text); },
      onTerminal: (terminal) => {
        if (!mounted.current) return;
        if (terminal.kind === "error") { setError(terminal.message); setState("error"); }
        else setState(terminal.kind);
      },
    });
  };

  const label = state === "checking" ? "Checking node" : state === "error" ? error : state[0].toUpperCase() + state.slice(1);
  return (
    <section aria-labelledby="chat-heading">
      <header className="screen-header"><div><p className="eyebrow">Operational proof</p><h1 id="chat-heading">Chat</h1></div><p className="status-badge" role="status" aria-live="polite">{label}{state === "disconnected" && error ? `. ${error}` : ""}</p></header>
      <p className="model-line">Model alias <span className="technical-value">{model ?? "Unavailable"}</span></p>
      <div className="chat-output" aria-label="Assistant response" aria-live="polite">{output || "Responses appear here."}</div>
      <form className="composer" onSubmit={(event) => { event.preventDefault(); send(); }}>
        <label htmlFor="message">Message</label>
        <textarea id="message" value={input} onChange={(event) => setInput(event.target.value)} disabled={state === "disconnected" || state === "checking" || state === "streaming"} />
        <div className="action-row">
          <button className="primary-button interactive-target" type="submit" disabled={state !== "ready" || !input.trim()}>Send message</button>
          {state === "streaming" && <button className="secondary-button interactive-target" type="button" onClick={() => handle.current?.cancel()}>Cancel response</button>}
        </div>
      </form>
    </section>
  );
}
