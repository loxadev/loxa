import { useEffect, useRef, useState, type CSSProperties, type KeyboardEvent, type ReactNode } from "react";

import mark from "../assets/brand/loxa-mark.svg?no-inline";
import { ChatScreen, type ChatScreenServices } from "../chat/ChatScreen";
import { ModelsScreen, type ModelsScreenServices } from "../models/ModelsScreen";
import { NodeScreen, type NodeScreenServices } from "../node/NodeScreen";
import { NodeSessionProvider, useNodeSession, type NodeSessionServices } from "../node/NodeSession";
import { SettingsScreen } from "../settings/SettingsScreen";
import { useThemePreference } from "../settings/theme";
import { appServices, DEFAULT_ENDPOINT } from "./services";

export type AppServices = NodeSessionServices & NodeScreenServices & ChatScreenServices & ModelsScreenServices;

type Route = "node" | "models" | "chat" | "settings";
const MIN_RAIL_WIDTH = 220;
const MAX_RAIL_WIDTH = 420;
const DEFAULT_RAIL_WIDTH = 280;
const RAIL_KEY_STEP = 20;

export function App({ services = appServices }: { services?: AppServices }) {
  return (
    <NodeSessionProvider services={services} endpoint={DEFAULT_ENDPOINT}>
      <AppWorkspace services={services} />
    </NodeSessionProvider>
  );
}

function AppWorkspace({ services }: { services: AppServices }) {
  const [route, setRoute] = useState<Route>("node");
  const [theme, setTheme] = useThemePreference();
  const [railWidth, setRailWidth] = useState(DEFAULT_RAIL_WIDTH);
  const [conversationRailTarget, setConversationRailTarget] = useState<HTMLDivElement | null>(null);
  const resizeHandle = useRef<HTMLDivElement | null>(null);
  const resizeSession = useRef<{ pointerId: number; startX: number; startWidth: number } | null>(null);
  const session = useNodeSession();

  useEffect(() => {
    const handle = resizeHandle.current;
    const move = (event: PointerEvent) => {
      const active = resizeSession.current;
      if (!active || event.pointerId !== active.pointerId) return;
      setRailWidth(clampRailWidth(active.startWidth + event.clientX - active.startX));
    };
    const finish = (event: PointerEvent) => {
      const active = resizeSession.current;
      if (!active || event.pointerId !== active.pointerId) return;
      if (handle?.hasPointerCapture?.(active.pointerId)) {
        handle.releasePointerCapture(active.pointerId);
      }
      resizeSession.current = null;
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", finish);
    window.addEventListener("pointercancel", finish);
    return () => {
      const active = resizeSession.current;
      if (active && handle?.hasPointerCapture?.(active.pointerId)) {
        handle.releasePointerCapture(active.pointerId);
      }
      resizeSession.current = null;
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", finish);
      window.removeEventListener("pointercancel", finish);
    };
  }, []);

  const navigate = (next: Route) => (event: React.MouseEvent) => {
    event.preventDefault();
    setRoute(next);
  };

  const resizeWithKeyboard = (event: KeyboardEvent<HTMLDivElement>) => {
    let next: number | null = null;
    if (event.key === "ArrowLeft") next = railWidth - RAIL_KEY_STEP;
    else if (event.key === "ArrowRight") next = railWidth + RAIL_KEY_STEP;
    else if (event.key === "Home") next = MIN_RAIL_WIDTH;
    else if (event.key === "End") next = MAX_RAIL_WIDTH;
    if (next === null) return;
    event.preventDefault();
    setRailWidth(clampRailWidth(next));
  };

  return (
    <div className="app-shell" style={{ "--loxa-rail-width": `${railWidth}px` } as CSSProperties}>
      <aside className="navigation-rail" aria-label="Primary">
        <div className="brand-lockup">
          <img src={mark} alt="" width="42" height="34" />
          <span>Loxa</span>
        </div>
        <nav className="navigation-primary-nav" aria-label="Primary node navigation">
          <div className="navigation-primary" role="group" aria-label="Node control">
            <a
              className="nav-link interactive-target"
              href="#node"
              aria-current={route === "node" ? "page" : undefined}
              onClick={navigate("node")}
            >
              Node
            </a>
            <a
              className="nav-link interactive-target"
              href="#models"
              aria-current={route === "models" ? "page" : undefined}
              onClick={navigate("models")}
            >
              Models
            </a>
          </div>
        </nav>
        {route === "chat" ? <div className="conversation-rail-slot" ref={setConversationRailTarget} /> : null}
        <nav className="navigation-secondary-nav" aria-label="Operational navigation">
          <div className="navigation-secondary" role="group" aria-label="Operational tools">
            <GlobalNodeStatus onNavigate={navigate("node")} />
            <a
              className="nav-link interactive-target"
              href="#chat"
              aria-current={route === "chat" ? "page" : undefined}
              onClick={navigate("chat")}
            >
              Chat
            </a>
            <a
              className="nav-link interactive-target"
              href="#settings"
              aria-current={route === "settings" ? "page" : undefined}
              onClick={navigate("settings")}
            >
              Settings
            </a>
          </div>
        </nav>
        <div
          ref={resizeHandle}
          className="rail-resize-handle"
          role="separator"
          aria-label="Resize navigation and conversation rail"
          aria-orientation="vertical"
          aria-valuemin={MIN_RAIL_WIDTH}
          aria-valuemax={MAX_RAIL_WIDTH}
          aria-valuenow={railWidth}
          tabIndex={0}
          onKeyDown={resizeWithKeyboard}
          onPointerDown={(event) => {
            if (event.button !== 0) return;
            resizeSession.current = { pointerId: event.pointerId, startX: event.clientX, startWidth: railWidth };
            event.currentTarget.setPointerCapture?.(event.pointerId);
            event.preventDefault();
          }}
        />
      </aside>
      <main className="workspace workspace-canvas">
        <div className="workspace-frame">
          {route === "node" ? (
            <NodeScreen services={services} onNavigateModels={() => setRoute("models")} />
          ) : route === "models" ? (
            <NodeSessionGate heading="Models">
              <ModelsScreen
                services={services}
                endpoint={session.endpoint}
                onModelMutationStart={session.invalidateModelTruth}
                onModelMutationSettled={session.settleModelMutation}
              />
            </NodeSessionGate>
          ) : route === "chat" ? (
            <ChatScreen
              services={services}
              endpoint={session.endpoint}
              nodeAvailability={{
                phase: session.phase,
                proven: session.proven,
                error: session.error,
              }}
              onModelMutationStart={session.invalidateModelTruth}
              onModelMutationSettled={session.settleModelMutation}
              conversationRailTarget={conversationRailTarget}
            />
          ) : (
            <SettingsScreen
              theme={theme}
              onThemeChange={setTheme}
              onClearChatHistory={
                services.clearChats
                  ? async (signal) => {
                      const token = await services.readControlToken(session.endpoint);
                      if (signal.aborted) throw new DOMException("aborted", "AbortError");
                      const result = await services.clearChats?.(session.endpoint, token, { signal });
                      return result?.deleted ?? 0;
                    }
                  : undefined
              }
              runtime={{
                phase: session.phase,
                endpoint: session.endpoint,
                ownership: session.ownership,
                status: session.status,
              }}
            />
          )}
        </div>
      </main>
    </div>
  );
}

function clampRailWidth(width: number) {
  return Math.min(MAX_RAIL_WIDTH, Math.max(MIN_RAIL_WIDTH, Math.round(width)));
}

function GlobalNodeStatus({ onNavigate }: { onNavigate: (event: React.MouseEvent) => void }) {
  const session = useNodeSession();
  const health = globalHealthLabel(session.phase);
  const model =
    session.phase === "ready" && session.status?.runtime_model
      ? `Active model ${session.status.runtime_model}`
      : session.phase === "unloaded"
        ? "No active model"
        : "Model status unavailable";

  return (
    <a
      className="global-node-status interactive-target"
      href="#node"
      aria-label={`${health}. ${model}`}
      aria-live="polite"
      onClick={onNavigate}
    >
      <span className="global-node-status-label">{health}</span>
      <span className="global-node-status-model">{model}</span>
    </a>
  );
}

function globalHealthLabel(phase: ReturnType<typeof useNodeSession>["phase"]) {
  if (phase === "checking") return "Checking node";
  if (phase === "starting") return "Starting node";
  if (phase === "unloaded") return "Node online";
  if (phase === "ready") return "Node ready";
  if (phase === "reconciling") return "Updating node";
  if (phase === "stopping") return "Stopping node";
  if (phase === "recovery-required") return "Recovery required";
  if (phase === "error") return "Node error";
  return "Node disconnected";
}

function NodeSessionGate({ children, heading }: { children: ReactNode; heading: string }) {
  const session = useNodeSession();
  if (session.proven) return children;

  const waiting = session.phase === "checking" || session.phase === "starting";
  return (
    <section aria-labelledby="node-session-gate-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Local runtime</p>
          <h1 id="node-session-gate-heading">{heading}</h1>
        </div>
      </header>
      <p role="status" aria-live="polite">
        {waiting ? "Starting the private Loxa node…" : (session.error ?? "The Loxa node is stopped.")}
      </p>
      {!waiting && (
        <button className="primary-button interactive-target" type="button" onClick={() => void session.retry()}>
          Retry node startup
        </button>
      )}
    </section>
  );
}

export default App;
