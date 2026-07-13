import { useState, type ReactNode } from "react";

import mark from "../assets/brand/loxa-mark.svg";
import { ChatScreen, type ChatScreenServices } from "../chat/ChatScreen";
import { ModelsScreen, type ModelsScreenServices } from "../models/ModelsScreen";
import { NodeScreen, type NodeScreenServices } from "../node/NodeScreen";
import {
  NodeSessionProvider,
  useNodeSession,
  type NodeSessionServices,
} from "../node/NodeSession";
import { SettingsScreen } from "../settings/SettingsScreen";
import { useThemePreference } from "../settings/theme";
import { appServices, DEFAULT_ENDPOINT } from "./services";

export type AppServices = NodeSessionServices & NodeScreenServices & ChatScreenServices & ModelsScreenServices;

type Route = "node" | "models" | "chat" | "settings";

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
  const session = useNodeSession();

  const navigate = (next: Route) => (event: React.MouseEvent) => {
    event.preventDefault();
    setRoute(next);
  };

  return (
    <div className="app-shell">
      <aside className="navigation-rail" aria-label="Primary">
        <div className="brand-lockup">
          <img src={mark} alt="" width="42" height="34" />
          <span>Loxa</span>
        </div>
        <nav>
          <a className="nav-link interactive-target" href="#node" aria-current={route === "node" ? "page" : undefined} onClick={navigate("node")}>Node</a>
          <a className="nav-link interactive-target" href="#models" aria-current={route === "models" ? "page" : undefined} onClick={navigate("models")}>Models</a>
          <a className="nav-link interactive-target" href="#chat" aria-current={route === "chat" ? "page" : undefined} onClick={navigate("chat")}>Chat</a>
          <a className="nav-link interactive-target" href="#settings" aria-current={route === "settings" ? "page" : undefined} onClick={navigate("settings")}>Settings</a>
        </nav>
      </aside>
      <main className="workspace">
        {route === "node" ? (
          <NodeScreen services={services} />
        ) : route === "models" ? (
          <NodeSessionGate heading="Models">
            <ModelsScreen services={services} endpoint={session.endpoint} />
          </NodeSessionGate>
        ) : route === "chat" ? (
          <NodeSessionGate heading="Chat">
            <ChatScreen services={services} endpoint={session.endpoint} />
          </NodeSessionGate>
        ) : (
          <SettingsScreen theme={theme} onThemeChange={setTheme} />
        )}
      </main>
    </div>
  );
}

function NodeSessionGate({ children, heading }: { children: ReactNode; heading: string }) {
  const session = useNodeSession();
  if (session.proven) return children;

  const waiting = session.phase === "checking" || session.phase === "starting";
  return (
    <section aria-labelledby="node-session-gate-heading">
      <header className="screen-header">
        <div><p className="eyebrow">Local runtime</p><h1 id="node-session-gate-heading">{heading}</h1></div>
      </header>
      <p role="status" aria-live="polite">
        {waiting
          ? "Starting the private Loxa node…"
          : session.error ?? "The Loxa node is stopped."}
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
