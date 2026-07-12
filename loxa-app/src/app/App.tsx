import { useState } from "react";

import mark from "../assets/brand/loxa-mark.svg";
import { ChatScreen, type ChatScreenServices } from "../chat/ChatScreen";
import { ModelsScreen, type ModelsScreenServices } from "../models/ModelsScreen";
import { NodeScreen, type NodeScreenServices } from "../node/NodeScreen";
import { SettingsScreen } from "../settings/SettingsScreen";
import { useThemePreference } from "../settings/theme";
import { appServices, DEFAULT_ENDPOINT } from "./services";

export type AppServices = NodeScreenServices & ChatScreenServices & ModelsScreenServices;

type Route = "node" | "models" | "chat" | "settings";

export function App({ services = appServices }: { services?: AppServices }) {
  const [route, setRoute] = useState<Route>("node");
  const [endpoint, setEndpoint] = useState(DEFAULT_ENDPOINT);
  const [theme, setTheme] = useThemePreference();

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
          <NodeScreen services={services} onEndpointChange={setEndpoint} />
        ) : route === "models" ? (
          <ModelsScreen services={services} endpoint={endpoint} />
        ) : route === "chat" ? (
          <ChatScreen services={services} endpoint={endpoint} />
        ) : (
          <SettingsScreen theme={theme} onThemeChange={setTheme} />
        )}
      </main>
    </div>
  );
}

export default App;
