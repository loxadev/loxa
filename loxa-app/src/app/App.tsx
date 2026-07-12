import { useState } from "react";

import mark from "../assets/brand/loxa-mark.svg";
import { ChatScreen, type ChatScreenServices } from "../chat/ChatScreen";
import { NodeScreen, type NodeScreenServices } from "../node/NodeScreen";
import { appServices, DEFAULT_ENDPOINT } from "./services";

export type AppServices = NodeScreenServices & ChatScreenServices;

export function App({ services = appServices }: { services?: AppServices }) {
  const [route, setRoute] = useState<"node" | "chat">("node");
  const [endpoint, setEndpoint] = useState(DEFAULT_ENDPOINT);

  const navigate = (next: "node" | "chat") => (event: React.MouseEvent) => {
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
          <a className="nav-link interactive-target" href="#chat" aria-current={route === "chat" ? "page" : undefined} onClick={navigate("chat")}>Chat</a>
        </nav>
      </aside>
      <main className="workspace">
        {route === "node" ? (
          <NodeScreen services={services} onEndpointChange={setEndpoint} />
        ) : (
          <ChatScreen services={services} endpoint={endpoint} />
        )}
      </main>
    </div>
  );
}

export default App;
