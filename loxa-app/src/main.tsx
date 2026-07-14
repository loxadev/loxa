import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";
import App from "./App";
import { CspProbePanel } from "./app/CspProbePanel";

const cspProbeEnabled = import.meta.env.VITE_LOXA_CSP_PROBE === "1";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <>
      <App />
      {cspProbeEnabled && <CspProbePanel />}
    </>
  </React.StrictMode>,
);
