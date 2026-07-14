import { cspProbeStore } from "./cspProbeStore";
import { installConsoleCountProbe } from "./consoleCountProbe";

const probeEnabled = import.meta.env.VITE_LOXA_CSP_PROBE === "1";

if (probeEnabled) {
  installConsoleCountProbe(console);
  window.addEventListener("securitypolicyviolation", (event) => cspProbeStore.recordViolation(event));

  if (import.meta.env.VITE_LOXA_CSP_PROBE_CASE === "early-blocked-image") {
    const policy = document.createElement("meta");
    policy.httpEquiv = "Content-Security-Policy";
    policy.content = "img-src 'none'";
    document.head.prepend(policy);

    const image = new Image();
    image.hidden = true;
    image.alt = "";
    image.src = "https://csp-probe.invalid/early-blocked-image?probe-secret=discard";
    document.documentElement.append(image);
  }
}
