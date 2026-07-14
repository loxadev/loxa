import { act, createContext, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";

const roots = new Set<Root>();
export const BrowserPortalContext = createContext<HTMLElement | null>(null);

export function mountBrowser(ui: ReactNode) {
  const host = document.createElement("div");
  host.setAttribute("data-loxa-theme", "light");
  const appRoot = document.createElement("div");
  const portal = document.createElement("div");
  portal.id = "loxa-portal-root";
  host.append(appRoot, portal);
  document.body.append(host);
  const root = createRoot(appRoot);
  roots.add(root);
  act(() => root.render(<BrowserPortalContext.Provider value={portal}>{ui}</BrowserPortalContext.Provider>));
  return { host, appRoot, portal };
}

export function cleanupBrowser() {
  for (const root of roots) act(() => root.unmount());
  roots.clear();
  document.body.removeAttribute("style");
  document.body.removeAttribute("class");
  document.body.replaceChildren();
  document.documentElement.removeAttribute("style");
  document.documentElement.removeAttribute("class");
  document.documentElement.removeAttribute("data-loxa-theme");
  document.documentElement.removeAttribute("data-loxa-theme-preference");
}
