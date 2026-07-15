import { Boxes } from "lucide-react";
import { MessageSquare } from "lucide-react";
import { Server } from "lucide-react";
import { Settings } from "lucide-react";
import type { ComponentType, MouseEvent, SVGProps } from "react";

import { Tooltip } from "../components/ui/tooltip";

import {
  selectActiveRoute,
  selectSetActiveSettingsPage,
  selectSetActiveRoute,
  useWorkspaceStore,
  type WorkspaceRoute,
} from "../stores/workspace-store";

type Icon = ComponentType<SVGProps<SVGSVGElement>>;
const primaryItems: Array<{ route: WorkspaceRoute; label: string; Icon: Icon }> = [
  { route: "chat", label: "Chat", Icon: MessageSquare },
  { route: "models", label: "Models", Icon: Boxes },
  { route: "node", label: "Node", Icon: Server },
];

export function SidebarNavigation({ footer = false }: { footer?: boolean }) {
  const route = useWorkspaceStore(selectActiveRoute);
  const collapsed = useWorkspaceStore((state) => state.sidebarCollapsed);
  const setRoute = useWorkspaceStore(selectSetActiveRoute);
  const setSettingsPage = useWorkspaceStore(selectSetActiveSettingsPage);
  const items = footer ? [{ route: "settings" as const, label: "Settings", Icon: Settings }] : primaryItems;
  const navigate = (next: WorkspaceRoute) => (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    if (next === "settings") setSettingsPage("overview");
    setRoute(next);
  };

  return (
    <nav aria-label={footer ? "Application settings" : "Primary navigation"} className="sidebar-navigation">
      {items.map(({ route: itemRoute, label, Icon }) => {
        const link = (
          <a
            key={itemRoute}
            className="nav-link interactive-target"
            href={`#${itemRoute}`}
            aria-label={label}
            aria-current={route === itemRoute ? "page" : undefined}
            onClick={navigate(itemRoute)}
          >
            <Icon aria-hidden="true" focusable="false" />
            <span className="sidebar-text">{label}</span>
          </a>
        );
        return collapsed ? (
          <Tooltip key={itemRoute} content={label}>
            {link}
          </Tooltip>
        ) : (
          link
        );
      })}
    </nav>
  );
}
