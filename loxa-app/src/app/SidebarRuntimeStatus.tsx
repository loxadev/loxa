import { selectSetActiveRoute, useWorkspaceStore } from "../stores/workspace-store";
import { Tooltip } from "../components/ui/tooltip";

export function SidebarRuntimeStatus({ health, model }: { health: string; model: string }) {
  const setRoute = useWorkspaceStore(selectSetActiveRoute);
  const collapsed = useWorkspaceStore((state) => state.sidebarCollapsed);
  const label = `${health}. ${model}`;

  const status = (
    <a
      className="global-node-status interactive-target"
      href="#node"
      aria-label={label}
      aria-live="polite"
      onClick={(event) => {
        event.preventDefault();
        setRoute("node");
      }}
    >
      <span className="runtime-indicator" aria-hidden="true" />
      <span className="sidebar-text runtime-status-copy">
        <span className="global-node-status-label">{health}</span>
        <span className="global-node-status-model">{model}</span>
      </span>
    </a>
  );

  return collapsed ? <Tooltip content={label}>{status}</Tooltip> : status;
}
