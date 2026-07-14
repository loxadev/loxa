import { selectSetActiveRoute, useWorkspaceStore } from "../stores/workspace-store";

export function SidebarRuntimeStatus({ health, model }: { health: string; model: string }) {
  const setRoute = useWorkspaceStore(selectSetActiveRoute);

  return (
    <a
      className="global-node-status interactive-target"
      href="#node"
      aria-label={`${health}. ${model}`}
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
}
