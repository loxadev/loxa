import { Activity, ChevronDown } from "lucide-react";
import type { Ref } from "react";

import { Button } from "../components/ui/button";
import type { WorkspaceRoute } from "../stores/workspace-store";

export function WorkspaceToolbar({
  route,
  health,
  inspectorOpen,
  onToggleInspector,
  triggerRef,
}: {
  route: WorkspaceRoute;
  health: string;
  inspectorOpen: boolean;
  onToggleInspector(): void;
  triggerRef?: Ref<HTMLButtonElement>;
}) {
  return (
    <div className="workspace-toolbar">
      <strong>{routeTitle(route)}</strong>
      <WorkspaceHealthTrigger
        health={health}
        inspectorOpen={inspectorOpen}
        onToggleInspector={onToggleInspector}
        triggerRef={triggerRef}
      />
    </div>
  );
}

export function WorkspaceHealthTrigger({
  health,
  inspectorOpen,
  onToggleInspector,
  triggerRef,
}: {
  health: string;
  inspectorOpen: boolean;
  onToggleInspector(): void;
  triggerRef?: Ref<HTMLButtonElement>;
}) {
  return (
    <Button
      ref={triggerRef}
      className="workspace-health-trigger"
      variant="quiet"
      aria-expanded={inspectorOpen}
      aria-controls="observability-inspector"
      onClick={onToggleInspector}
    >
      <Activity aria-hidden="true" />
      <span>{health}</span>
      <ChevronDown aria-hidden="true" />
    </Button>
  );
}

function routeTitle(route: WorkspaceRoute) {
  if (route === "chat") return "Chat";
  if (route === "models") return "Models";
  if (route === "node") return "Nodes";
  return "Settings";
}
