import { PanelLeftClose } from "lucide-react";
import { PanelLeftOpen } from "lucide-react";

import { IconButton } from "../components/ui/button";
import { selectSidebarCollapsed, selectToggleSidebar, useWorkspaceStore } from "../stores/workspace-store";

export function SidebarHeader({ brandMark }: { brandMark: string }) {
  const collapsed = useWorkspaceStore(selectSidebarCollapsed);
  const toggleSidebar = useWorkspaceStore(selectToggleSidebar);
  const label = collapsed ? "Expand sidebar" : "Collapse sidebar";

  return (
    <header className="sidebar-header">
      <div className="brand-lockup" aria-label="Loxa">
        <img src={brandMark} alt="" width="24" height="24" />
        <span className="sidebar-text">Loxa</span>
      </div>
      <span className="visually-hidden" id="sidebar-toggle-help">
        {label}
      </span>
      <IconButton
        className="sidebar-toggle"
        variant="quiet"
        label={label}
        helpId="sidebar-toggle-help"
        onClick={toggleSidebar}
      >
        {collapsed ? <PanelLeftOpen /> : <PanelLeftClose />}
      </IconButton>
    </header>
  );
}
