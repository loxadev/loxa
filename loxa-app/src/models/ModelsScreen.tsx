import { useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { Search } from "lucide-react";

import { Input } from "../components/ui/input";
import type { NodeSnapshot } from "../control/contracts";
import type { CatalogModel } from "./catalog";
import { DiscoverModels } from "./DiscoverModels";
import { InstalledModels } from "./InstalledModels";
import styles from "./ModelsScreen.module.css";
import {
  useModelsController,
  type ModelsLiveState,
  type ModelsScreenServices as ModelsServices,
} from "./useModelsController";

export type { ModelsScreenServices } from "./useModelsController";

type WorkspaceTab = "installed" | "discover";

export function ModelsScreen({
  endpoint,
  services,
  reconnectDelayMs = 1_000,
  verificationPollMs = 2_000,
  verificationPollLimit = 6,
  reconnectLimit = 6,
  onModelMutationStart,
  onModelMutationSettled,
  catalogModels = null,
}: {
  endpoint: string;
  services: ModelsServices;
  reconnectDelayMs?: number;
  verificationPollMs?: number;
  verificationPollLimit?: number;
  reconnectLimit?: number;
  onModelMutationStart?: (operationId: string) => void;
  onModelMutationSettled?: (operationId: string) => void | Promise<void>;
  catalogModels?: readonly CatalogModel[] | null;
}) {
  const controller = useModelsController({
    endpoint,
    services,
    reconnectDelayMs,
    verificationPollMs,
    verificationPollLimit,
    reconnectLimit,
    onModelMutationStart,
    onModelMutationSettled,
  });
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedModelId, setSelectedModelId] = useState<string | null>(null);
  const [workspaceTab, setWorkspaceTab] = useState<WorkspaceTab>("installed");
  const installedTabRef = useRef<HTMLButtonElement>(null);
  const discoverTabRef = useRef<HTMLButtonElement>(null);
  const normalizedSearchQuery = searchQuery.trim().toLowerCase();
  const visibleModels = useMemo(() => {
    if (normalizedSearchQuery === "") return controller.models;
    return controller.models.filter((entry) =>
      [entry.id, entry.repo, entry.engine.engine, entry.params, entry.quant, entry.license].some((value) =>
        value.toLowerCase().includes(normalizedSearchQuery),
      ),
    );
  }, [controller.models, normalizedSearchQuery]);
  const selectedModel = visibleModels.find((entry) => entry.id === selectedModelId) ?? visibleModels[0];

  const focusWorkspaceTab = (tab: WorkspaceTab) => {
    setWorkspaceTab(tab);
    (tab === "installed" ? installedTabRef : discoverTabRef).current?.focus();
  };

  const handleWorkspaceTabKeyDown = (event: ReactKeyboardEvent<HTMLButtonElement>, current: WorkspaceTab) => {
    let next: WorkspaceTab | undefined;
    if (event.key === "Home") next = "installed";
    else if (event.key === "End") next = "discover";
    else if (event.key === "ArrowRight") next = current === "installed" ? "discover" : "installed";
    else if (event.key === "ArrowLeft") next = current === "installed" ? "discover" : "installed";
    if (next === undefined) return;
    event.preventDefault();
    focusWorkspaceTab(next);
  };

  return (
    <section className={styles.screen} aria-labelledby="models-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Verified registry</p>
          <h1 id="models-heading">Models</h1>
          <p className="screen-summary">Download Loxa-tested recipes. Downloading never loads or switches a model.</p>
        </div>
        <p className={`status-badge live-${controller.liveState}`} role="status" aria-live="polite">
          {liveLabel(controller.liveState)}
        </p>
      </header>

      <div className={styles.toolbar} aria-label="Model control summary">
        <span>
          Node <strong>{controller.node === null ? "Checking" : nodeStatusLabel(controller.node)}</strong>
        </span>
        <span>
          Endpoint <span className="technical-value">{endpoint}</span>
        </span>
        <span>
          {normalizedSearchQuery === ""
            ? `${controller.models.length} verified recipes`
            : `${visibleModels.length} of ${controller.models.length} verified recipes`}
        </span>
      </div>

      <div className={styles.tabs} role="tablist" aria-label="Model workspace">
        <button
          ref={installedTabRef}
          id="installed-tab"
          type="button"
          role="tab"
          aria-controls="installed-panel"
          aria-selected={workspaceTab === "installed"}
          tabIndex={workspaceTab === "installed" ? 0 : -1}
          className={workspaceTab === "installed" ? styles.activeTab : styles.tab}
          onClick={() => setWorkspaceTab("installed")}
          onKeyDown={(event) => handleWorkspaceTabKeyDown(event, "installed")}
        >
          Installed
        </button>
        <button
          ref={discoverTabRef}
          id="discover-tab"
          type="button"
          role="tab"
          aria-controls="discover-panel"
          aria-selected={workspaceTab === "discover"}
          tabIndex={workspaceTab === "discover" ? 0 : -1}
          className={workspaceTab === "discover" ? styles.activeTab : styles.tab}
          onClick={() => setWorkspaceTab("discover")}
          onKeyDown={(event) => handleWorkspaceTabKeyDown(event, "discover")}
        >
          Discover
        </button>
      </div>

      {workspaceTab === "installed" && (
        <div className={styles.searchControl}>
          <Search aria-hidden="true" size={16} strokeWidth={1.8} />
          <Input
            type="search"
            aria-label="Search models"
            placeholder="Search models"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.currentTarget.value)}
          />
        </div>
      )}

      {controller.error && (
        <p className={styles.panel} role="alert">
          {controller.error}
        </p>
      )}
      {controller.node?.status === "recovery_required" && (
        <p className={styles.panel} role="alert">
          Recovery required. Model and chat controls are blocked until the node is safely restarted.
        </p>
      )}
      {controller.liveState === "error" && (
        <button className="secondary-button interactive-target" type="button" onClick={controller.retry}>
          Retry live updates
        </button>
      )}
      {workspaceTab === "installed" && !controller.error && controller.models.length === 0 && (
        <p className={styles.empty}>
          {controller.inventoryLoaded
            ? "No verified recipes are available in this build."
            : "Checking the known model registry…"}
        </p>
      )}
      {workspaceTab === "installed" &&
        !controller.error &&
        controller.inventoryLoaded &&
        controller.models.length > 0 &&
        visibleModels.length === 0 &&
        normalizedSearchQuery !== "" && <p className={styles.empty}>No models match “{searchQuery.trim()}”.</p>}
      {workspaceTab === "installed" && visibleModels.length > 0 && (
        <div id="installed-panel" role="tabpanel" aria-labelledby="installed-tab">
          <InstalledModels
            models={visibleModels}
            selected={selectedModel}
            node={controller.node}
            operations={controller.latestByModel}
            activeUnload={controller.activeUnload}
            pendingModels={controller.pendingModels}
            downloadingModelIds={controller.downloadingModelIds}
            globallyClosed={controller.globallyClosed}
            lifecycleBusy={controller.lifecycleBusy}
            onSelect={setSelectedModelId}
            onDownload={(modelId) => void controller.download(modelId)}
            onLoad={(modelId) => void controller.startLifecycle("load", modelId)}
            onUnload={(modelId) => void controller.startLifecycle("unload", modelId)}
            onCancel={(operation, modelId) => void controller.cancel(operation, modelId)}
          />
        </div>
      )}
      {workspaceTab === "discover" && (
        <div id="discover-panel" role="tabpanel" aria-labelledby="discover-tab">
          <DiscoverModels
            catalogModels={catalogModels}
            inventory={controller.models}
            operations={controller.latestByModel}
            pendingModels={controller.pendingModels}
            downloadingModelIds={controller.downloadingModelIds}
            globallyClosed={controller.globallyClosed}
            onDownload={(modelId) => void controller.download(modelId)}
            onCancel={(operation, modelId) => void controller.cancel(operation, modelId)}
          />
        </div>
      )}
      <p className="visually-hidden" aria-live="polite">
        {controller.notice}
      </p>
    </section>
  );
}

function nodeStatusLabel(node: NodeSnapshot): string {
  if (node.status === "recovery_required") return "Recovery required";
  return node.status[0].toUpperCase() + node.status.slice(1);
}

function liveLabel(state: ModelsLiveState): string {
  if (state === "live") return "Live updates connected";
  if (state === "reconnecting") return "Reconnecting";
  if (state === "error") return "Controls unavailable";
  return "Connecting";
}
