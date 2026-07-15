import { Boxes, Search } from "lucide-react";
import { useMemo, useState } from "react";

import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import type { ModelInventoryEntry } from "../control/contracts";
import styles from "./ChatModelControl.module.css";

type ChatModelControlProps = {
  activeModel: string | null;
  selectedModel: string;
  eligibleModels: ModelInventoryEntry[];
  status: string;
  guidance: string;
  modelBusy: boolean;
  modelOperation: "idle" | "switching";
  modelControlsAvailable: boolean;
  responseInProgress: boolean;
  canBrowseModels: boolean;
  onSelectedModel(value: string): void;
  onSwitchModel(): void;
  onBrowseModels?(): void;
};

export function ChatModelControl({
  activeModel,
  selectedModel,
  eligibleModels,
  status,
  guidance,
  modelBusy,
  modelOperation,
  modelControlsAvailable,
  responseInProgress,
  canBrowseModels,
  onSelectedModel,
  onSwitchModel,
  onBrowseModels,
}: ChatModelControlProps) {
  const [query, setQuery] = useState("");
  const filteredModels = useMemo(() => filterModels(eligibleModels, query), [eligibleModels, query]);
  const switchingDisabled =
    !modelControlsAvailable || modelOperation === "switching" || modelBusy || responseInProgress;
  const selectionCanLoad = selectedModel !== "" && selectedModel !== activeModel;
  const showCatalogFallback = query.trim() !== "" && filteredModels.length === 0 && onBrowseModels !== undefined;
  const updateQuery = (value: string) => {
    setQuery(value);
    const matches = filterModels(eligibleModels, value);
    if (!matches.some((model) => model.id === selectedModel)) onSelectedModel(matches[0]?.id ?? "");
  };

  return (
    <section className={styles.control} aria-label="Chat model">
      <div className={styles.summary}>
        <div className={styles.modelIdentity}>
          <Boxes aria-hidden="true" focusable="false" />
          <div>
            <p className={styles.label}>Model</p>
            <p className={styles.activeModel}>
              {activeModel === null ? "No active model" : `Active model: ${activeModel}`}
            </p>
          </div>
        </div>
        <div className={styles.statusCopy}>
          <p className={styles.status} role="status" aria-live="polite" aria-atomic="true">
            {status}
          </p>
          {guidance ? <p className={styles.guidance}>{guidance}</p> : null}
        </div>
      </div>

      <div className={styles.actions}>
        <label className={styles.searchField}>
          <span className={styles.visuallyHidden}>Search downloaded models</span>
          <Search aria-hidden="true" focusable="false" />
          <Input
            type="search"
            value={query}
            onChange={(event) => updateQuery(event.target.value)}
            placeholder="Search downloaded models"
            disabled={!modelControlsAvailable || modelBusy || responseInProgress}
          />
        </label>
        <label className={styles.modelField}>
          <span className={styles.visuallyHidden}>Choose model</span>
          <select
            className={styles.modelPicker}
            value={selectedModel}
            disabled={switchingDisabled}
            onChange={(event) => onSelectedModel(event.target.value)}
          >
            <option value="">No active model</option>
            {filteredModels.map((model) => (
              <option key={model.id} value={model.id}>
                {model.id}
              </option>
            ))}
          </select>
        </label>
        {selectionCanLoad ? (
          <Button
            variant="secondary"
            disabled={switchingDisabled}
            onClick={onSwitchModel}
            aria-label={`${activeModel === null ? "Load" : "Switch to"} ${selectedModel}`}
          >
            {modelOperation === "switching" ? "Loading…" : activeModel === null ? "Load" : "Switch"}
          </Button>
        ) : null}
        {(canBrowseModels || showCatalogFallback) && onBrowseModels ? (
          <Button variant="secondary" onClick={onBrowseModels} disabled={modelBusy || responseInProgress}>
            Browse models
          </Button>
        ) : null}
      </div>
    </section>
  );
}

function filterModels(models: ModelInventoryEntry[], query: string): ModelInventoryEntry[] {
  const normalizedQuery = query.trim().toLocaleLowerCase();
  return normalizedQuery ? models.filter((model) => model.id.toLocaleLowerCase().includes(normalizedQuery)) : models;
}
