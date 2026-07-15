import { ChevronDown, Cpu, HardDrive, Search } from "lucide-react";
import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import type { ModelInventoryEntry } from "../control/contracts";
import styles from "./ChatModelControl.module.css";

type ChatModelControlProps = {
  title?: string;
  headerActions?: ReactNode;
  endActions?: ReactNode;
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
  title = "New Chat",
  headerActions,
  endActions,
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
  const controlsEnabled = modelControlsAvailable && !modelBusy && !responseInProgress;
  const [pickerState, setPickerState] = useState({ open: false, controlsEnabled });
  if (pickerState.controlsEnabled !== controlsEnabled) {
    setPickerState({ open: controlsEnabled ? pickerState.open : false, controlsEnabled });
  }
  const open = pickerState.open && controlsEnabled;
  const setOpen = useCallback(
    (next: boolean | ((current: boolean) => boolean)) => {
      setPickerState((current) => ({
        controlsEnabled,
        open: typeof next === "function" ? next(current.open) : next,
      }));
    },
    [controlsEnabled],
  );
  const [query, setQuery] = useState("");
  const [selectionOverride, setSelectionOverride] = useState<string | null>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const pickerRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const optionRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const filteredModels = useMemo(() => filterModels(eligibleModels, query), [eligibleModels, query]);
  const pendingSelection =
    selectionOverride !== null && eligibleModels.some((model) => model.id === selectionOverride)
      ? selectionOverride
      : selectedModel;
  const switchingDisabled =
    !modelControlsAvailable || modelOperation === "switching" || modelBusy || responseInProgress;
  const selectionCanLoad = pendingSelection !== "" && pendingSelection !== activeModel;
  const normalizedQuery = query.trim();

  useEffect(() => {
    if (open) searchRef.current?.focus();
  }, [open]);
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "l") {
        if (!modelControlsAvailable || modelBusy || responseInProgress) return;
        event.preventDefault();
        setOpen(true);
      } else if (event.key === "Escape" && open) {
        event.preventDefault();
        setOpen(false);
        triggerRef.current?.focus();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [modelBusy, modelControlsAvailable, open, responseInProgress, setOpen]);
  useEffect(() => {
    if (!open) return;

    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (pickerRef.current?.contains(target) || triggerRef.current?.contains(target)) return;
      setOpen(false);
    };

    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, [open, setOpen]);

  const navigateOptions = (event: React.KeyboardEvent) => {
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return;
    if (filteredModels.length === 0) return;

    event.preventDefault();
    const focusedIndex = optionRefs.current.findIndex((option) => option === document.activeElement);
    const nextIndex =
      focusedIndex === -1
        ? event.key === "ArrowDown"
          ? 0
          : filteredModels.length - 1
        : (focusedIndex + (event.key === "ArrowDown" ? 1 : -1) + filteredModels.length) % filteredModels.length;
    optionRefs.current.forEach((option) => {
      if (option) option.tabIndex = -1;
    });
    const nextOption = optionRefs.current[nextIndex];
    if (nextOption) {
      nextOption.tabIndex = 0;
      nextOption.focus();
    }
  };

  const choose = (modelId: string) => {
    setSelectionOverride(modelId);
    onSelectedModel(modelId);
  };
  const switchModel = () => {
    if (!selectionCanLoad || switchingDisabled) return;
    onSwitchModel();
  };
  const browse = () => {
    setOpen(false);
    onBrowseModels?.();
  };

  return (
    <section className={styles.control} aria-label="Chat model">
      <div className={styles.triggerRow}>
        <div className={styles.titleGroup}>
          <h1 id="chat-heading" className={styles.title}>
            {title.trim() || "New Chat"}
          </h1>
          {headerActions}
        </div>
        <button
          ref={triggerRef}
          className={styles.modelTrigger}
          type="button"
          aria-label="Choose model"
          aria-haspopup="dialog"
          aria-expanded={open}
          aria-controls="chat-model-picker"
          disabled={!modelControlsAvailable || modelBusy || responseInProgress}
          onClick={() => setOpen((current) => !current)}
        >
          <Cpu aria-hidden="true" />
          <span>{activeModel ?? "Choose a model to load"}</span>
          <kbd>⌘L</kbd>
          <ChevronDown aria-hidden="true" />
          <span className={styles.visuallyHidden}>Active model: {activeModel ?? "None"}</span>
        </button>
        <div className={styles.endActions}>{endActions}</div>
      </div>

      {(status || guidance) && (
        <div className={styles.notice} role="status" aria-live="polite" aria-atomic="true">
          <span>{status}</span>
          {guidance && <span className={styles.guidance}>{guidance}</span>}
          {canBrowseModels && onBrowseModels && (
            <button type="button" onClick={browse}>
              Open Models
            </button>
          )}
        </div>
      )}

      {open && (
        <div
          ref={pickerRef}
          id="chat-model-picker"
          className={styles.picker}
          role="dialog"
          aria-label="Choose a model"
          onKeyDown={navigateOptions}
        >
          <label className={styles.searchField}>
            <span className={styles.visuallyHidden}>Search downloaded models</span>
            <Search aria-hidden="true" />
            <Input
              ref={searchRef}
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Type to filter models…"
              disabled={modelBusy || responseInProgress}
            />
          </label>
          <div className={styles.pickerHeading}>
            <span>On this Mac</span>
            <HardDrive aria-hidden="true" />
          </div>
          <div className={styles.modelList} role="listbox" aria-label="Downloaded models">
            {filteredModels.map((model, index) => (
              <button
                key={model.id}
                ref={(element) => {
                  optionRefs.current[index] = element;
                }}
                type="button"
                role="option"
                aria-label={model.id}
                aria-selected={pendingSelection === model.id}
                tabIndex={index === 0 ? 0 : -1}
                className={styles.modelOption}
                onClick={() => choose(model.id)}
              >
                <span>{model.id}</span>
                <span className={styles.modelMeta}>
                  {model.params} · {model.quant}
                </span>
              </button>
            ))}
          </div>
          {filteredModels.length === 0 && (
            <div className={styles.noResults}>
              <p>No downloaded models match “{normalizedQuery}”.</p>
              {onBrowseModels && normalizedQuery && (
                <Button variant="secondary" onClick={browse} aria-label={`Search Hugging Face for ${normalizedQuery}`}>
                  Search Hugging Face
                </Button>
              )}
            </div>
          )}
          <div className={styles.pickerFooter}>
            {onBrowseModels && (
              <Button
                variant="quiet"
                aria-label="Browse models"
                onClick={browse}
                disabled={modelBusy || responseInProgress}
              >
                Browse all models
              </Button>
            )}
            {selectionCanLoad && (
              <Button
                variant="primary"
                disabled={switchingDisabled}
                onClick={switchModel}
                aria-label={`${activeModel === null ? "Load" : "Switch to"} ${pendingSelection}`}
              >
                {modelOperation === "switching" ? "Loading…" : activeModel === null ? "Load" : "Switch"}
              </Button>
            )}
          </div>
        </div>
      )}
    </section>
  );
}

function filterModels(models: ModelInventoryEntry[], query: string): ModelInventoryEntry[] {
  const normalizedQuery = query.trim().toLocaleLowerCase();
  return normalizedQuery
    ? models.filter((model) =>
        [model.id, model.repo, model.params, model.quant].some((value) =>
          value.toLocaleLowerCase().includes(normalizedQuery),
        ),
      )
    : models;
}
