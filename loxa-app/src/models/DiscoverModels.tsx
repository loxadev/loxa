import { Download, Search } from "lucide-react";
import { useMemo, useState } from "react";

import type { ModelInventoryEntry, OperationView } from "../control/contracts";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import type { CatalogModel } from "./catalog";
import { formatBytes } from "./modelRowLabels";
import styles from "./ModelsScreen.module.css";

export function DiscoverModels({
  catalogModels,
  inventory,
  operations,
  pendingModels,
  mutationBusy,
  onDownload,
  onCancel,
}: {
  catalogModels: readonly CatalogModel[] | null;
  inventory: readonly ModelInventoryEntry[];
  operations: ReadonlyMap<string, OperationView>;
  pendingModels: ReadonlySet<string>;
  mutationBusy: boolean;
  onDownload(modelId: string): void;
  onCancel(operation: OperationView, modelId: string): void;
}) {
  const [query, setQuery] = useState("");
  const normalized = query.trim().toLowerCase();
  const visible = useMemo(
    () =>
      catalogModels?.filter((entry) =>
        [entry.title, entry.publisher, entry.summary, ...entry.tags].some((value) =>
          value.toLowerCase().includes(normalized),
        ),
      ) ?? [],
    [catalogModels, normalized],
  );

  if (catalogModels === null) {
    return (
      <section className={styles.catalogEmpty} aria-labelledby="catalog-unavailable-heading">
        <Download aria-hidden="true" size={22} />
        <h2 id="catalog-unavailable-heading">Model catalog unavailable</h2>
        <p>
          No catalog source is connected. Installed recipes remain available without showing invented catalog results.
        </p>
      </section>
    );
  }

  return (
    <section className={styles.discover} aria-label="Discover models">
      <div className={styles.searchControl}>
        <Search aria-hidden="true" size={16} strokeWidth={1.8} />
        <Input
          type="search"
          aria-label="Search catalog"
          placeholder="Search the model catalog"
          value={query}
          onChange={(event) => setQuery(event.currentTarget.value)}
        />
      </div>
      {catalogModels.length === 0 ? (
        <div className={styles.catalogEmpty}>
          <h2>Catalog is empty</h2>
          <p>The connected source returned no models.</p>
        </div>
      ) : visible.length === 0 ? (
        <div className={styles.catalogEmpty}>
          <h2>No catalog matches</h2>
          <p>No models match “{query.trim()}”.</p>
        </div>
      ) : (
        <div className={styles.catalogGrid}>
          {visible.map((entry) => {
            const recipe = inventory.find((candidate) => candidate.id === entry.modelId);
            const operation = operations.get(entry.modelId);
            const inProgress =
              operation?.kind === "download" && (operation.status === "queued" || operation.status === "running");
            const canDownload =
              recipe !== undefined &&
              recipe.artifact.kind !== "downloaded" &&
              recipe.compatibility.compatible &&
              recipe.engine.eligible;
            return (
              <article key={entry.modelId} className={styles.catalogCard}>
                <div className={styles.catalogCardHeading}>
                  <div>
                    <p>{entry.publisher}</p>
                    <h2>{entry.title}</h2>
                  </div>
                  {recipe?.artifact.kind === "downloaded" && <Badge variant="success">Installed</Badge>}
                </div>
                <p className={styles.catalogSummary}>{entry.summary}</p>
                <div className={styles.catalogTags}>
                  {entry.tags.map((tag) => (
                    <Badge key={tag}>{tag}</Badge>
                  ))}
                </div>
                {inProgress && (
                  <div className={styles.catalogProgress}>
                    <progress
                      aria-label={`Download progress for ${entry.title}`}
                      value={operation?.progress?.totalBytes === null ? undefined : operation?.progress?.completedBytes}
                      max={operation?.progress?.totalBytes ?? undefined}
                    />
                    <span>
                      {operation?.progress === null
                        ? "Preparing download"
                        : `${formatBytes(operation.progress.completedBytes)}${
                            operation.progress.totalBytes === null
                              ? " downloaded"
                              : ` of ${formatBytes(operation.progress.totalBytes)}`
                          }`}
                    </span>
                  </div>
                )}
                <div className={styles.catalogAction}>
                  {inProgress && operation ? (
                    <Button
                      variant="secondary"
                      onClick={() => onCancel(operation, entry.modelId)}
                      disabled={pendingModels.has(entry.modelId)}
                    >
                      Cancel
                    </Button>
                  ) : canDownload ? (
                    <Button
                      onClick={() => onDownload(entry.modelId)}
                      disabled={pendingModels.has(entry.modelId) || mutationBusy}
                      aria-label={`Download ${entry.title}`}
                    >
                      <Download aria-hidden="true" size={14} /> Download
                    </Button>
                  ) : recipe === undefined ? (
                    <span>Catalog details only</span>
                  ) : recipe.artifact.kind === "downloaded" ? (
                    <span>Available in Installed</span>
                  ) : (
                    <span>Unavailable for this Mac</span>
                  )}
                </div>
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}
