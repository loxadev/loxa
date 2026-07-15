import type { ModelInventoryEntry, NodeSnapshot, OperationView } from "../control/contracts";
import { Table, TableBody, TableCaption, TableHead, TableHeader, TableRow } from "../components/ui/table";
import { ModelDetail } from "./ModelDetail";
import { ModelRow } from "./ModelRow";
import styles from "./ModelsScreen.module.css";

type InstalledModelsProps = {
  models: ModelInventoryEntry[];
  selected: ModelInventoryEntry | undefined;
  node: NodeSnapshot | null;
  operations: Map<string, OperationView>;
  activeUnload?: OperationView;
  pendingModels: Set<string>;
  mutationBusy: boolean;
  onSelect(modelId: string): void;
  onDownload(modelId: string): void;
  onLoad(modelId: string): void;
  onUnload(modelId: string): void;
  onCancel(operation: OperationView, modelId: string): void;
};

export function InstalledModels(props: InstalledModelsProps) {
  return (
    <div className={styles.workspaceGrid}>
      <section className={styles.tablePanel} aria-label="Installed models">
        <Table className={styles.modelsTable}>
          <TableCaption className="visually-hidden">Installed and verified model recipes</TableCaption>
          <TableHeader>
            <TableRow>
              <TableHead scope="col">Model</TableHead>
              <TableHead scope="col">Format</TableHead>
              <TableHead scope="col">Size</TableHead>
              <TableHead scope="col">State</TableHead>
              <TableHead scope="col">
                <span className="visually-hidden">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {props.models.map((entry) => (
              <ModelRow
                key={entry.id}
                entry={entry}
                operation={props.operations.get(entry.id)}
                unloadOperation={props.node?.activeModelId === entry.id ? props.activeUnload : undefined}
                pending={props.pendingModels.has(entry.id)}
                active={props.node?.activeModelId === entry.id}
                selected={props.selected?.id === entry.id}
                node={props.node}
                mutationBusy={props.mutationBusy}
                onSelect={() => props.onSelect(entry.id)}
                onDownload={() => props.onDownload(entry.id)}
                onLoad={() => props.onLoad(entry.id)}
                onUnload={() => props.onUnload(entry.id)}
                onCancel={(operation) => props.onCancel(operation, entry.id)}
              />
            ))}
          </TableBody>
        </Table>
      </section>
      {props.selected && <ModelDetail entry={props.selected} />}
    </div>
  );
}
