import { Children, type ReactNode } from "react";

import { StatusBadge, type StatusBadgeProps } from "../components/loxa/status-badge";
import { Table, TableBody, TableCaption, TableCell, TableHead, TableHeader, TableRow } from "../components/ui/table";
import styles from "./NodeTable.module.css";

export type NodeTableActions = {
  copyEndpoint: ReactNode;
  model?: ReactNode;
  start?: ReactNode;
  retry?: ReactNode;
  lifecycle?: ReactNode;
};

export type NodeTableRow = {
  rowId: string;
  name: string;
  kind: string;
  nodeId: string;
  statusLabel: string;
  statusTone: StatusBadgeProps["tone"];
  activeModel: string;
  engineName: string;
  engineVersion: string;
  profile: string;
  endpoint: string;
  ownership: string;
  actions?: NodeTableActions;
};

type NodeTableCollectionProps = {
  rows: readonly NodeTableRow[];
  selectedRowId?: string;
  onSelectRow?(rowId: string): void;
};

export type NodeTableProps = NodeTableRow | NodeTableCollectionProps;

export function NodeTable(props: NodeTableProps) {
  const rows = "rows" in props ? props.rows : [props];
  const selectedRowId = "rows" in props ? props.selectedRowId : undefined;
  const onSelectRow = "rows" in props ? props.onSelectRow : undefined;
  const hasActions = rows.some((row) => hasRenderableActions(row.actions));

  return (
    <Table className={styles.table}>
      <TableCaption className="visually-hidden">Local node inventory</TableCaption>
      <TableHeader>
        <TableRow className={styles.headerRow}>
          <TableHead scope="col">Node</TableHead>
          <TableHead scope="col">Status</TableHead>
          <TableHead scope="col">Active model</TableHead>
          <TableHead scope="col">Engine</TableHead>
          <TableHead scope="col">Version</TableHead>
          <TableHead scope="col">Profile</TableHead>
          <TableHead scope="col">Endpoint</TableHead>
          <TableHead scope="col">Ownership</TableHead>
          {hasActions && <TableHead scope="col">Actions</TableHead>}
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => {
          const selected = selectedRowId === row.rowId;
          return (
            <TableRow
              aria-selected={onSelectRow ? selected : undefined}
              className={styles.nodeRow}
              data-selected={onSelectRow && selected ? true : undefined}
              key={row.rowId}
            >
              <TableCell>
                {onSelectRow ? (
                  <button
                    className={styles.rowSelector}
                    type="button"
                    aria-label={`Select ${row.name}`}
                    aria-pressed={selected}
                    onClick={() => onSelectRow(row.rowId)}
                  >
                    <strong className={styles.primaryValue}>{row.name}</strong>
                    <span className={styles.detail}>{row.kind}</span>
                    <span className={`${styles.detail} technical-value`}>{row.nodeId}</span>
                  </button>
                ) : (
                  <>
                    <strong className={styles.primaryValue}>{row.name}</strong>
                    <span className={styles.detail}>{row.kind}</span>
                    <span className={`${styles.detail} technical-value`}>{row.nodeId}</span>
                  </>
                )}
              </TableCell>
              <TableCell>
                <StatusBadge tone={row.statusTone}>{row.statusLabel}</StatusBadge>
              </TableCell>
              <TableCell>
                <span className={`${styles.primaryValue} technical-value`}>{row.activeModel}</span>
              </TableCell>
              <TableCell>
                <span className="technical-value">{row.engineName}</span>
              </TableCell>
              <TableCell>
                <span className="technical-value">{row.engineVersion}</span>
              </TableCell>
              <TableCell>
                <span className="technical-value">{row.profile}</span>
              </TableCell>
              <TableCell>
                <span className={`${styles.endpoint} technical-value`}>{row.endpoint}</span>
              </TableCell>
              <TableCell>
                <span className={styles.primaryValue}>{row.ownership}</span>
              </TableCell>
              {hasActions && (
                <TableCell>
                  {hasRenderableActions(row.actions) && row.actions ? (
                    <div className={styles.actions}>
                      {row.actions.copyEndpoint}
                      {row.actions.model}
                      {row.actions.start}
                      {row.actions.retry}
                      {row.actions.lifecycle}
                    </div>
                  ) : null}
                </TableCell>
              )}
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function hasRenderableActions(actions?: NodeTableActions) {
  return actions ? Children.toArray(Object.values(actions)).length > 0 : false;
}
