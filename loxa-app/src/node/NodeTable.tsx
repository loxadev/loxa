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

export type NodeTableProps = {
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

export function NodeTable({
  nodeId,
  statusLabel,
  statusTone,
  activeModel,
  engineName,
  engineVersion,
  profile,
  endpoint,
  ownership,
  actions,
}: NodeTableProps) {
  const hasActions = actions ? Children.toArray(Object.values(actions)).length > 0 : false;

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
        <TableRow className={styles.nodeRow}>
          <TableCell>
            <strong className={styles.primaryValue}>Local node</strong>
            <span className={`${styles.detail} technical-value`}>{nodeId}</span>
          </TableCell>
          <TableCell>
            <StatusBadge tone={statusTone}>{statusLabel}</StatusBadge>
          </TableCell>
          <TableCell>
            <span className={`${styles.primaryValue} technical-value`}>{activeModel}</span>
          </TableCell>
          <TableCell>
            <span className="technical-value">{engineName}</span>
          </TableCell>
          <TableCell>
            <span className="technical-value">{engineVersion}</span>
          </TableCell>
          <TableCell>
            <span className="technical-value">{profile}</span>
          </TableCell>
          <TableCell>
            <span className={`${styles.endpoint} technical-value`}>{endpoint}</span>
          </TableCell>
          <TableCell>
            <span className={styles.primaryValue}>{ownership}</span>
          </TableCell>
          {hasActions && actions && (
            <TableCell>
              <div className={styles.actions}>
                {actions.copyEndpoint}
                {actions.model}
                {actions.start}
                {actions.retry}
                {actions.lifecycle}
              </div>
            </TableCell>
          )}
        </TableRow>
      </TableBody>
    </Table>
  );
}
