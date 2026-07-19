import type { V2Operation } from "./contracts";

export type OperationLane = "lifecycle" | "download";

export type CompactActiveOperations = {
  activeCount: number;
  displayed: V2Operation[];
  remaining: number;
};

const ACTIVE_STATUSES = new Set<V2Operation["status"]>(["queued", "running", "cancelling"]);

export function operationLane(operation: V2Operation): OperationLane {
  return operation.kind === "download" ? "download" : "lifecycle";
}

function statusPriority(status: V2Operation["status"]): number {
  if (status === "cancelling") return 0;
  if (status === "running") return 1;
  if (status === "queued") return 2;
  return 3;
}

function compareActiveOperations(left: V2Operation, right: V2Operation): number {
  const laneDifference = Number(operationLane(left) === "download") - Number(operationLane(right) === "download");
  if (laneDifference !== 0) return laneDifference;

  const statusDifference = statusPriority(left.status) - statusPriority(right.status);
  if (statusDifference !== 0) return statusDifference;

  const revisionDifference = BigInt(left.created_revision) - BigInt(right.created_revision);
  if (revisionDifference < 0n) return -1;
  if (revisionDifference > 0n) return 1;

  if (left.operation_id < right.operation_id) return -1;
  if (left.operation_id > right.operation_id) return 1;
  return 0;
}

export function activeOperations(operations: readonly V2Operation[]): V2Operation[] {
  return operations.filter((operation) => ACTIVE_STATUSES.has(operation.status)).sort(compareActiveOperations);
}

export function compactActiveOperations(operations: readonly V2Operation[], limit = 5): CompactActiveOperations {
  const active = activeOperations(operations);
  const boundedLimit = Number.isSafeInteger(limit) && limit > 0 ? limit : 0;
  const displayed = active.slice(0, boundedLimit);
  return {
    activeCount: active.length,
    displayed,
    remaining: active.length - displayed.length,
  };
}
