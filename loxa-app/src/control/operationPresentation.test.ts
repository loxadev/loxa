import { describe, expect, it } from "vitest";

import type { V2Operation } from "./contracts";
import { activeOperations, compactActiveOperations, operationLane } from "./operationPresentation";

type OperationOverrides = {
  operation_id?: string;
  kind?: V2Operation["kind"];
  status?: V2Operation["status"];
  created_revision?: string;
};

function operation(overrides: OperationOverrides = {}): V2Operation {
  return {
    operation_id: "123e4567-e89b-42d3-a456-426614174010",
    node_id: "123e4567-e89b-42d3-a456-426614174000",
    kind: "download",
    status: "running",
    slot_id: null,
    model_id: "model",
    progress: null,
    error: null,
    created_revision: "1",
    updated_revision: "1",
    created_at_unix_ms: "1",
    updated_at_unix_ms: "1",
    ...overrides,
  } as V2Operation;
}

describe("operation presentation", () => {
  it("classifies load and unload as lifecycle and download as download", () => {
    expect(operationLane(operation({ kind: "load" }))).toBe("lifecycle");
    expect(operationLane(operation({ kind: "unload" }))).toBe("lifecycle");
    expect(operationLane(operation({ kind: "download" }))).toBe("download");
  });

  it("returns only active operations in deterministic menu priority without mutating input", () => {
    const input = Object.freeze([
      operation({
        operation_id: "123e4567-e89b-42d3-a456-426614174016",
        kind: "download",
        status: "queued",
        created_revision: "2",
      }),
      operation({
        operation_id: "123e4567-e89b-42d3-a456-426614174015",
        kind: "load",
        status: "running",
        created_revision: "3",
      }),
      operation({
        operation_id: "123e4567-e89b-42d3-a456-426614174014",
        kind: "download",
        status: "cancelling",
        created_revision: "9",
      }),
      operation({
        operation_id: "123e4567-e89b-42d3-a456-426614174013",
        kind: "unload",
        status: "cancelling",
        created_revision: "8",
      }),
      operation({ status: "succeeded" }),
      operation({ status: "failed" }),
      operation({ status: "cancelled" }),
    ] satisfies readonly V2Operation[]);
    const canonicalOrder = input.map((candidate) => candidate.operation_id);

    const active = activeOperations(input);

    expect(active.map((candidate) => [candidate.kind, candidate.status])).toEqual([
      ["unload", "cancelling"],
      ["load", "running"],
      ["download", "cancelling"],
      ["download", "queued"],
    ]);
    expect(input.map((candidate) => candidate.operation_id)).toEqual(canonicalOrder);
    expect(active).not.toBe(input);
  });

  it("orders equal-priority rows by arbitrary-precision revision and then UUID", () => {
    const lowerUuid = operation({
      operation_id: "123e4567-e89b-42d3-a456-426614174011",
      created_revision: "18446744073709551615",
    });
    const higherUuid = operation({
      operation_id: "123e4567-e89b-42d3-a456-426614174012",
      created_revision: "18446744073709551615",
    });
    const earlierRevision = operation({
      operation_id: "123e4567-e89b-42d3-a456-426614174099",
      created_revision: "9007199254740993",
    });

    expect(activeOperations([higherUuid, lowerUuid, earlierRevision])).toEqual([
      earlierRevision,
      lowerUuid,
      higherUuid,
    ]);
  });

  it("returns a bounded default display with exact active and remaining counts", () => {
    const operations = Array.from({ length: 8 }, (_, index) =>
      operation({
        operation_id: `123e4567-e89b-42d3-a456-4266141740${String(index).padStart(2, "0")}`,
        created_revision: String(index + 1),
      }),
    );

    const compact = compactActiveOperations(operations);

    expect(compact.activeCount).toBe(8);
    expect(compact.displayed).toHaveLength(5);
    expect(compact.remaining).toBe(3);
    expect(compact.displayed).toEqual(activeOperations(operations).slice(0, 5));
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    "keeps invalid limit %s safely bounded at zero",
    (limit) => {
      const compact = compactActiveOperations([operation()], limit);

      expect(compact).toEqual({ activeCount: 1, displayed: [], remaining: 1 });
    },
  );
});
