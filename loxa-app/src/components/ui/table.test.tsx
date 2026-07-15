import { act, render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { Table, TableBody, TableCaption, TableCell, TableHead, TableHeader, TableRow } from "./table";

describe("Table", () => {
  let notifyResize: ResizeObserverCallback;

  beforeEach(() => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        constructor(callback: ResizeObserverCallback) {
          notifyResize = callback;
        }
        observe() {}
        unobserve() {}
        disconnect() {}
      },
    );
  });

  it("owns horizontal overflow as a last-resort container behavior", () => {
    const source = readFileSync(resolve(process.cwd(), "src/components/ui/table.tsx"), "utf8");
    expect(source).toContain('data-slot="table-container"');
    expect(source).toContain("overflow-x-auto");
  });

  it("renders source-owned semantic table elements inside an overflow container", () => {
    const { container } = render(
      <Table>
        <TableCaption>Local node inventory</TableCaption>
        <TableHeader>
          <TableRow>
            <TableHead>Node</TableHead>
            <TableHead>Status</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow>
            <TableCell>Local node</TableCell>
            <TableCell>Ready</TableCell>
          </TableRow>
        </TableBody>
      </Table>,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(table.tagName).toBe("TABLE");
    const overflowContainer = container.querySelector("[data-slot='table-container']");
    expect(overflowContainer).toContainElement(table);
    expect(overflowContainer).not.toHaveAttribute("tabindex");
    expect(within(table).getAllByRole("columnheader")).toHaveLength(2);
    expect(within(table).getAllByRole("row")).toHaveLength(2);
    expect(within(table).getByText("Local node").tagName).toBe("TD");
  });

  it("becomes keyboard-focusable only while its content overflows horizontally", () => {
    const { container } = render(
      <Table>
        <TableBody>
          <TableRow>
            <TableCell>Wide content</TableCell>
          </TableRow>
        </TableBody>
      </Table>,
    );
    const overflowContainer = container.querySelector("[data-slot='table-container']");
    Object.defineProperties(overflowContainer, {
      clientWidth: { configurable: true, value: 200 },
      scrollWidth: { configurable: true, value: 400 },
    });

    act(() => notifyResize([], {} as ResizeObserver));
    expect(overflowContainer).toHaveAttribute("tabindex", "0");
  });
});
