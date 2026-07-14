import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Table, TableBody, TableCaption, TableCell, TableHead, TableHeader, TableRow } from "./table";

describe("Table", () => {
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
    expect(container.querySelector("[data-slot='table-container']")).toContainElement(table);
    expect(within(table).getAllByRole("columnheader")).toHaveLength(2);
    expect(within(table).getAllByRole("row")).toHaveLength(2);
    expect(within(table).getByText("Local node").tagName).toBe("TD");
  });
});
