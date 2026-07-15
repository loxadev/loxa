import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ObservabilityInspector } from "./ObservabilityInspector";

describe("ObservabilityInspector", () => {
  it("shows truthful runtime facts and marks unsupported live metrics unavailable", () => {
    render(<ObservabilityInspector health="Node ready" model="gemma" onClose={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "Observability" })).toBeInTheDocument();
    expect(screen.getByText("gemma")).toBeInTheDocument();
    expect(screen.getAllByText("Unavailable").length).toBeGreaterThanOrEqual(3);
    expect(screen.queryByText(/browser preview/i)).not.toBeInTheDocument();
  });
});
