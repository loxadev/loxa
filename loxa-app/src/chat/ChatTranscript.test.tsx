import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChatTranscript } from "./ChatTranscript";

describe("ChatTranscript empty state", () => {
  it("begins with a named 48px Loxa mark", () => {
    render(<ChatTranscript turns={[]} emptyMessage="Choose a model to begin." copyText={vi.fn()} />);

    const emptyState = screen.getByText("Choose a model to begin.").parentElement;
    const mark = screen.getByRole("img", { name: "Loxa" });

    expect(emptyState?.firstElementChild).toBe(mark);
    expect(mark).toHaveAttribute("width", "48");
    expect(mark).toHaveAttribute("height", "48");
  });
});
