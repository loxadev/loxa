import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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

  it("replaces completed-state prose with icon copy and truthful optional metrics", async () => {
    const user = userEvent.setup();
    const copyText = vi.fn().mockResolvedValue(undefined);
    render(
      <ChatTranscript
        emptyMessage=""
        copyText={copyText}
        turns={[
          {
            id: "turn-1",
            model: "gemma",
            prompt: "Hello",
            response: "Hi there",
            status: "completed",
            error: "",
            metrics: {
              outputTokens: 8,
              totalDurationMs: 1_000,
              ttftMs: 200,
              stopReason: "stop",
            },
          },
        ]}
      />,
    );

    expect(screen.queryByText("Turn completed")).not.toBeInTheDocument();
    expect(screen.getByText("8 tokens")).toBeVisible();
    expect(screen.getByText("1.00s")).toBeVisible();
    expect(screen.getByText("TTFT 200ms")).toBeVisible();
    expect(screen.getByText("10.00 tok/s")).toBeVisible();
    expect(screen.getByText("Stop reason: stop")).toBeVisible();

    const copy = screen.getByRole("button", { name: "Copy response" });
    expect(copy).not.toHaveTextContent("Copy response");
    await user.click(copy);
    expect(copyText).toHaveBeenCalledWith("Hi there");
    expect(await screen.findByRole("status", { name: "Copy response status" })).toHaveTextContent("Response copied");
  });

  it("hides unavailable or contradictory metrics instead of estimating them", () => {
    render(
      <ChatTranscript
        emptyMessage=""
        copyText={vi.fn()}
        turns={[
          {
            id: "turn-2",
            model: "gemma",
            prompt: "Hello",
            response: "Hi",
            status: "completed",
            error: "",
            metrics: {
              outputTokens: null,
              totalDurationMs: 100,
              ttftMs: 200,
              stopReason: null,
            },
          },
        ]}
      />,
    );

    expect(screen.getByText("0.10s")).toBeVisible();
    expect(screen.queryByText(/tokens$/)).not.toBeInTheDocument();
    expect(screen.queryByText(/tok\/s$/)).not.toBeInTheDocument();
    expect(screen.queryByText(/TTFT/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Stop reason/)).not.toBeInTheDocument();
  });

  it.each([
    ["cancelled" as const, "Generation stopped"],
    ["failed" as const, "Turn failed — backend unavailable"],
  ])("keeps actionable %s terminal feedback", (status, expected) => {
    render(
      <ChatTranscript
        emptyMessage=""
        copyText={vi.fn()}
        turns={[
          {
            id: status,
            model: "gemma",
            prompt: "Hello",
            response: "Partial",
            status,
            error: status === "failed" ? "backend unavailable" : "",
          },
        ]}
      />,
    );

    expect(screen.getByText(expected)).toBeVisible();
  });
});
