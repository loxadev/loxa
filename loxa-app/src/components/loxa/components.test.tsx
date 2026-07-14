import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Button } from "../ui/button";
import { AsyncAction } from "./async-action";
import { EmptyState } from "./empty-state";
import { OperationProgress } from "./operation-progress";
import { RuntimeStatus } from "./runtime-status";
import { ScreenHeader } from "./screen-header";
import { StatusBadge } from "./status-badge";
import { StatusBanner } from "./status-banner";
import { TechnicalValue } from "./technical-value";

const sourceRoot = resolve(import.meta.dirname, "../..");

function readOrEmpty(path: string) {
  try {
    return readFileSync(resolve(sourceRoot, path), "utf8");
  } catch {
    return "";
  }
}

describe("Loxa presentation component source contracts", () => {
  it.each([
    ["components/loxa/screen-header.tsx", "ScreenHeader"],
    ["components/loxa/status-badge.tsx", "StatusBadge"],
    ["components/loxa/status-banner.tsx", "StatusBanner"],
    ["components/loxa/technical-value.tsx", "TechnicalValue"],
    ["components/loxa/empty-state.tsx", "EmptyState"],
    ["components/loxa/operation-progress.tsx", "OperationProgress"],
    ["components/loxa/runtime-status.tsx", "RuntimeStatus"],
    ["components/loxa/async-action.tsx", "AsyncAction"],
  ])("source-owns %s with a %s export", (path, exportName) => {
    const source = readOrEmpty(path);
    expect(source, `${path} must exist`).not.toBe("");
    expect(source).toMatch(
      new RegExp(`export\\s+(?:\\{[^}]*\\b${exportName}\\b|(?:function|const)\\s+${exportName}\\b)`),
    );
  });

  it("keeps product components presentation-only", () => {
    for (const path of [
      "components/loxa/screen-header.tsx",
      "components/loxa/status-badge.tsx",
      "components/loxa/status-banner.tsx",
      "components/loxa/technical-value.tsx",
      "components/loxa/empty-state.tsx",
      "components/loxa/operation-progress.tsx",
      "components/loxa/runtime-status.tsx",
      "components/loxa/async-action.tsx",
    ]) {
      const source = readOrEmpty(path);
      expect(source).not.toMatch(/(?:fetch\(|invoke\(|EventSource|AbortController|useEffect|useLayoutEffect)/);
      expect(source.split("\n").length).toBeLessThanOrEqual(250);
    }
  });
});

describe("Loxa presentation component runtime contracts", () => {
  it("renders a semantic screen header without empty optional regions", () => {
    const { container, rerender } = render(<ScreenHeader eyebrow="Runtime" title="Local node" />);

    expect(screen.getByRole("heading", { level: 1, name: "Local node" })).toBeVisible();
    expect(screen.getByText("Runtime")).toBeVisible();
    expect(container.querySelector("[data-slot='screen-header-summary']")).not.toBeInTheDocument();
    expect(container.querySelector("[data-slot='screen-header-actions']")).not.toBeInTheDocument();

    rerender(
      <ScreenHeader
        eyebrow="Runtime"
        title="Local node"
        summary="Manage the local inference runtime."
        actions={<Button>Restart</Button>}
      />,
    );
    expect(screen.getByText("Manage the local inference runtime.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Restart" })).toBeVisible();
  });

  it.each(["neutral", "info", "success", "warning", "danger"] as const)(
    "maps the %s status tone without inventing a live region",
    (tone) => {
      render(<StatusBadge tone={tone}>{tone}</StatusBadge>);

      const badge = screen.getByText(tone);
      expect(badge).toHaveAttribute("data-variant", tone);
      expect(badge).not.toHaveAttribute("role");
    },
  );

  it("defaults a status banner to polite status and honors explicit alerts", () => {
    const { rerender } = render(
      <StatusBanner tone="info" title="Starting">
        Waiting for the node.
      </StatusBanner>,
    );

    expect(screen.getByRole("status")).toHaveTextContent("StartingWaiting for the node.");
    rerender(<StatusBanner tone="danger" title="Failed" role="alert" />);
    expect(screen.getByRole("alert")).toHaveTextContent("Failed");
  });

  it("forwards native code props and safely wraps technical values", () => {
    render(
      <TechnicalValue title="Model identifier" className="caller-class">
        hf://organization/a-very-long-model-identifier
      </TechnicalValue>,
    );

    const value = screen.getByTitle("Model identifier");
    expect(value.tagName).toBe("CODE");
    expect(value).toHaveClass("caller-class", "break-all");
  });

  it("renders empty and runtime states without empty optional markup", () => {
    const { container, rerender } = render(<EmptyState title="No models" description="Pull a model to begin." />);
    expect(screen.getByRole("heading", { level: 2, name: "No models" })).toBeVisible();
    expect(screen.getByText("Pull a model to begin.")).toBeVisible();
    expect(container.querySelector("[data-slot='empty-state-action']")).not.toBeInTheDocument();

    rerender(<RuntimeStatus label="Node ready" detail="Listening on 127.0.0.1" tone="success" />);
    expect(screen.getByText("Node ready")).toHaveAttribute("data-variant", "success");
    expect(screen.getByText("Listening on 127.0.0.1")).toBeVisible();
    expect(container.querySelector("[data-slot='runtime-status-action']")).not.toBeInTheDocument();
  });

  it("renders progress only when both value and total make it determinate", () => {
    const { rerender } = render(<OperationProgress label="Downloading" value={4} total={10} detail="4 of 10 GB" />);

    const determinate = screen.getByRole("progressbar", { name: "Downloading" });
    expect(determinate).toHaveAttribute("value", "4");
    expect(determinate).toHaveAttribute("max", "10");
    expect(screen.getByText("4 of 10 GB")).toBeVisible();

    rerender(<OperationProgress label="Preparing" value={4} />);
    const indeterminate = screen.getByRole("progressbar", { name: "Preparing" });
    expect(indeterminate).not.toHaveAttribute("value");
    expect(indeterminate).not.toHaveAttribute("max");
    expect(screen.queryByText("4 of 10 GB")).not.toBeInTheDocument();
  });

  it("keeps AsyncAction controlled by Button busy state", () => {
    const { rerender } = render(<AsyncAction pendingLabel="Starting…">Start</AsyncAction>);
    const ready = screen.getByRole("button", { name: "Start" });
    expect(ready).toBeEnabled();
    expect(ready).toHaveAttribute("type", "button");

    rerender(
      <AsyncAction busy pendingLabel="Starting…">
        Start
      </AsyncAction>,
    );
    const pending = screen.getByRole("button", { name: "Starting…" });
    expect(pending).toBeDisabled();
    expect(pending).toHaveAttribute("aria-busy", "true");
  });
});
