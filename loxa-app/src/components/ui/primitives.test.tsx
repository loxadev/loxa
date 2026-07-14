import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fireEvent, render, screen } from "@testing-library/react";
import type { FormEvent } from "react";
import {
  Check,
  ChevronDown,
  Copy,
  Download,
  Pencil,
  Play,
  Plus,
  RefreshCw,
  Replace,
  Send,
  Settings,
  Square,
  Trash2,
  Unplug,
  X,
} from "lucide-react";
import { describe, expect, it, vi } from "vitest";

import { Alert, AlertDescription, AlertTitle } from "./alert";
import { Badge } from "./badge";
import { Button, IconButton } from "./button";
import { Input } from "./input";
import { Label } from "./label";
import { Progress } from "./progress";
import { Separator } from "./separator";
import { Textarea } from "./textarea";
import { VisuallyHidden } from "./visually-hidden";

const sourceRoot = resolve(import.meta.dirname, "../..");

function readOrEmpty(path: string) {
  try {
    return readFileSync(resolve(sourceRoot, path), "utf8");
  } catch {
    return "";
  }
}

describe("native UI primitive source contracts", () => {
  it.each([
    ["components/ui/button.tsx", "Button"],
    ["components/ui/button.tsx", "IconButton"],
    ["components/ui/badge.tsx", "Badge"],
    ["components/ui/alert.tsx", "Alert"],
    ["components/ui/input.tsx", "Input"],
    ["components/ui/textarea.tsx", "Textarea"],
    ["components/ui/progress.tsx", "Progress"],
    ["components/ui/separator.tsx", "Separator"],
    ["components/ui/label.tsx", "Label"],
    ["components/ui/visually-hidden.tsx", "VisuallyHidden"],
  ])("source-owns %s with a %s export", (path, exportName) => {
    const source = readOrEmpty(path);
    expect(source, `${path} must exist`).not.toBe("");
    expect(source).toMatch(
      new RegExp(`export\\s+(?:\\{[^}]*\\b${exportName}\\b|(?:function|const)\\s+${exportName}\\b)`),
    );
  });

  it("defines the exact Button variants, sizes, and busy state", () => {
    const source = readOrEmpty("components/ui/button.tsx");
    expect(source).toMatch(/variant:\s*\{\s*primary:/);
    for (const variant of ["primary", "secondary", "quiet", "danger"]) {
      expect(source).toMatch(new RegExp(`\\b${variant}:`));
    }
    for (const size of ["default", "icon"]) {
      expect(source).toMatch(new RegExp(`\\b${size}:`));
    }
    expect(source).toContain("cva(");
    expect(source).toContain("busy?: boolean");
    expect(source).toContain("aria-busy={busy || undefined}");
    expect(source).not.toMatch(/\b(?:outline|ghost|destructive|link|xs|sm|lg|"icon-(?:xs|sm|lg)"):/);
  });

  it("keeps every 3A primitive native instead of importing Radix catalogs", () => {
    const progress = readOrEmpty("components/ui/progress.tsx");
    expect(progress).toContain("<progress");
    expect(progress).toContain("max={total}");
    expect(progress).toContain("value={value}");
    expect(progress).toContain("value?: number");
    expect(readOrEmpty("components/ui/separator.tsx")).toContain("<hr");
    expect(readOrEmpty("components/ui/label.tsx")).toContain("<label");

    for (const path of [
      "components/ui/button.tsx",
      "components/ui/badge.tsx",
      "components/ui/alert.tsx",
      "components/ui/input.tsx",
      "components/ui/textarea.tsx",
      "components/ui/progress.tsx",
      "components/ui/separator.tsx",
      "components/ui/label.tsx",
      "components/ui/visually-hidden.tsx",
    ]) {
      expect(readOrEmpty(path)).not.toMatch(/from ["'](?:radix-ui|@radix-ui\/)/);
    }
  });
});

describe("native UI primitive runtime contracts", () => {
  it("disables and announces a busy Button", () => {
    render(
      <Button variant="primary" busy>
        Start
      </Button>,
    );

    expect(screen.getByRole("button", { name: "Start" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Start" })).toHaveAttribute("aria-busy", "true");
  });

  it("keeps computed busy state authoritative over native prop overrides", () => {
    render(
      <Button busy aria-busy={false} disabled={false}>
        Start
      </Button>,
    );

    const button = screen.getByRole("button", { name: "Start" });
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute("aria-busy", "true");
  });

  it("defaults to a non-submitting button while preserving explicit submit", () => {
    const onSubmit = vi.fn((event: FormEvent) => event.preventDefault());
    render(
      <form onSubmit={onSubmit}>
        <Button>Safe action</Button>
        <Button type="submit">Submit action</Button>
      </form>,
    );

    const safeAction = screen.getByRole("button", { name: "Safe action" });
    const submitAction = screen.getByRole("button", { name: "Submit action" });
    expect(safeAction).toHaveAttribute("type", "button");
    expect(submitAction).toHaveAttribute("type", "submit");
    fireEvent.click(safeAction);
    expect(onSubmit).not.toHaveBeenCalled();
    fireEvent.click(submitAction);
    expect(onSubmit).toHaveBeenCalledOnce();
  });

  it("gives IconButton one explicit name and persistent visible help", () => {
    render(
      <>
        <IconButton label="Copy endpoint" helpId="copy-help">
          <Copy aria-label="Copy glyph" />
        </IconButton>
        <span id="copy-help">Copies the local endpoint.</span>
      </>,
    );

    const button = screen.getByRole("button", { name: "Copy endpoint" });
    expect(button).toBeVisible();
    expect(button).toHaveAttribute("aria-describedby", "copy-help");
    expect(screen.getByText("Copies the local endpoint.")).toBeVisible();
    expect(button.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    expect(screen.queryByRole("img", { name: "Copy glyph" })).not.toBeInTheDocument();
  });

  it.each([
    ["label", "", <Copy aria-hidden="true" key="copy" />],
    ["helpId", "Copy endpoint", <Copy aria-hidden="true" key="copy" />],
  ])("rejects an empty IconButton %s", (_field, label, icon) => {
    expect(() =>
      render(
        <IconButton label={label} helpId={label ? "" : "copy-help"}>
          {icon}
        </IconButton>,
      ),
    ).toThrow();
  });

  it("keeps every mapped Lucide icon decorative inside a named control", () => {
    const iconMap = [
      ["Add attachment", Plus],
      ["Copy", Copy],
      ["Copied", Check],
      ["Rename", Pencil],
      ["Delete", Trash2],
      ["Send", Send],
      ["Stop", Square],
      ["Retry", RefreshCw],
      ["Download", Download],
      ["Start", Play],
      ["Switch", Replace],
      ["Unload", Unplug],
      ["Close", X],
      ["Show options", ChevronDown],
      ["Settings", Settings],
    ] as const;

    render(
      <>
        {iconMap.map(([label, Icon], index) => {
          const helpId = `icon-help-${index}`;
          return (
            <span key={label}>
              <IconButton label={label} helpId={helpId}>
                <Icon aria-hidden="true" />
              </IconButton>
              <span id={helpId}>Help for {label}</span>
            </span>
          );
        })}
      </>,
    );

    for (const [label] of iconMap) expect(screen.getByRole("button", { name: label })).toBeVisible();
    expect(screen.queryAllByRole("img")).toHaveLength(0);
  });

  it("renders the semantic helpers as native elements", () => {
    const { container } = render(
      <>
        <Label htmlFor="endpoint">Endpoint</Label>
        <Input id="endpoint" />
        <Textarea aria-label="Prompt" />
        <Progress aria-label="Download progress" value={4} total={10} />
        <Progress aria-label="Indeterminate progress" />
        <Separator />
        <Badge>Ready</Badge>
        <Alert>
          <AlertTitle>Connected</AlertTitle>
          <AlertDescription>The node is ready.</AlertDescription>
        </Alert>
        <VisuallyHidden>Additional context</VisuallyHidden>
      </>,
    );

    expect(screen.getByLabelText("Endpoint")).toBeInstanceOf(HTMLInputElement);
    expect(screen.getByLabelText("Prompt")).toBeInstanceOf(HTMLTextAreaElement);
    expect(screen.getByRole("progressbar", { name: "Download progress" })).toHaveAttribute("max", "10");
    expect(screen.getByRole("progressbar", { name: "Download progress" })).toHaveAttribute("value", "4");
    const indeterminate = screen.getByRole("progressbar", { name: "Indeterminate progress" });
    expect(indeterminate).not.toHaveAttribute("max");
    expect(indeterminate).not.toHaveAttribute("value");
    expect(container.querySelector("hr[data-slot='separator']")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("ConnectedThe node is ready.");
    expect(screen.getByText("Additional context")).toHaveClass("sr-only");
  });
});
