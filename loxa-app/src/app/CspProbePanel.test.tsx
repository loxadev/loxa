import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CspProbePanel } from "./CspProbePanel";
import { cspProbeStore } from "./cspProbeStore";

describe("CSP probe panel", () => {
  beforeEach(() => cspProbeStore.reset());
  afterEach(() => vi.restoreAllMocks());

  it("renders labelled deterministic evidence and resets CSP only", () => {
    cspProbeStore.recordConsole("warn");
    render(<CspProbePanel />);
    act(() => {
      cspProbeStore.recordViolation({
        effectiveDirective: "img-src",
        blockedURI: "https://example.invalid/private?token=secret",
        sourceFile: "/private/main.tsx",
        lineNumber: 2,
        columnNumber: 3,
      });
    });

    const status = screen.getByRole("status");
    expect(status).toHaveAttribute("aria-live", "polite");
    expect(status).toHaveTextContent("CSP violations: 1; console warnings: 1; console errors: 0");
    expect(status).not.toHaveTextContent("example.invalid");
    expect(screen.getByRole("textbox", { name: "Sanitized probe JSON" })).toHaveValue(
      '{"schemaVersion":1,"cspViolations":[{"effectiveDirective":"img-src","blockedTarget":"https://example.invalid/[redacted]","sourceBasename":"main.tsx","line":2,"column":3}],"consoleCounts":{"warn":1,"error":0}}',
    );
    expect(within(screen.getByRole("list")).getByText(/https:\/\/example\.invalid\/\[redacted\]/)).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Reset CSP" }));
    expect(screen.getByRole("status")).toHaveTextContent("CSP violations: 0; console warnings: 1; console errors: 0");
  });

  it("refreshes console counts manually without using export or persistence APIs", () => {
    const createObjectUrl = vi.spyOn(URL, "createObjectURL");
    const append = vi.spyOn(document.body, "append");
    const storage = vi.spyOn(Storage.prototype, "setItem");
    const fetchRequest = vi.spyOn(globalThis, "fetch");
    render(<CspProbePanel />);

    act(() => cspProbeStore.recordConsole("error"));
    expect(screen.getByRole("status")).toHaveTextContent("console errors: 0");
    fireEvent.click(screen.getByRole("button", { name: "Refresh evidence" }));

    expect(screen.getByRole("status")).toHaveTextContent("console errors: 1");
    expect(screen.getByRole("textbox", { name: "Sanitized probe JSON" })).toHaveValue(
      '{"schemaVersion":1,"cspViolations":[],"consoleCounts":{"warn":0,"error":1}}',
    );
    expect(createObjectUrl).not.toHaveBeenCalled();
    expect(append).not.toHaveBeenCalled();
    expect(storage).not.toHaveBeenCalled();
    expect(fetchRequest).not.toHaveBeenCalled();
  });
});
