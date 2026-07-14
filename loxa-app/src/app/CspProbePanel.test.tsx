import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { CspProbePanel } from "./CspProbePanel";
import { cspProbeStore } from "./cspProbeStore";

describe("CSP probe panel", () => {
  beforeEach(() => cspProbeStore.reset());

  it("renders live sanitized details and resets them", () => {
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

    expect(screen.getByRole("status")).toHaveTextContent("1 violation");
    expect(screen.getByRole("status")).toHaveTextContent("https://example.invalid/[redacted]");
    fireEvent.click(screen.getByRole("button", { name: "Reset" }));
    expect(screen.getByRole("status")).toHaveTextContent("0 violations");
  });

  it("exports the snapshot with a fixed filename through a short-lived object URL", async () => {
    const objectUrl = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:csp-export");
    const revoke = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    let download = "";
    let href = "";
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      download = this.download;
      href = this.href;
    });
    render(<CspProbePanel />);

    fireEvent.click(screen.getByRole("button", { name: "Export JSON" }));

    expect(objectUrl).toHaveBeenCalledOnce();
    const blob = objectUrl.mock.calls[0]?.[0];
    expect(blob).toBeInstanceOf(Blob);
    if (!(blob instanceof Blob)) throw new Error("expected a JSON Blob");
    const contents = await new Promise<string>((resolve) => {
      const reader = new FileReader();
      reader.addEventListener("load", () => resolve(String(reader.result)));
      reader.readAsText(blob);
    });
    expect(contents).toBe("[]");
    expect(download).toBe("loxa-csp-probe.json");
    expect(href).toBe("blob:csp-export");
    expect(click).toHaveBeenCalledOnce();
    expect(revoke).toHaveBeenCalledWith("blob:csp-export");
  });
});
