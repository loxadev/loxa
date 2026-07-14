import { act } from "react";
import { expect, test, vi } from "vitest";
import { page, userEvent } from "vitest/browser";

import { CspProbePanel } from "./CspProbePanel";
import "./cspProbeBootstrap";
import { cspProbeStore } from "./cspProbeStore";
import { installConsoleCountProbe } from "./consoleCountProbe";
import { expectNoAxeViolations } from "@/test/axe";
import { cleanupBrowser, mountBrowser } from "@/test/browser";

test("captures a real early CSP violation without retaining its path", async () => {
  await expect.poll(() => cspProbeStore.getSnapshot().length).toBeGreaterThan(0);
  const exported = cspProbeStore.exportJson();
  expect(exported).toContain('"effectiveDirective":"img-src"');
  expect(exported).toContain('"blockedTarget":"https://csp-probe.invalid/[redacted]"');
  expect(exported).not.toMatch(/early-blocked-image|probe-secret/);
});

test("exposes long sanitized one-line evidence accessibly at narrow width", async () => {
  cspProbeStore.reset();
  cspProbeStore.recordViolation({
    effectiveDirective: "style-src-attr",
    blockedURI: "https://example.invalid/a/very/long/private/path?secret=never-retain",
    sourceFile: "/Users/alice/private/a-very-long-source-basename-that-remains-safe.css",
    lineNumber: 123,
    columnNumber: 456,
  });
  const originalWarn = vi.fn();
  const originalError = vi.fn();
  const target = { warn: originalWarn, error: originalError };
  const cleanupConsole = installConsoleCountProbe(target);
  mountBrowser(<CspProbePanel />);
  const field = document.querySelector<HTMLTextAreaElement>("#loxa-probe-json");
  if (!field) throw new Error("Missing Sanitized probe JSON field");

  const secret = {
    token: "browser-private-model-token",
    path: "/Users/alice/private/browser-model.gguf",
  };
  target.warn("browser-warning-secret", secret);
  target.error(secret);

  expect(field.value).toContain('"consoleCounts":{"warn":0,"error":0}');
  await act(async () => userEvent.click(page.getByRole("button", { name: "Refresh evidence" })));

  expect(field.value).toBe(
    '{"schemaVersion":1,"cspViolations":[{"effectiveDirective":"style-src-attr","blockedTarget":"https://example.invalid/[redacted]","sourceBasename":"a-very-long-source-basename-that-remains-safe.css","line":123,"column":456}],"consoleCounts":{"warn":1,"error":1}}',
  );
  expect(field.readOnly).toBe(true);
  expect(field.labels?.[0]?.textContent).toBe("Sanitized probe JSON");
  expect(field.value).not.toMatch(
    /never-retain|Users|alice|private\/path|browser-private-model-token|browser-model|browser-warning-secret/,
  );
  expect(originalWarn).toHaveBeenCalledOnce();
  expect(originalWarn.mock.calls[0]?.[1]).toBe(secret);
  expect(originalError).toHaveBeenCalledOnce();
  expect(originalError.mock.calls[0]?.[0]).toBe(secret);
  expect(field.getBoundingClientRect().right).toBeLessThanOrEqual(window.innerWidth);
  const panel = field.closest("section");
  if (!panel) throw new Error("Missing packaged probe evidence panel");
  await expectNoAxeViolations(panel);

  cleanupBrowser();
  cleanupConsole();
  expect(target.warn).toBe(originalWarn);
  expect(target.error).toBe(originalError);
});
