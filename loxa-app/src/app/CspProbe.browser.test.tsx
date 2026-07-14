import { expect, test } from "vitest";

import { CspProbePanel } from "./CspProbePanel";
import "./cspProbeBootstrap";
import { cspProbeStore, serializeEvidence } from "./cspProbeStore";
import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";

test("captures a real early CSP violation without retaining its path", async () => {
  await expect.poll(() => cspProbeStore.getSnapshot().length).toBeGreaterThan(0);
  const exported = cspProbeStore.exportJson();
  expect(exported).toContain('"effectiveDirective":"img-src"');
  expect(exported).toContain('"blockedTarget":"https://csp-probe.invalid/[redacted]"');
  expect(exported).not.toMatch(/early-blocked-image|probe-secret/);
});

test("exposes long sanitized one-line evidence accessibly at narrow width", async () => {
  cspProbeStore.reset();
  cspProbeStore.recordConsole("warn");
  cspProbeStore.recordViolation({
    effectiveDirective: "style-src-attr",
    blockedURI: "https://example.invalid/a/very/long/private/path?secret=never-retain",
    sourceFile: "/Users/alice/private/a-very-long-source-basename-that-remains-safe.css",
    lineNumber: 123,
    columnNumber: 456,
  });
  mountBrowser(<CspProbePanel />);
  const field = document.querySelector<HTMLTextAreaElement>("#loxa-probe-json");
  if (!field) throw new Error("Missing Sanitized probe JSON field");

  expect(field.value).toBe(serializeEvidence());
  expect(field.readOnly).toBe(true);
  expect(field.labels?.[0]?.textContent).toBe("Sanitized probe JSON");
  expect(field.value).not.toMatch(/never-retain|Users|alice|private\/path/);
  expect(field.getBoundingClientRect().right).toBeLessThanOrEqual(window.innerWidth);
  const panel = field.closest("section");
  if (!panel) throw new Error("Missing packaged probe evidence panel");
  await expectNoAxeViolations(panel);
});
