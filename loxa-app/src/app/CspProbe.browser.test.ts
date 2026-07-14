import { expect, test } from "vitest";

import "./cspProbeBootstrap";
import { cspProbeStore } from "./cspProbeStore";

test("captures a real early CSP violation without retaining its path", async () => {
  await expect.poll(() => cspProbeStore.getSnapshot().length).toBeGreaterThan(0);
  const exported = cspProbeStore.exportJson();
  expect(exported).toContain('"effectiveDirective":"img-src"');
  expect(exported).toContain('"blockedTarget":"https://csp-probe.invalid/[redacted]"');
  expect(exported).not.toMatch(/early-blocked-image|probe-secret/);
});
