import { act } from "react";
import type { CDPSession } from "@vitest/browser-playwright";
import { expect, test } from "vitest";
import { cdp, userEvent } from "vitest/browser";

import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { Button } from "../ui/button";
import { AsyncAction } from "./async-action";
import { EmptyState } from "./empty-state";
import { OperationProgress } from "./operation-progress";
import { RuntimeStatus } from "./runtime-status";
import { ScreenHeader } from "./screen-header";
import { StatusBanner } from "./status-banner";
import { TechnicalValue } from "./technical-value";

function PresentationFixture() {
  return (
    <main className="space-y-6 p-6">
      <ScreenHeader
        eyebrow="Runtime"
        title="Local node"
        summary="Manage the local inference runtime."
        actions={<Button>Restart</Button>}
      />
      <StatusBanner tone="info" title="Starting">
        Waiting for the node.
      </StatusBanner>
      <RuntimeStatus label="Node ready" detail="Listening locally" tone="success" action={<Button>Reconnect</Button>} />
      <TechnicalValue>hf://organization/a-very-long-model-identifier-that-must-wrap-safely</TechnicalValue>
      <OperationProgress label="Downloading model" value={4} total={10} detail="4 of 10 GB" />
      <EmptyState title="No models" description="Pull a model to begin." action={<Button>Pull model</Button>} />
      <AsyncAction pendingLabel="Starting…">Start node</AsyncAction>
      <AsyncAction busy pendingLabel="Stopping…">
        Stop node
      </AsyncAction>
    </main>
  );
}

test("keeps presentation actions at least 44px with visible keyboard focus", async () => {
  mountBrowser(<PresentationFixture />);
  const controls = [...document.querySelectorAll<HTMLButtonElement>("button:not(:disabled)")];

  expect(controls.length).toBeGreaterThan(0);
  for (const control of controls) {
    const { height, width } = control.getBoundingClientRect();
    expect(height, `${control.textContent} height`).toBeGreaterThanOrEqual(44);
    expect(width, `${control.textContent} width`).toBeGreaterThanOrEqual(44);
    await act(async () => userEvent.keyboard("{Tab}"));
    expect(document.activeElement).toBe(control);
    const style = getComputedStyle(control);
    expect(style.outlineStyle).toBe("solid");
    expect(style.outlineWidth).toBe("2px");
  }

  await expectNoAxeViolations(document);
});

test("stays accessible under emulated reduced motion, forced colors, and increased contrast", async () => {
  const session = cdp() as CDPSession;
  await session.send("Emulation.setEmulatedMedia", {
    features: [
      { name: "prefers-reduced-motion", value: "reduce" },
      { name: "forced-colors", value: "active" },
      { name: "prefers-contrast", value: "more" },
    ],
  });

  try {
    mountBrowser(<PresentationFixture />);
    expect(matchMedia("(prefers-reduced-motion: reduce)").matches).toBe(true);
    expect(matchMedia("(forced-colors: active)").matches).toBe(true);
    expect(matchMedia("(prefers-contrast: more)").matches).toBe(true);
    expect(getComputedStyle(document.documentElement).getPropertyValue("--loxa-motion-fast").trim()).toBe("0.01ms");
    await expectNoAxeViolations(document);
  } finally {
    await session.send("Emulation.setEmulatedMedia", { features: [] });
  }
});
