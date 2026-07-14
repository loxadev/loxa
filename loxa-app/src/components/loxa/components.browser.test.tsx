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
        title="Local node with an intentionally long heading that must reflow inside a narrow workspace"
        summary="Manage the local inference runtime without allowing long presentation content to widen the workspace."
        actions={<Button>Restart</Button>}
      />
      <StatusBanner tone="info" title="Starting">
        Waiting for the node.
      </StatusBanner>
      <RuntimeStatus
        label="Node ready"
        detail="listening-on-an-intentionally-long-unbroken-runtime-endpoint-that-must-wrap.local"
        tone="success"
        action={<Button>Reconnect</Button>}
      />
      <TechnicalValue>
        hf://organization/a-very-long-model-identifier-that-must-wrap-safely-without-horizontal-overflow
      </TechnicalValue>
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
  const busyControl = document.querySelector<HTMLButtonElement>("button:disabled[aria-busy='true']");

  expect(controls).toHaveLength(4);
  expect(document.querySelectorAll("button:disabled[aria-busy='true']")).toHaveLength(1);
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
  await act(async () => userEvent.keyboard("{Tab}"));
  expect(document.activeElement).not.toBe(busyControl);

  await expectNoAxeViolations(document);
});

test("reflows long content in a narrow 200 percent text container", async () => {
  const { host } = mountBrowser(<PresentationFixture />);
  host.style.width = "320px";
  host.style.fontSize = "200%";

  const technicalValue = document.querySelector<HTMLElement>("[data-slot='technical-value']");
  expect(technicalValue).not.toBeNull();
  const hostRight = host.getBoundingClientRect().right;
  const overflowSources = [...host.querySelectorAll<HTMLElement>("*")]
    .filter((element) => element.getBoundingClientRect().right > hostRight)
    .map((element) => `${element.tagName}:${element.dataset.slot ?? "none"}:${element.getBoundingClientRect().right}`);
  expect(host.scrollWidth, overflowSources.join(", ")).toBeLessThanOrEqual(host.clientWidth);
  expect(technicalValue!.scrollWidth).toBeLessThanOrEqual(technicalValue!.clientWidth);
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
    const transitionDuration = getComputedStyle(document.querySelector("button")!).transitionDuration;
    const transitionMilliseconds = transitionDuration.endsWith("ms")
      ? Number.parseFloat(transitionDuration)
      : Number.parseFloat(transitionDuration) * 1000;
    expect(transitionMilliseconds).toBeCloseTo(0.01);
    expect(getComputedStyle(document.querySelector("[data-slot='status-badge']")!).borderTopWidth).toBe("2px");
    expect(getComputedStyle(document.querySelector("[data-slot='status-banner']")!).borderTopWidth).toBe("2px");
    const action = document.querySelector<HTMLButtonElement>("button:not(:disabled)")!;
    action.focus();
    const actionStyle = getComputedStyle(action);
    expect(action.getBoundingClientRect().height).toBeGreaterThanOrEqual(44);
    expect(actionStyle.outlineStyle).toBe("solid");
    expect(actionStyle.outlineWidth).toBe("2px");
    expect(actionStyle.outlineColor).not.toBe("rgba(0, 0, 0, 0)");
    await expectNoAxeViolations(document);
  } finally {
    await session.send("Emulation.setEmulatedMedia", { features: [] });
  }
});
