import { act } from "react";
import { Copy } from "lucide-react";
import { expect, test } from "vitest";
import { page, userEvent } from "vitest/browser";

import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { Button, IconButton } from "./button";
import { Input } from "./input";
import { Label } from "./label";
import { Textarea } from "./textarea";

function NativeControlFixture() {
  return (
    <main>
      <h1>Native controls</h1>
      <Button>Start</Button>
      <IconButton label="Copy endpoint" helpId="copy-help">
        <Copy aria-hidden="true" />
      </IconButton>
      <p id="copy-help">Copies the local endpoint.</p>
      <Label htmlFor="endpoint">Endpoint</Label>
      <Input id="endpoint" />
      <Label htmlFor="prompt">Prompt</Label>
      <Textarea id="prompt" />
    </main>
  );
}

test("keeps every enabled native control at least 44 by 44 CSS pixels", async () => {
  mountBrowser(<NativeControlFixture />);

  for (const control of document.querySelectorAll<HTMLElement>(
    "button:not(:disabled), input:not(:disabled), textarea:not(:disabled)",
  )) {
    const { height, width } = control.getBoundingClientRect();
    expect(height, `${control.tagName} height`).toBeGreaterThanOrEqual(44);
    expect(width, `${control.tagName} width`).toBeGreaterThanOrEqual(44);
  }

  await expectNoAxeViolations(document);
});

test("shows visible keyboard focus for every enabled native control", async () => {
  mountBrowser(<NativeControlFixture />);
  const controls = [
    ...document.querySelectorAll<HTMLElement>("button:not(:disabled), input:not(:disabled), textarea:not(:disabled)"),
  ];

  for (const control of controls) {
    await act(async () => userEvent.keyboard("{Tab}"));
    expect(document.activeElement).toBe(control);
    const style = getComputedStyle(control);
    expect(style.outlineStyle).toBe("solid");
    expect(style.outlineWidth).toBe("2px");
  }

  await act(async () => userEvent.click(page.getByText("Endpoint", { exact: true })));
  await expect.element(page.getByLabelText("Endpoint", { exact: true })).toHaveFocus();
});
