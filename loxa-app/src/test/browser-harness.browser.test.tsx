import { act, useContext, useState } from "react";
import { createPortal } from "react-dom";
import { expect, test } from "vitest";
import { page, userEvent } from "vitest/browser";

import { expectNoAxeViolations } from "@/test/axe";
import { BrowserPortalContext, cleanupBrowser, mountBrowser } from "@/test/browser";

function PortalFixture() {
  const portal = useContext(BrowserPortalContext);
  const [clicks, setClicks] = useState(0);
  const [open, setOpen] = useState(false);

  return (
    <>
      <main>
        <h1>Browser harness</h1>
        <button type="button" onClick={() => setClicks((count) => count + 1)}>
          Click fixture
        </button>
        <output aria-live="polite">Clicks: {clicks}</output>
        <button type="button" onClick={() => setOpen(true)}>
          Open portal
        </button>
      </main>
      {open && portal
        ? createPortal(
            <div role="dialog" aria-label="Portal fixture">
              Portal content
            </div>,
            portal,
          )
        : null}
    </>
  );
}

test("mounts an interactive native fixture and passes a strict document axe scan", async () => {
  mountBrowser(<PortalFixture />);

  await act(async () => userEvent.click(page.getByRole("button", { name: "Click fixture" })));

  await expect.element(page.getByText("Clicks: 1")).toBeVisible();
  await expectNoAxeViolations(document);
});

test("removes portal content, focus guards, hidden state, inert state, and body locks during cleanup", async () => {
  mountBrowser(<PortalFixture />);
  await act(async () => userEvent.click(page.getByRole("button", { name: "Open portal" })));
  await expect.element(page.getByRole("dialog", { name: "Portal fixture" })).toBeVisible();

  document.body.classList.add("scroll-locked");
  document.body.style.overflow = "hidden";
  document.body.insertAdjacentHTML("beforeend", '<span data-radix-focus-guard aria-hidden="true" inert></span>');
  cleanupBrowser();

  expect(document.querySelector("[data-radix-focus-guard]")).toBeNull();
  expect(document.querySelector('[aria-hidden="true"]')).toBeNull();
  expect(document.querySelector("[inert]")).toBeNull();
  expect(document.body.className).toBe("");
  expect(document.body.style.cssText).toBe("");
  expect(document.querySelector("#loxa-portal-root")).toBeNull();
});

test("clears local and session storage during cleanup", () => {
  window.localStorage.setItem("loxa.theme", "dark");
  window.localStorage.setItem("loxa.rail-order", "models-first");
  window.sessionStorage.setItem("loxa.browser-state", "open");

  cleanupBrowser();

  expect(window.localStorage).toHaveLength(0);
  expect(window.sessionStorage).toHaveLength(0);
});
