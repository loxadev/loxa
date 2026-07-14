import { act } from "react";
import { expect, test } from "vitest";
import { page, userEvent } from "vitest/browser";

import { expectNoAxeViolations } from "@/test/axe";
import { mountBrowser } from "@/test/browser";
import { ConversationList, type ConversationListItem } from "./ConversationList";

const conversations: ConversationListItem[] = [
  {
    id: "0123456789abcdef0123456789abcdef",
    title: "A very long conversation title that remains stable when row actions appear",
    createdAtMs: 1,
    updatedAtMs: 20,
  },
  {
    id: "1123456789abcdef0123456789abcdef",
    title: "Second conversation",
    createdAtMs: 1,
    updatedAtMs: 10,
  },
];

function RailFixture() {
  return (
    <main style={{ width: 400, height: 640 }}>
      <h1 className="visually-hidden">Conversation history</h1>
      <ConversationList
        conversations={conversations}
        groupedConversations={[{ label: "Today", conversations }]}
        query=""
        setQuery={() => undefined}
        selectedId={conversations[0].id}
        state="ready"
        hasMore={false}
        onCreate={() => undefined}
        onSelect={() => undefined}
        onRename={() => undefined}
        onDelete={async () => null}
        onLoadMore={() => undefined}
      />
    </main>
  );
}

test("keeps row geometry stable through hover and focus while meeting target and overflow contracts", async () => {
  mountBrowser(<RailFixture />);
  const open = page.getByRole("button", { name: /Open A very long/ });
  const row = open.element().closest("li") as HTMLLIElement;
  const before = open.element().getBoundingClientRect();

  await act(async () => userEvent.hover(row));
  const hovered = open.element().getBoundingClientRect();
  expect(hovered.width).toBe(before.width);
  expect(hovered.height).toBe(before.height);

  page
    .getByRole("button", { name: /Rename A very long/ })
    .element()
    .focus();
  const focused = open.element().getBoundingClientRect();
  expect(focused.width).toBe(before.width);
  expect(focused.height).toBe(before.height);

  for (const control of document.querySelectorAll<HTMLElement>("button:not(:disabled), input:not(:disabled)")) {
    const { height, width } = control.getBoundingClientRect();
    expect(height, `${control.ariaLabel ?? control.textContent} height`).toBeGreaterThanOrEqual(44);
    expect(width, `${control.ariaLabel ?? control.textContent} width`).toBeGreaterThanOrEqual(44);
  }
});

test("reflows at an effective 400 CSS pixels without horizontal overflow and passes axe", async () => {
  mountBrowser(<RailFixture />);
  const rail = document.querySelector<HTMLElement>("nav")!;
  expect(rail.scrollWidth).toBeLessThanOrEqual(rail.clientWidth);
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(document.documentElement.clientWidth);
  await expectNoAxeViolations(document);
});
