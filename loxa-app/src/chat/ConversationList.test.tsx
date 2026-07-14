import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ConversationList, type ConversationListItem } from "./ConversationList";

const conversations: ConversationListItem[] = [
  {
    id: "0123456789abcdef0123456789abcdef",
    title: "Node health",
    createdAtMs: 1,
    updatedAtMs: 20,
    terminalState: "completed",
  },
  {
    id: "1123456789abcdef0123456789abcdef",
    title: "Download model",
    createdAtMs: 1,
    updatedAtMs: 10,
    terminalState: "failed",
  },
];

const baseProps = {
  conversations,
  groupedConversations: [
    { label: "Today" as const, conversations: [conversations[0]] },
    { label: "Older" as const, conversations: [conversations[1]] },
  ],
  query: "",
  setQuery: vi.fn(),
  selectedId: conversations[0].id,
  state: "ready" as const,
  hasMore: false,
  onCreate: vi.fn(),
  onSelect: vi.fn(),
  onRename: vi.fn(),
  onDelete: vi.fn(),
  onLoadMore: vi.fn(),
};

describe("ConversationList", () => {
  it("renders non-empty controller groups in fixed order and omits empty groups", () => {
    render(<ConversationList {...baseProps} />);

    const headings = screen.getAllByRole("heading").map((heading) => heading.textContent);
    expect(headings).toEqual(["Conversations", "Today", "Older"]);
    expect(screen.queryByRole("heading", { name: "Yesterday" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Previous 7 days" })).not.toBeInTheDocument();
  });

  it("uses a controlled labelled search and reports background loading and errors truthfully", async () => {
    const setQuery = vi.fn();
    const { rerender } = render(<ConversationList {...baseProps} query="NODE" setQuery={setQuery} state="loading" />);

    const search = screen.getByRole("searchbox", { name: "Search conversations" });
    expect(search).toHaveValue("NODE");
    fireEvent.change(search, { target: { value: "NODE health" } });
    expect(setQuery).toHaveBeenLastCalledWith("NODE health");
    expect(screen.getByRole("status")).toHaveTextContent("Searching conversations");

    rerender(<ConversationList {...baseProps} state="error" errorMessage="History is unavailable." />);
    expect(screen.getByRole("alert")).toHaveTextContent("History is unavailable.");
  });

  it("uses Lucide-backed named controls and preserves the full title contract", () => {
    const longTitle = "A very long conversation title that must truncate without losing its accessible name";
    const longConversation = { ...conversations[0], title: longTitle };
    render(
      <ConversationList
        {...baseProps}
        conversations={[longConversation]}
        groupedConversations={[{ label: "Today", conversations: [longConversation] }]}
      />,
    );

    for (const control of [
      screen.getByRole("button", { name: "New chat" }),
      screen.getByRole("button", { name: `Rename ${longTitle}` }),
      screen.getByRole("button", { name: `Delete ${longTitle}` }),
    ]) {
      expect(control.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    }
    expect(screen.getByRole("button", { name: `Open ${longTitle}` })).toHaveAttribute("title", longTitle);
  });

  it("shows a compact recent-chat rail with selection, timestamps, and terminal text", async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    const onCreate = vi.fn();
    render(<ConversationList {...baseProps} onCreate={onCreate} onSelect={onSelect} />);

    expect(screen.getByRole("navigation", { name: "Chat conversations" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Conversations" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "New chat" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Open Node health/ })).toHaveAttribute("aria-current", "page");
    expect(screen.getByText("Completed")).toBeVisible();
    expect(screen.getByText("Failed")).toBeVisible();
    expect(document.querySelectorAll("time")).toHaveLength(2);

    await user.click(screen.getByRole("button", { name: "New chat" }));
    expect(onCreate).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: /Open Download model/ }));
    expect(onSelect).toHaveBeenCalledWith(conversations[1].id);
  });

  it("disables only non-selected conversation rows while a response is active", () => {
    render(<ConversationList {...baseProps} mutationsDisabled />);

    const selected = screen.getByRole("button", { name: "Open Node health" });
    const blocked = screen.getByRole("button", { name: "Open Download model" });
    expect(selected).toBeEnabled();
    expect(selected).not.toHaveAttribute("aria-describedby");
    expect(blocked).toBeDisabled();
    expect(blocked).toHaveAccessibleDescription("Unavailable while a response is active.");
  });

  it("provides loading, empty, error, and paginated states without color-only meaning", async () => {
    const user = userEvent.setup();
    const onLoadMore = vi.fn();
    const { rerender } = render(<ConversationList {...baseProps} conversations={[]} state="loading" />);
    expect(screen.getByRole("status")).toHaveTextContent("Loading conversations");

    rerender(<ConversationList {...baseProps} conversations={[]} state="ready" />);
    expect(screen.getByText("No conversations yet.")).toBeVisible();

    rerender(
      <ConversationList {...baseProps} conversations={[]} state="error" errorMessage="History is unavailable." />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("History is unavailable.");

    rerender(<ConversationList {...baseProps} hasMore onLoadMore={onLoadMore} />);
    await user.click(screen.getByRole("button", { name: "Load more conversations" }));
    expect(onLoadMore).toHaveBeenCalledOnce();
  });

  it("awaits create and load-more actions, prevents duplicates, and catches failures", async () => {
    const user = userEvent.setup();
    let rejectCreate!: (reason?: unknown) => void;
    const onCreate = vi.fn(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectCreate = reject;
        }),
    );
    const onLoadMore = vi.fn(async () => {
      throw new Error("private failure");
    });
    render(<ConversationList {...baseProps} hasMore onCreate={onCreate} onLoadMore={onLoadMore} />);

    const create = screen.getByRole("button", { name: "New chat" });
    await user.click(create);
    expect(create).toBeDisabled();
    await user.click(create);
    expect(onCreate).toHaveBeenCalledOnce();
    rejectCreate(new Error("secret"));
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not create a new conversation.");
    expect(create).toBeEnabled();

    const more = screen.getByRole("button", { name: "Load more conversations" });
    await user.click(more);
    expect(onLoadMore).toHaveBeenCalledOnce();
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not load more conversations.");
    expect(more).toBeEnabled();
  });

  it("blocks every mutation with an accessible active-turn reason", async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn();
    const onRename = vi.fn();
    const onDelete = vi.fn();
    render(
      <ConversationList {...baseProps} mutationsDisabled onCreate={onCreate} onRename={onRename} onDelete={onDelete} />,
    );

    const reason = screen.getByText("Unavailable while a response is active.");
    for (const control of [
      screen.getByRole("button", { name: "New chat" }),
      screen.getByRole("button", { name: "Rename Node health" }),
      screen.getByRole("button", { name: "Delete Node health" }),
    ]) {
      expect(control).toBeDisabled();
      expect(control).toHaveAttribute("aria-describedby", reason.id);
      await user.click(control);
    }
    expect(screen.getByRole("button", { name: "Open Download model" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Open Download model" })).toHaveAttribute("aria-describedby", reason.id);
    expect(onCreate).not.toHaveBeenCalled();
    expect(onRename).not.toHaveBeenCalled();
    expect(onDelete).not.toHaveBeenCalled();
  });

  it("renames accessibly and restores focus after cancelling", async () => {
    const user = userEvent.setup();
    const onRename = vi.fn(async () => undefined);
    render(<ConversationList {...baseProps} onRename={onRename} />);

    const rename = screen.getByRole("button", { name: "Rename Node health" });
    await user.click(rename);
    const input = screen.getByRole("textbox", { name: "Conversation title" });
    expect(input).toHaveFocus();
    await user.clear(input);
    await user.type(input, "Runtime checks");
    await user.click(screen.getByRole("button", { name: "Save title" }));
    expect(onRename).toHaveBeenCalledWith(conversations[0].id, "Runtime checks");

    await user.click(screen.getByRole("button", { name: "Rename Node health" }));
    await user.click(screen.getByRole("button", { name: "Cancel rename" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Rename Node health" })).toHaveFocus());
  });

  it("supports Escape to cancel rename and preserves a nonblank bounded title", async () => {
    const user = userEvent.setup();
    const onRename = vi.fn();
    render(<ConversationList {...baseProps} onRename={onRename} />);
    const rename = screen.getByRole("button", { name: "Rename Node health" });
    await user.click(rename);
    const input = screen.getByRole("textbox", { name: "Conversation title" });
    fireEvent.change(input, { target: { value: " ".repeat(4) } });
    expect(screen.getByRole("button", { name: "Save title" })).toBeDisabled();
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.getByRole("button", { name: "Rename Node health" })).toHaveFocus());
    expect(onRename).not.toHaveBeenCalled();
  });

  it("keeps rename open and restores input focus when rename fails", async () => {
    const user = userEvent.setup();
    const onRename = vi.fn(async () => {
      throw new Error("private rename detail");
    });
    render(<ConversationList {...baseProps} onRename={onRename} />);

    await user.click(screen.getByRole("button", { name: "Rename Node health" }));
    const input = screen.getByRole("textbox", { name: "Conversation title" });
    await user.clear(input);
    await user.type(input, "Runtime checks");
    await user.click(screen.getByRole("button", { name: "Save title" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Could not rename this conversation.");
    await waitFor(() => expect(input).toHaveFocus());
    expect(screen.getByRole("button", { name: "Save title" })).toBeEnabled();
  });

  it("requires explicit delete confirmation, supports cancel, and restores focus", async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn(async () => conversations[1].id);
    render(<ConversationList {...baseProps} onDelete={onDelete} />);

    const remove = screen.getByRole("button", { name: "Delete Node health" });
    await user.click(remove);
    const dialog = screen.getByRole("group", { name: "Delete Node health?" });
    expect(within(dialog).getByRole("button", { name: "Cancel delete" })).toHaveFocus();
    expect(within(dialog).getByText("This cannot be undone.")).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Cancel delete" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Delete Node health" })).toHaveFocus());
    expect(onDelete).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Delete Node health" }));
    await user.click(
      within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", {
        name: "Delete conversation",
      }),
    );
    expect(onDelete).toHaveBeenCalledWith(conversations[0].id);
    await waitFor(() => expect(screen.getByRole("button", { name: "Open Download model" })).toHaveFocus());
  });

  it("focuses the nearest survivor after a nonselected delete and New chat when none remains", async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn(async () => null);
    const view = render(<ConversationList {...baseProps} selectedId={conversations[0].id} onDelete={onDelete} />);

    await user.click(screen.getByRole("button", { name: "Delete Download model" }));
    await user.click(screen.getByRole("button", { name: "Delete conversation" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Open Node health" })).toHaveFocus());

    view.rerender(
      <ConversationList
        {...baseProps}
        conversations={[conversations[0]]}
        groupedConversations={[{ label: "Today", conversations: [conversations[0]] }]}
        selectedId={conversations[0].id}
        onDelete={onDelete}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Delete Node health" }));
    await user.click(screen.getByRole("button", { name: "Delete conversation" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "New chat" })).toHaveFocus());
  });

  it("uses non-modal delete focus, cancels with Escape, and restores focus after failure", async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn(async () => {
      throw new Error("secret delete detail");
    });
    render(<ConversationList {...baseProps} onDelete={onDelete} />);

    const trigger = screen.getByRole("button", { name: "Delete Node health" });
    await user.click(trigger);
    let dialog = screen.getByRole("group", { name: "Delete Node health?" });
    const cancel = within(dialog).getByRole("button", { name: "Cancel delete" });
    const confirm = within(dialog).getByRole("button", { name: "Delete conversation" });
    expect(cancel).toHaveFocus();
    await user.tab();
    expect(confirm).toHaveFocus();
    await user.tab();
    expect(dialog).not.toContainElement(document.activeElement as HTMLElement);
    await user.tab({ shift: true });
    expect(confirm).toHaveFocus();
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.getByRole("button", { name: "Delete Node health" })).toHaveFocus());

    await user.click(screen.getByRole("button", { name: "Delete Node health" }));
    dialog = screen.getByRole("group", { name: "Delete Node health?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete conversation" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not delete this conversation.");
    expect(
      within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", {
        name: "Delete conversation",
      }),
    ).toHaveFocus();
    expect(document.activeElement).toBe(
      within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", {
        name: "Delete conversation",
      }),
    );
  });

  it("does not publish action failure or schedule focus after an unmount rejection", async () => {
    const user = userEvent.setup();
    let rejectDelete!: (reason?: unknown) => void;
    const onDelete = vi.fn(
      () =>
        new Promise<string | null>((_resolve, reject) => {
          rejectDelete = reject;
        }),
    );
    const focusFrame = vi.spyOn(window, "requestAnimationFrame");
    const view = render(<ConversationList {...baseProps} onDelete={onDelete} />);
    await user.click(screen.getByRole("button", { name: "Delete Node health" }));
    await user.click(
      within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", {
        name: "Delete conversation",
      }),
    );
    expect(onDelete).toHaveBeenCalledOnce();

    view.unmount();
    await act(async () => rejectDelete(new DOMException("aborted", "AbortError")));
    expect(focusFrame).not.toHaveBeenCalled();
    focusFrame.mockRestore();
  });

  it("keeps all actions keyboard reachable with 44px targets and canonical accessibility CSS", async () => {
    const user = userEvent.setup();
    render(<ConversationList {...baseProps} />);
    await user.tab();
    expect(screen.getByRole("button", { name: "New chat" })).toHaveFocus();

    const css = readFileSync(resolve(process.cwd(), "src/chat/ConversationList.module.css"), "utf8");
    expect(css).toMatch(/\.rail\s*\{[^}]*height:\s*100%/s);
    expect(css).toContain("var(--loxa-component-minimum-interactive-target)");
    expect(css).toContain("@media (prefers-contrast: more)");
    expect(css).toContain("@media (forced-colors: active)");
    expect(css).toContain("@media (prefers-reduced-motion: reduce)");
    expect(css).not.toMatch(/#[0-9a-f]{3,8}\b/i);
    expect(css).toMatch(/grid-template-columns:\s*minmax\(0, 1fr\)\s+[^;]+/);
    expect(css).toMatch(/\.conversationButton\[aria-current="page"\]::before/);
  });

  it("uses only variables defined by the canonical Loxa token sheet", () => {
    const css = readFileSync(resolve(process.cwd(), "src/chat/ConversationList.module.css"), "utf8");
    const canonical = readFileSync(resolve(process.cwd(), "src/styles/loxa.css"), "utf8");
    const definitions = new Set(Array.from(canonical.matchAll(/(--loxa-[a-z0-9-]+)\s*:/gi), ([, name]) => name));
    const undefinedReferences = Array.from(css.matchAll(/var\((--loxa-[a-z0-9-]+)/gi), ([, name]) => name).filter(
      (name) => !definitions.has(name),
    );

    expect(undefinedReferences).toEqual([]);
  });
});
