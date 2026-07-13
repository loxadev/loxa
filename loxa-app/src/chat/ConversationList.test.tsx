import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ConversationList, type ConversationListItem } from "./ConversationList";

const conversations: ConversationListItem[] = [
  { id: "0123456789abcdef0123456789abcdef", title: "Node health", createdAtMs: 1, updatedAtMs: 20, terminalState: "completed" },
  { id: "1123456789abcdef0123456789abcdef", title: "Download model", createdAtMs: 1, updatedAtMs: 10, terminalState: "failed" },
];

const baseProps = {
  conversations,
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

  it("provides loading, empty, error, and paginated states without color-only meaning", async () => {
    const user = userEvent.setup();
    const onLoadMore = vi.fn();
    const { rerender } = render(<ConversationList {...baseProps} conversations={[]} state="loading" />);
    expect(screen.getByRole("status")).toHaveTextContent("Loading conversations");

    rerender(<ConversationList {...baseProps} conversations={[]} state="ready" />);
    expect(screen.getByText("No conversations yet.")).toBeVisible();

    rerender(<ConversationList {...baseProps} conversations={[]} state="error" errorMessage="History is unavailable." />);
    expect(screen.getByRole("alert")).toHaveTextContent("History is unavailable.");

    rerender(<ConversationList {...baseProps} hasMore onLoadMore={onLoadMore} />);
    await user.click(screen.getByRole("button", { name: "Load more conversations" }));
    expect(onLoadMore).toHaveBeenCalledOnce();
  });

  it("awaits create and load-more actions, prevents duplicates, and catches failures", async () => {
    const user = userEvent.setup();
    let rejectCreate!: (reason?: unknown) => void;
    const onCreate = vi.fn(() => new Promise<void>((_resolve, reject) => { rejectCreate = reject; }));
    const onLoadMore = vi.fn(async () => { throw new Error("private failure"); });
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

  it("requires explicit delete confirmation, supports cancel, and restores focus", async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn(async () => undefined);
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
    await user.click(within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", { name: "Delete conversation" }));
    expect(onDelete).toHaveBeenCalledWith(conversations[0].id);
    await waitFor(() => expect(screen.getByRole("button", { name: "New chat" })).toHaveFocus());
  });

  it("uses non-modal delete focus, cancels with Escape, and restores focus after failure", async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn(async () => { throw new Error("secret delete detail"); });
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
    expect(within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", { name: "Delete conversation" })).toHaveFocus();
    expect(document.activeElement).toBe(within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", { name: "Delete conversation" }));
  });

  it("does not publish action failure or schedule focus after an unmount rejection", async () => {
    const user = userEvent.setup();
    let rejectDelete!: (reason?: unknown) => void;
    const onDelete = vi.fn(() => new Promise<void>((_resolve, reject) => { rejectDelete = reject; }));
    const focusFrame = vi.spyOn(window, "requestAnimationFrame");
    const view = render(<ConversationList {...baseProps} onDelete={onDelete} />);
    await user.click(screen.getByRole("button", { name: "Delete Node health" }));
    await user.click(within(screen.getByRole("group", { name: "Delete Node health?" })).getByRole("button", { name: "Delete conversation" }));
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
  });
});
