import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ChatTranscript, type ChatTurn } from "./ChatTranscript";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function transcriptWith(turn: Partial<ChatTurn>, copyText = vi.fn().mockResolvedValue(undefined)) {
  const complete: ChatTurn = {
    id: 1,
    model: "gemma",
    prompt: "Plain user prompt",
    response: "",
    status: "completed",
    error: "",
    ...turn,
  };

  return {
    ...render(<ChatTranscript turns={[complete]} emptyMessage="Empty" copyText={copyText} />),
    copyText,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
}

describe("assistant Markdown rendering", () => {
  it("renders assistant headings, lists, emphasis, inline code, and GFM tables", () => {
    transcriptWith({
      response:
        "# Runtime status\n\n- Model is **ready**\n- Alias is `loxa`\n\n| State | Value |\n| --- | --- |\n| Health | Ready |",
    });

    expect(screen.getByRole("heading", { name: "Runtime status" })).toBeInTheDocument();
    expect(screen.getByRole("list")).toHaveTextContent("Model is ready");
    expect(screen.getByText("ready", { selector: "strong" })).toBeInTheDocument();
    expect(screen.getByText("loxa", { selector: "code" })).toBeInTheDocument();
    expect(screen.getByRole("table")).toHaveTextContent("Health");
  });

  it("skips raw HTML rather than interpreting it", () => {
    transcriptWith({ response: "Before <button>Unsafe action</button> <script>bad()</script> after" });

    expect(screen.queryByRole("button", { name: "Unsafe action" })).not.toBeInTheDocument();
    expect(screen.queryByText("bad()", { selector: "script" })).not.toBeInTheDocument();
    expect(screen.getByText(/Before/)).toHaveTextContent("Before Unsafe action bad() after");
  });

  it("does not render assistant images", () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    transcriptWith({ response: "![Model diagram](https://example.com/model.png)" });

    expect(screen.queryByRole("img")).not.toBeInTheDocument();
    expect(screen.getByText("Model diagram")).toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled();
    fetch.mockRestore();
  });

  it("keeps unsafe assistant links inert while allowing credential-free absolute HTTPS links", () => {
    transcriptWith({
      response:
        "[Run](javascript:alert(1)) [Private](https://user:secret@example.com) [Status](https://loxa.dev/status)",
    });

    expect(screen.queryByRole("link", { name: "Run" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Private" })).not.toBeInTheDocument();
    const status = screen.getByRole("link", { name: "Status" });
    expect(status).toHaveAttribute("href", "https://loxa.dev/status");
    expect(status).toHaveAttribute("target", "_blank");
    expect(status).toHaveAttribute("rel", "noopener noreferrer");
  });

  it("keeps user prompts as literal text", () => {
    transcriptWith({ prompt: "# Not a heading and **not bold**", response: "Acknowledged." });

    expect(screen.queryByRole("heading", { name: "Not a heading and not bold" })).not.toBeInTheDocument();
    expect(screen.queryByText("not bold", { selector: "strong" })).not.toBeInTheDocument();
    expect(screen.getByText("# Not a heading and **not bold**")).toBeInTheDocument();
  });

  it.each([
    ["data URL", "[Unsafe](data:text/html,bad)"],
    ["file URL", "[Unsafe](file:///tmp/secret)"],
    ["custom URL", "[Unsafe](loxa://settings)"],
    ["relative URL", "[Unsafe](/settings)"],
    ["literal control character", "[Unsafe](https://loxa.dev/\u0007bad)"],
  ])("keeps %s links inert", (_case, response) => {
    transcriptWith({ response });

    expect(screen.queryByRole("link", { name: "Unsafe" })).not.toBeInTheDocument();
    expect(screen.getByText(/Unsafe/)).toBeInTheDocument();
  });

  it.each([
    ["encoded NUL", "[Unsafe](https://loxa.dev/%00bad)"],
    ["encoded unit separator", "[Unsafe](https://loxa.dev/%1fbad)"],
    ["encoded DEL", "[Unsafe](https://loxa.dev/%7Fbad)"],
    ["encoded JavaScript scheme", "[Unsafe](javascript%3Aalert(1))"],
    ["fully encoded HTTPS URL", "[Unsafe](%68%74%74%70%73%3A%2F%2Floxa.dev)"],
    ["custom plus scheme", "[Unsafe](loxa+desktop://settings)"],
    ["credential and encoded control", "[Unsafe](https://user:secret@loxa.dev/%00bad)"],
    ["encoded credential control", "[Unsafe](https://user%00:secret@loxa.dev/path)"],
  ])("rejects %s navigation", (_case, response) => {
    transcriptWith({ response });

    expect(screen.queryAllByRole("link")).toHaveLength(0);
  });

  it("renders the approved GFM and accessible prose surface", () => {
    transcriptWith({
      response:
        "> Node quote\n\n```rust\nlet ready = true;\n```\n\n- [x] Verified\n  - Nested\n\n~~stale~~\n\n<https://loxa.dev/status>\n\nUnicode: مرحبا 🦊\n\nLine one  \nLine two\n\nFootnote[^1]\n\n[^1]: Local note",
    });

    expect(screen.getByText("Node quote").closest("blockquote")).toBeInTheDocument();
    expect(screen.getByText("let ready = true;").closest("pre")).toBeInTheDocument();
    expect(screen.getByRole("checkbox")).toBeDisabled();
    expect(screen.getByText("Nested").closest("ul")).toBeInTheDocument();
    expect(screen.getByText("stale", { selector: "del" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "https://loxa.dev/status" })).toHaveAttribute(
      "href",
      "https://loxa.dev/status",
    );
    expect(screen.getByText(/مرحبا 🦊/)).toBeInTheDocument();
    const lineBreakParagraph = screen.getByText(
      (_content, element) => element?.tagName === "P" && element.textContent?.includes("Line one") === true,
    );
    expect(lineBreakParagraph.querySelector("br")).toBeInTheDocument();
    expect(screen.getByText(/Local note/)).toBeInTheDocument();
  });

  it("handles incomplete streamed Markdown and renders the valid completion", () => {
    const copyText = vi.fn().mockResolvedValue(undefined);
    const initial: ChatTurn = {
      id: 1,
      model: "gemma",
      prompt: "Plain",
      response: "**stream",
      status: "streaming",
      error: "",
    };
    const view = render(<ChatTranscript turns={[initial]} emptyMessage="Empty" copyText={copyText} />);

    expect(screen.getByText("**stream")).toBeInTheDocument();
    view.rerender(
      <ChatTranscript
        turns={[{ ...initial, response: "**stream**", status: "completed" }]}
        emptyMessage="Empty"
        copyText={copyText}
      />,
    );
    expect(screen.getByText("stream", { selector: "strong" })).toBeInTheDocument();
  });

  it("recovers an incomplete streamed fence when the closing fence arrives", () => {
    const copyText = vi.fn().mockResolvedValue(undefined);
    const initial: ChatTurn = {
      id: 1,
      model: "gemma",
      prompt: "Plain",
      response: "```rust\nlet ready = true;",
      status: "streaming",
      error: "",
    };
    const view = render(<ChatTranscript turns={[initial]} emptyMessage="Empty" copyText={copyText} />);

    expect(screen.getByText("let ready = true;").closest("pre")).toBeInTheDocument();
    view.rerender(
      <ChatTranscript
        turns={[{ ...initial, response: "```rust\nlet ready = true;\n```", status: "completed" }]}
        emptyMessage="Empty"
        copyText={copyText}
      />,
    );
    expect(screen.getByText("let ready = true;").closest("code")).toHaveClass("language-rust");
  });

  it("recovers an incomplete streamed table when the remaining cells arrive", () => {
    const copyText = vi.fn().mockResolvedValue(undefined);
    const initial: ChatTurn = {
      id: 1,
      model: "gemma",
      prompt: "Plain",
      response: "| State | Value |\n| ---",
      status: "streaming",
      error: "",
    };
    const view = render(<ChatTranscript turns={[initial]} emptyMessage="Empty" copyText={copyText} />);

    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    view.rerender(
      <ChatTranscript
        turns={[{ ...initial, response: "| State | Value |\n| --- | --- |\n| Health | Ready |", status: "completed" }]}
        emptyMessage="Empty"
        copyText={copyText}
      />,
    );
    expect(within(screen.getByRole("table")).getByText("Health")).toBeInTheDocument();
    expect(within(screen.getByRole("table")).getByText("Ready")).toBeInTheDocument();
  });

  it("bounds oversized restored Markdown before parsing", () => {
    transcriptWith({ response: "x".repeat(2 * 1024 * 1024 + 1) });

    expect(screen.getByRole("status")).toHaveTextContent("too large to render safely");
    expect(screen.queryByText(/^x+$/)).not.toBeInTheDocument();
  });

  it("handles bounded pathological nesting and long unbroken content", () => {
    transcriptWith({ response: `${"> ".repeat(128)}Nested\n\n${"a".repeat(32_768)}` });

    expect(screen.getByText("Nested")).toBeInTheDocument();
    expect(screen.getByText(/^a+$/)).toBeInTheDocument();
  });

  it("copies the full raw assistant source and announces success without interpreting hidden HTML", async () => {
    const user = userEvent.setup();
    const raw = "Before <script>unsafe()</script> **after**";
    const copyText = vi.fn().mockResolvedValue(undefined);
    transcriptWith({ response: raw }, copyText);

    const copy = screen.getByRole("button", { name: "Copy response" });
    expect(copy).toHaveClass("interactive-target");
    await user.click(copy);
    expect(copyText).toHaveBeenCalledWith(raw);
    expect(await screen.findByRole("status", { name: "Copy response status" })).toHaveTextContent("Response copied");
  });

  it("announces clipboard failure and keeps the response available", async () => {
    const user = userEvent.setup();
    const copyText = vi.fn().mockRejectedValue(new Error("clipboard unavailable"));
    transcriptWith({ response: "Keep me" }, copyText);

    await user.click(screen.getByRole("button", { name: "Copy response" }));
    expect(await screen.findByRole("status", { name: "Copy response status" })).toHaveTextContent("Copy failed");
    expect(screen.getByText("Keep me")).toBeInTheDocument();
  });

  it("activates Copy response with Tab plus Enter and Space", async () => {
    const user = userEvent.setup();
    const copyText = vi.fn().mockResolvedValue(undefined);
    transcriptWith({ response: "Keyboard source" }, copyText);

    await user.tab();
    expect(screen.getByRole("log", { name: "Conversation" })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole("button", { name: "Copy response" })).toHaveFocus();
    await user.keyboard("{Enter}");
    await user.keyboard(" ");
    expect(copyText).toHaveBeenNthCalledWith(1, "Keyboard source");
    expect(copyText).toHaveBeenNthCalledWith(2, "Keyboard source");
  });

  it.each(["queued", "streaming"] as const)("keeps Copy response disabled while the turn is %s", (status) => {
    const copyText = vi.fn().mockResolvedValue(undefined);
    transcriptWith({ response: "Partial", status }, copyText);

    expect(screen.getByRole("button", { name: "Copy response" })).toBeDisabled();
    expect(copyText).not.toHaveBeenCalled();
  });

  it.each(["completed", "cancelled", "failed"] as const)(
    "copies the exact raw terminal source for a %s turn",
    async (status) => {
      const user = userEvent.setup();
      const raw = `Partial ${status} <button>not interpreted</button>`;
      const copyText = vi.fn().mockResolvedValue(undefined);
      transcriptWith({ response: raw, status, error: status === "failed" ? "runtime stopped" : "" }, copyText);

      await user.click(screen.getByRole("button", { name: "Copy response" }));
      expect(copyText).toHaveBeenCalledWith(raw);
    },
  );

  it("ignores deferred clipboard settlement after the turn is removed", async () => {
    const clipboard = deferred<void>();
    const copyText = vi.fn(() => clipboard.promise);
    const turn: ChatTurn = {
      id: 1,
      model: "gemma",
      prompt: "Plain",
      response: "Old response",
      status: "completed",
      error: "",
    };
    const view = render(<ChatTranscript turns={[turn]} emptyMessage="Empty" copyText={copyText} />);
    await userEvent.setup().click(screen.getByRole("button", { name: "Copy response" }));

    view.rerender(<ChatTranscript turns={[]} emptyMessage="Empty" copyText={copyText} />);
    await act(async () => clipboard.resolve());
    expect(screen.queryByRole("status", { name: "Copy response status" })).not.toBeInTheDocument();
    expect(screen.getByText("Empty")).toBeInTheDocument();
  });

  it("ignores deferred clipboard settlement after transcript unmount", async () => {
    const clipboard = deferred<void>();
    const copyText = vi.fn(() => clipboard.promise);
    const view = transcriptWith({ response: "Unmounted response" }, copyText);
    await userEvent.setup().click(screen.getByRole("button", { name: "Copy response" }));

    view.unmount();
    await act(async () => clipboard.reject(new Error("late clipboard failure")));
    expect(screen.queryByRole("status", { name: "Copy response status" })).not.toBeInTheDocument();
  });

  it("ignores deferred clipboard settlement when the current turn content changes", async () => {
    const clipboard = deferred<void>();
    const copyText = vi.fn(() => clipboard.promise);
    const turn: ChatTurn = {
      id: 1,
      model: "gemma",
      prompt: "Plain",
      response: "Old response",
      status: "completed",
      error: "",
    };
    const view = render(<ChatTranscript turns={[turn]} emptyMessage="Empty" copyText={copyText} />);
    await userEvent.setup().click(screen.getByRole("button", { name: "Copy response" }));

    view.rerender(
      <ChatTranscript turns={[{ ...turn, response: "New response" }]} emptyMessage="Empty" copyText={copyText} />,
    );
    await act(async () => clipboard.resolve());
    expect(screen.queryByRole("status", { name: "Copy response status" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy response" })).toBeEnabled();
  });

  it("keeps the labelled log navigable and streaming output silent until terminal status", () => {
    const copyText = vi.fn().mockResolvedValue(undefined);
    const turn: ChatTurn = {
      id: 1,
      model: "gemma",
      prompt: "Plain",
      response: "Partial",
      status: "streaming",
      error: "",
    };
    const view = render(<ChatTranscript turns={[turn]} emptyMessage="Empty" copyText={copyText} />);
    const log = screen.getByRole("log", { name: "Conversation" });

    expect(log).toHaveAttribute("tabindex", "0");
    expect(log).toHaveAttribute("aria-live", "off");
    expect(log).toHaveAttribute("aria-relevant", "additions");
    const response = screen.getByRole("region", { name: "Assistant response from gemma" });
    expect(response).toHaveAttribute("aria-live", "off");
    expect(response).toHaveAttribute("aria-busy", "true");
    expect(within(log).queryByRole("status")).not.toBeInTheDocument();

    view.rerender(
      <ChatTranscript turns={[{ ...turn, status: "completed" }]} emptyMessage="Empty" copyText={copyText} />,
    );
    expect(screen.getByRole("region", { name: "Assistant response from gemma" })).toHaveAttribute("aria-busy", "false");
  });

  it("uses one polite Copy response announcement path inside an inactive log", async () => {
    const user = userEvent.setup();
    transcriptWith({ response: "Copy source" });
    await user.click(screen.getByRole("button", { name: "Copy response" }));

    const log = screen.getByRole("log", { name: "Conversation" });
    const copyStatus = within(log).getByRole("status", { name: "Copy response status" });
    expect(log).toHaveAttribute("aria-live", "off");
    expect(copyStatus).not.toHaveAttribute("aria-live");
    expect(within(log).getAllByRole("status")).toHaveLength(1);
  });

  it("keeps the Markdown policy immutable and excludes unsafe renderer hooks", () => {
    const source = readFileSync(resolve(process.cwd(), "src/chat/MarkdownMessage.tsx"), "utf8");

    expect(source).toMatch(/Object\.freeze/);
    expect(source).not.toMatch(/dangerouslySetInnerHTML|rehype-raw|rehypeRaw|MDX|syntaxHighlighter/i);
  });

  it("defines overflow, zoom, contrast, forced-color, reduced-motion, and selection contracts", () => {
    const css = readFileSync(resolve(process.cwd(), "src/chat/ChatTranscript.module.css"), "utf8");

    expect(css).toMatch(/\.markdownMessage pre[\s\S]*overflow-x:\s*auto/);
    expect(css).toMatch(/\.markdownMessage table[\s\S]*overflow-x:\s*auto/);
    expect(css).toMatch(/overflow-wrap:\s*anywhere/);
    expect(css).toMatch(/user-select:\s*text/);
    expect(css).toMatch(/@media \(max-width:/);
    expect(css).toMatch(/@media \(prefers-contrast: more\)/);
    expect(css).toMatch(/@media \(forced-colors: active\)/);
    expect(css).toMatch(/@media \(prefers-reduced-motion: reduce\)/);
    expect(css).not.toMatch(/font-size:\s*\d+px/);
    expect(css).toMatch(/\.copyResponseButton[\s\S]*min-width:\s*var\(--loxa-component-minimum-interactive-target\)/);
    expect(css).toMatch(/\.copyResponseButton[\s\S]*min-height:\s*var\(--loxa-component-minimum-interactive-target\)/);
    const canonicalCss = readFileSync(resolve(process.cwd(), "src/styles/loxa.css"), "utf8");
    expect(canonicalCss).toMatch(/--loxa-component-minimum-interactive-target:\s*44px/);
  });
});
