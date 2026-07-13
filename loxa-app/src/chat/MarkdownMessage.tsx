import type { ComponentProps, ComponentPropsWithoutRef } from "react";

import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";

import styles from "./ChatScreen.module.css";

const controlCharacter = /[\u0000-\u001F\u007F]/;
const encodedControlCharacter = /%(?:0[0-9a-f]|1[0-9a-f]|7f)/i;
const whitespace = /\s/;
const markdownEncoder = new TextEncoder();
const MAX_MARKDOWN_SOURCE_BYTES = 2 * 1024 * 1024;
type RemarkPluginList = NonNullable<ComponentProps<typeof ReactMarkdown>["remarkPlugins"]>;
const GFM_PLUGINS: ReadonlyArray<RemarkPluginList[number]> = Object.freeze([remarkGfm]);

export function MarkdownMessage({ content }: { content: string }) {
  if (!isMarkdownSourceWithinLimit(content)) {
    return (
      <div className={styles.markdownMessage}>
        <p className={styles.markdownFallback} role="status">
          This response is too large to render safely. Use Copy response to copy the full text.
        </p>
      </div>
    );
  }

  return (
    <div className={styles.markdownMessage}>
      <ReactMarkdown
        components={components}
        remarkPlugins={GFM_PLUGINS as RemarkPluginList}
        skipHtml
        urlTransform={safeHttpUrl}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
}

const components: Components = {
  a: ({ children, href }: ComponentPropsWithoutRef<"a">) => {
    const safeUrl = safeHttpUrl(href);
    if (safeUrl === null) return <span>{children}</span>;

    return <a href={safeUrl} target="_blank" rel="noopener noreferrer">{children}</a>;
  },
  img: ({ alt }: ComponentPropsWithoutRef<"img">) => (
    <span className={styles.omittedImage}>{alt || "Image omitted."}</span>
  ),
};

function safeHttpUrl(value: string | undefined): string | null {
  if (!value || controlCharacter.test(value) || encodedControlCharacter.test(value) || whitespace.test(value)) return null;

  try {
    const parsed = new URL(value);
    if ((parsed.protocol !== "http:" && parsed.protocol !== "https:") || !parsed.hostname || parsed.username || parsed.password) {
      return null;
    }
    return parsed.href;
  } catch {
    return null;
  }
}

function isMarkdownSourceWithinLimit(content: string): boolean {
  if (content.length > MAX_MARKDOWN_SOURCE_BYTES) return false;
  return markdownEncoder.encode(content).byteLength <= MAX_MARKDOWN_SOURCE_BYTES;
}
