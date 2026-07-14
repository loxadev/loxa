import { describe, expect, it } from "vitest";

import type { ChatSummary } from "./historyClient";
import { groupConversations, orderConversations } from "./conversationHistory";

const chat = (id: string, updatedAtMs: number, title = id): ChatSummary => ({
  id,
  title,
  createdAtMs: updatedAtMs - 1,
  updatedAtMs,
});

describe("conversation history data", () => {
  it("deduplicates by ID, keeps the newest summary, and orders stably", () => {
    expect(orderConversations([chat("b", 10, "old b"), chat("a", 20), chat("b", 30, "new b"), chat("c", 20)])).toEqual([
      chat("b", 30, "new b"),
      chat("c", 20),
      chat("a", 20),
    ]);
  });

  it("groups exact local calendar boundaries in fixed non-empty order", () => {
    const now = new Date(2026, 6, 14, 12, 0, 0);
    const local = (daysAgo: number, hour = 10) => new Date(2026, 6, 14 - daysAgo, hour, 0, 0).getTime();
    const conversations = [
      chat("today", local(0)),
      chat("yesterday", local(1)),
      chat("day2", local(2)),
      chat("day7", local(7)),
      chat("older", local(8)),
    ];

    expect(groupConversations(conversations, now)).toEqual([
      { label: "Today", conversations: [conversations[0]] },
      { label: "Yesterday", conversations: [conversations[1]] },
      { label: "Previous 7 days", conversations: [conversations[2], conversations[3]] },
      { label: "Older", conversations: [conversations[4]] },
    ]);
  });

  it("uses local Date construction across DST-shaped calendar days", () => {
    const now = new Date(2026, 2, 9, 0, 30);
    const yesterdayLate = new Date(2026, 2, 8, 23, 30).getTime();

    expect(groupConversations([chat("dst", yesterdayLate)], now)[0]?.label).toBe("Yesterday");
  });
});
