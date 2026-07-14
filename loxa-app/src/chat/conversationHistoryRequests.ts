import { CONVERSATION_HISTORY_PAGE_SIZE, type ConversationHistoryServices } from "./conversationHistory";
import type { ChatPage } from "./historyClient";

type RequestServices = Pick<ConversationHistoryServices, "readControlToken" | "listChats">;
type PageResult = { kind: "page"; page: ChatPage };
type RepeatedCursorResult = { kind: "repeated-cursor"; page: ChatPage };
export type ConversationHistoryPageResult = PageResult | RepeatedCursorResult | { kind: "invalidated" };

class ConversationHistoryInvalidatedError extends Error {
  constructor() {
    super("Conversation history request was invalidated.");
    this.name = "ConversationHistoryInvalidatedError";
  }
}

export function isConversationHistoryInvalidated(error: unknown): boolean {
  return error instanceof ConversationHistoryInvalidatedError;
}

export class ConversationHistoryRequests {
  private generation = 0;
  private readonly controllers = new Set<AbortController>();
  private pageFlight: Promise<ConversationHistoryPageResult> | null = null;
  private cursor: string | null = null;
  private seenCursors = new Set<string>();
  private pageFailed = false;
  private failedBefore: string | null = null;

  constructor(
    private readonly services: RequestServices,
    private readonly endpoint: string,
  ) {}

  get nextCursor(): string | null {
    return this.cursor;
  }

  get hasMore(): boolean {
    return this.cursor !== null;
  }

  async loadPage(before: string | null): Promise<ConversationHistoryPageResult> {
    if (this.pageFlight) return this.pageFlight;
    const generation = this.generation;
    const controller = this.ownController();
    const flight = this.requestPage(before, generation, controller);
    this.pageFlight = flight;
    void flight.then(
      () => this.detachPageFlight(flight),
      () => this.detachPageFlight(flight),
    );
    return flight;
  }

  async exhaust(onPage: (page: ChatPage) => void): Promise<ConversationHistoryPageResult> {
    let outcome: ConversationHistoryPageResult = { kind: "page", page: { chats: [], nextBefore: null } };
    while (this.cursor !== null) {
      outcome = await this.loadPage(this.cursor);
      if (outcome.kind === "invalidated") return outcome;
      onPage(outcome.page);
      if (outcome.kind === "repeated-cursor") return outcome;
    }
    return outcome;
  }

  async retry(): Promise<ConversationHistoryPageResult | null> {
    const retryFailedPage = this.pageFailed;
    const retryBefore = this.failedBefore;
    this.pageFailed = false;
    if (retryFailedPage) {
      if (retryBefore === null) this.seenCursors = new Set();
      return this.loadPage(retryBefore);
    }
    if (this.cursor !== null) return this.loadPage(this.cursor);
    return null;
  }

  async runAction<T>(operation: (token: string, signal: AbortSignal) => Promise<T>): Promise<T> {
    const generation = this.generation;
    const controller = this.ownController();
    try {
      const token = await this.services.readControlToken(this.endpoint);
      this.assertCurrent(generation, controller);
      const result = await operation(token, controller.signal);
      this.assertCurrent(generation, controller);
      return result;
    } catch (error) {
      if (!this.isCurrent(generation, controller)) throw new ConversationHistoryInvalidatedError();
      throw error;
    } finally {
      this.controllers.delete(controller);
    }
  }

  invalidate(): void {
    this.generation += 1;
    for (const controller of this.controllers) controller.abort();
    this.controllers.clear();
    this.pageFlight = null;
    this.cursor = null;
    this.seenCursors = new Set();
    this.pageFailed = false;
    this.failedBefore = null;
  }

  private async requestPage(
    before: string | null,
    generation: number,
    controller: AbortController,
  ): Promise<ConversationHistoryPageResult> {
    try {
      const token = await this.services.readControlToken(this.endpoint);
      if (!this.isCurrent(generation, controller)) return { kind: "invalidated" };
      const page = await this.services.listChats(
        this.endpoint,
        token,
        before === null ? { limit: CONVERSATION_HISTORY_PAGE_SIZE } : { limit: CONVERSATION_HISTORY_PAGE_SIZE, before },
        { signal: controller.signal },
      );
      if (!this.isCurrent(generation, controller)) return { kind: "invalidated" };
      this.cursor = page.nextBefore;
      if (page.nextBefore !== null) {
        if (this.seenCursors.has(page.nextBefore)) {
          this.cursor = null;
          this.pageFailed = true;
          this.failedBefore = before;
          return { kind: "repeated-cursor", page };
        }
        this.seenCursors.add(page.nextBefore);
      }
      this.pageFailed = false;
      this.failedBefore = null;
      return { kind: "page", page };
    } catch (error) {
      if (!this.isCurrent(generation, controller)) return { kind: "invalidated" };
      this.pageFailed = true;
      this.failedBefore = before;
      throw error;
    } finally {
      this.controllers.delete(controller);
    }
  }

  private ownController(): AbortController {
    const controller = new AbortController();
    this.controllers.add(controller);
    return controller;
  }

  private isCurrent(generation: number, controller: AbortController): boolean {
    return generation === this.generation && !controller.signal.aborted;
  }

  private assertCurrent(generation: number, controller: AbortController): void {
    if (!this.isCurrent(generation, controller)) throw new ConversationHistoryInvalidatedError();
  }

  private detachPageFlight(flight: Promise<ConversationHistoryPageResult>): void {
    if (this.pageFlight === flight) this.pageFlight = null;
  }
}
