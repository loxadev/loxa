import { createRef } from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChatComposer } from "./ChatComposer";

describe("ChatComposer", () => {
  it("uses compact document, tools, context, and send controls", () => {
    render(
      <ChatComposer
        input="Hello"
        inputRef={createRef<HTMLTextAreaElement>()}
        canCompose
        responseInProgress={false}
        attachmentReason="Documents are unavailable."
        toolsReason="Tool use is unavailable."
        contextUsage={null}
        onInput={vi.fn()}
        onSend={vi.fn()}
        onStop={vi.fn()}
      />,
    );

    expect(screen.getByRole("button", { name: "Attach document" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Tools" })).toBeInTheDocument();
    expect(screen.getByText("Context unavailable")).toBeInTheDocument();
    const send = screen.getByRole("button", { name: "Send message" });
    expect(send.querySelector(".lucide-send-horizontal")).toBeInTheDocument();
  });
});
