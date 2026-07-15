import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { ModelInventoryEntry } from "../control/contracts";
import { ChatModelControl } from "./ChatModelControl";

const models = [model("Gemma 3 4B"), model("Qwen 2.5 Coder 7B")];

describe("ChatModelControl", () => {
  it("uses a compact top-bar trigger and opens the searchable local model picker", async () => {
    const user = userEvent.setup();
    renderControl();

    expect(screen.queryByRole("searchbox", { name: "Search downloaded models" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Choose model" }));

    expect(screen.getByRole("dialog", { name: "Choose a model" })).toBeInTheDocument();
    expect(screen.getByText("On this Mac")).toBeInTheDocument();
    expect(screen.getByRole("searchbox", { name: "Search downloaded models" })).toHaveFocus();
    expect(screen.getByRole("option", { name: "Gemma 3 4B" })).toBeInTheDocument();
  });

  it("exposes the picker from a disclosure button", () => {
    renderControl();

    expect(screen.getByRole("button", { name: "Choose model" })).toHaveAttribute("aria-haspopup", "dialog");
  });

  it("navigates filtered model options with arrow keys and selects with Enter", async () => {
    const user = userEvent.setup();
    const onSelectedModel = vi.fn();
    renderControl({ onSelectedModel });

    await user.click(screen.getByLabelText("Choose model"));
    const search = screen.getByRole("searchbox", { name: "Search downloaded models" });
    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("option", { name: "Gemma 3 4B" })).toHaveFocus();

    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("option", { name: "Qwen 2.5 Coder 7B" })).toHaveFocus();

    await user.keyboard("{ArrowUp}");
    expect(screen.getByRole("option", { name: "Gemma 3 4B" })).toHaveFocus();
    await user.keyboard("{Enter}");

    expect(onSelectedModel).toHaveBeenCalledWith("Gemma 3 4B");
    expect(search).toBeInTheDocument();
  });

  it("closes on Escape and restores focus to the trigger", async () => {
    const user = userEvent.setup();
    renderControl();
    const trigger = screen.getByLabelText("Choose model");

    await user.click(trigger);
    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog", { name: "Choose a model" })).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it("dismisses the picker when a pointer press occurs outside it", async () => {
    const user = userEvent.setup();
    renderControl();

    await user.click(screen.getByLabelText("Choose model"));
    await user.click(document.body);

    expect(screen.queryByRole("dialog", { name: "Choose a model" })).not.toBeInTheDocument();
  });

  it("opens with Command-L and routes unmatched searches to model discovery", async () => {
    const user = userEvent.setup();
    const onBrowseModels = vi.fn();
    renderControl({ onBrowseModels });

    await user.keyboard("{Meta>}l{/Meta}");
    await user.type(screen.getByRole("searchbox", { name: "Search downloaded models" }), "nemotron");
    await user.click(screen.getByRole("button", { name: "Search Hugging Face for nemotron" }));

    expect(onBrowseModels).toHaveBeenCalledOnce();
  });

  it("ignores Command-L while model controls are unavailable", async () => {
    const user = userEvent.setup();
    renderControl({ modelControlsAvailable: false });

    await user.keyboard("{Meta>}l{/Meta}");

    expect(screen.queryByRole("dialog", { name: "Choose a model" })).not.toBeInTheDocument();
  });

  it("closes the picker when model controls become busy", async () => {
    const user = userEvent.setup();
    const view = renderControl();
    await user.click(screen.getByRole("button", { name: "Choose model" }));
    expect(screen.getByRole("dialog", { name: "Choose a model" })).toBeInTheDocument();

    view.rerender(<ChatModelControl {...view.props} modelBusy />);

    expect(screen.queryByRole("dialog", { name: "Choose a model" })).not.toBeInTheDocument();
  });

  it("keeps selection and load as separate actions", async () => {
    const user = userEvent.setup();
    const onSelectedModel = vi.fn();
    const onSwitchModel = vi.fn();
    renderControl({ onSelectedModel, onSwitchModel });

    await user.click(screen.getByRole("button", { name: "Choose model" }));
    await user.click(screen.getByRole("option", { name: "Gemma 3 4B" }));
    await user.click(screen.getByRole("button", { name: "Load Gemma 3 4B" }));

    expect(onSelectedModel).toHaveBeenCalledWith("Gemma 3 4B");
    expect(onSwitchModel).toHaveBeenCalledOnce();
  });
});

function renderControl(overrides: Partial<React.ComponentProps<typeof ChatModelControl>> = {}) {
  const props: React.ComponentProps<typeof ChatModelControl> = {
    title: "New Chat",
    activeModel: null,
    selectedModel: "",
    eligibleModels: models,
    status: "Node offline",
    guidance: "Start the node to load a model.",
    modelBusy: false,
    modelOperation: "idle",
    modelControlsAvailable: true,
    responseInProgress: false,
    canBrowseModels: true,
    onSelectedModel: vi.fn(),
    onSwitchModel: vi.fn(),
    onBrowseModels: vi.fn(),
    ...overrides,
  };
  return { ...render(<ChatModelControl {...props} />), props };
}

function model(id: string): ModelInventoryEntry {
  return {
    id,
    repo: `loxa/${id}`,
    revision: "main",
    filename: `${id}.gguf`,
    sizeBytes: 1,
    sha256: "0".repeat(64),
    engine: { engine: "llama-cpp", eligible: true, reason: "Eligible" },
    params: "4B",
    quant: "Q4_K_M",
    license: "test",
    minFreeMemoryGiB: 4,
    artifact: { kind: "downloaded" },
    compatibility: { compatible: true, reason: "Compatible" },
  };
}
