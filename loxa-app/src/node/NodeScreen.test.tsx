import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { NodeScreen, type NodeScreenServices } from "./NodeScreen";
import { NodeSessionProvider, type BootstrapSnapshot, type NodeSessionServices, useNodeSession } from "./NodeSession";
import { NodeTable } from "./NodeTable";
import { controlSnapshot, scriptedV2Control, servicesWithControl } from "./testSupport";
import { v2Ids } from "../control/testSupport";

const endpoint = "http://127.0.0.1:8080";
const unloadedStatus = {
  node_id: "node-7",
  health: "unavailable" as const,
  model: "loxa" as const,
  engine: null,
  runtime_model: null,
  profile: null,
};

function snapshot(overrides: Partial<BootstrapSnapshot> = {}): BootstrapSnapshot {
  return { ownership: "owned", endpoint, childRunning: true, error: null, ...overrides };
}

function services(overrides: Partial<NodeSessionServices & NodeScreenServices> = {}) {
  return {
    ...servicesWithControl(),
    getStatus: vi.fn().mockResolvedValue(unloadedStatus),
    copyText: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

function renderNode(api = services(), onNavigateModels = vi.fn()) {
  return {
    api,
    ...render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <NodeScreen services={api} onNavigateModels={onNavigateModels} />
      </NodeSessionProvider>,
    ),
  };
}

function activeModelCell() {
  const row = within(screen.getByRole("table", { name: "Local node inventory" })).getAllByRole("row")[1];
  return within(row).getAllByRole("cell")[2];
}

function expectActiveModelUnavailable() {
  const cell = activeModelCell();
  expect(cell.firstElementChild).toHaveTextContent("—");
  expect(cell.firstElementChild?.textContent).toBe("—");
  expect(cell).not.toHaveTextContent("No model loaded");
}

function statusBadge(label: string) {
  return screen.getByText(label, { selector: '[data-slot="status-badge"]' });
}

function findStatusBadge(label: string) {
  return screen.findByText(label, { selector: '[data-slot="status-badge"]' });
}

function ReconcileControl() {
  const session = useNodeSession();
  return (
    <button type="button" onClick={() => session.invalidateModelTruth()}>
      Invalidate model truth
    </button>
  );
}

describe("NodeScreen", () => {
  it("renders a row-oriented inventory without changing the scalar screen truth", () => {
    const first = {
      rowId: "local-node",
      name: "Local node",
      kind: "Local",
      nodeId: "node-7",
      statusLabel: "Ready",
      statusTone: "success" as const,
      activeModel: "gemma-3-4b-it-q4",
      engineName: "llama.cpp",
      engineVersion: "b9999",
      profile: "default",
      endpoint,
      ownership: "App-owned node",
    };
    render(
      <NodeTable
        rows={[
          first,
          { ...first, nodeId: "node-8", endpoint: "http://127.0.0.1:8081", ownership: "Externally attached" },
        ]}
      />,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).getAllByRole("row")).toHaveLength(3);
    expect(within(table).getByText("node-8")).toBeVisible();
  });

  it("selects inventory rows through a named keyboard-operable control", async () => {
    const user = userEvent.setup();
    const onSelectRow = vi.fn();
    const first = {
      rowId: "local-node",
      name: "Local node",
      kind: "Local",
      nodeId: "node-7",
      statusLabel: "Ready",
      statusTone: "success" as const,
      activeModel: "gemma-3-4b-it-q4",
      engineName: "llama.cpp",
      engineVersion: "b9999",
      profile: "default",
      endpoint,
      ownership: "App-owned node",
    };
    render(
      <NodeTable
        rows={[first, { ...first, rowId: "future-node", name: "Future node", nodeId: "node-8" }]}
        selectedRowId="local-node"
        onSelectRow={onSelectRow}
      />,
    );

    const local = screen.getByRole("button", { name: "Select Local node" });
    const future = screen.getByRole("button", { name: "Select Future node" });
    expect(local).toHaveAttribute("aria-pressed", "true");
    expect(future).toHaveAttribute("aria-pressed", "false");
    future.focus();
    await user.keyboard("{Enter}");
    expect(onSelectRow).toHaveBeenCalledWith("future-node");
  });

  it("omits the Actions column and cells when actions are absent", () => {
    render(
      <NodeTable
        rowId="local-node"
        name="Local node"
        kind="Local"
        nodeId="node-7"
        statusLabel="Ready"
        statusTone="success"
        activeModel="gemma-3-4b-it-q4"
        engineName="llama.cpp"
        engineVersion="b9999"
        profile="default"
        endpoint={endpoint}
        ownership="App-owned node"
      />,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["Node", "Status", "Active model", "Engine", "Version", "Profile", "Endpoint", "Ownership"]);
    expect(within(table).getAllByRole("cell")).toHaveLength(8);
  });

  it("omits the Actions column when every provided action slot is empty", () => {
    render(
      <NodeTable
        rowId="local-node"
        name="Local node"
        kind="Local"
        nodeId="node-7"
        statusLabel="Ready"
        statusTone="success"
        activeModel="gemma-3-4b-it-q4"
        engineName="llama.cpp"
        engineVersion="b9999"
        profile="default"
        endpoint={endpoint}
        ownership="App-owned node"
        actions={{ copyEndpoint: null, model: undefined, retry: null, lifecycle: undefined }}
      />,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).queryByRole("columnheader", { name: "Actions" })).not.toBeInTheDocument();
    expect(within(table).getAllByRole("cell")).toHaveLength(8);
  });

  it("omits the Actions column when every action slot is false", () => {
    render(
      <NodeTable
        rowId="local-node"
        name="Local node"
        kind="Local"
        nodeId="node-7"
        statusLabel="Ready"
        statusTone="success"
        activeModel="gemma-3-4b-it-q4"
        engineName="llama.cpp"
        engineVersion="b9999"
        profile="default"
        endpoint={endpoint}
        ownership="App-owned node"
        actions={{ copyEndpoint: false, model: false, retry: false, lifecycle: false }}
      />,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).queryByRole("columnheader", { name: "Actions" })).not.toBeInTheDocument();
    expect(within(table).getAllByRole("cell")).toHaveLength(8);
  });

  it("omits the Actions column when action slots contain only empty arrays", () => {
    render(
      <NodeTable
        rowId="local-node"
        name="Local node"
        kind="Local"
        nodeId="node-7"
        statusLabel="Ready"
        statusTone="success"
        activeModel="gemma-3-4b-it-q4"
        engineName="llama.cpp"
        engineVersion="b9999"
        profile="default"
        endpoint={endpoint}
        ownership="App-owned node"
        actions={{ copyEndpoint: [], model: [], retry: [], lifecycle: [] }}
      />,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).queryByRole("columnheader", { name: "Actions" })).not.toBeInTheDocument();
    expect(within(table).getAllByRole("cell")).toHaveLength(8);
  });

  it("shows the Actions column when mixed action slots include a renderable child", () => {
    render(
      <NodeTable
        rowId="local-node"
        name="Local node"
        kind="Local"
        nodeId="node-7"
        statusLabel="Ready"
        statusTone="success"
        activeModel="gemma-3-4b-it-q4"
        engineName="llama.cpp"
        engineVersion="b9999"
        profile="default"
        endpoint={endpoint}
        ownership="App-owned node"
        actions={{
          copyEndpoint: [false, null, <button key="copy">Copy</button>],
          model: [],
          retry: undefined,
          lifecycle: false,
        }}
      />,
    );

    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(within(table).getByRole("columnheader", { name: "Actions" })).toBeVisible();
    expect(within(table).getByRole("button", { name: "Copy" })).toBeVisible();
    expect(within(table).getAllByRole("cell")).toHaveLength(9);
  });

  it("keeps the status badge visual-only", () => {
    render(
      <NodeTable
        rowId="local-node"
        name="Local node"
        kind="Local"
        nodeId="node-7"
        statusLabel="Ready"
        statusTone="success"
        activeModel="gemma-3-4b-it-q4"
        engineName="llama.cpp"
        engineVersion="b9999"
        profile="default"
        endpoint={endpoint}
        ownership="App-owned node"
      />,
    );

    const badge = screen.getByText("Ready", { selector: '[data-slot="status-badge"]' });
    expect(badge).not.toHaveAttribute("role");
    expect(badge).not.toHaveAttribute("aria-live");
  });

  it("presents one truthful local node row without unsupported inventory controls", async () => {
    renderNode();

    expect(await screen.findByText("Node ready — no model loaded")).toBeVisible();
    const table = screen.getByRole("table", { name: "Local node inventory" });
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["Node", "Status", "Active model", "Engine", "Version", "Profile", "Endpoint", "Ownership", "Actions"]);
    const rows = within(table).getAllByRole("row");
    expect(rows).toHaveLength(2);
    const localNode = rows[1];
    expect(within(localNode).getByText("Local node")).toBeVisible();
    expect(within(localNode).getByText(v2Ids.node)).toBeVisible();
    expect(within(localNode).getByText("Node ready — no model loaded")).toBeVisible();
    expect(within(localNode).queryByText("unavailable")).not.toBeInTheDocument();
    expect(within(localNode).getByText("No model loaded")).toBeVisible();
    expect(within(localNode).getByText(endpoint)).toBeVisible();
    expect(within(localNode).getByText("App-owned node")).toBeVisible();
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /add node/i })).not.toBeInTheDocument();
  });

  it("renders a selected-node runtime summary from authoritative node fields", async () => {
    const control = scriptedV2Control(
      controlSnapshot({ slot: { status: "ready", model_id: "gemma-3-4b-it-q4", operation_id: null } }),
    );
    renderNode({ ...servicesWithControl(control), copyText: vi.fn() });

    await findStatusBadge("Ready");
    const summary = screen.getByRole("region", { name: "Selected node runtime" });
    expect(within(summary).getByRole("heading", { name: "Local node runtime" })).toBeVisible();
    for (const value of [v2Ids.node, endpoint, "gemma-3-4b-it-q4"]) {
      expect(within(summary).getByText(value)).toBeVisible();
    }
    expect(within(summary).getAllByText("Unavailable")).toHaveLength(3);
  });

  it("shows truthful unavailable runtime values and an unsupported developer-log state", async () => {
    renderNode();

    await screen.findByText("Node ready — no model loaded");
    const summary = screen.getByRole("region", { name: "Selected node runtime" });
    expect(within(summary).getByText("No model loaded")).toBeVisible();
    expect(within(summary).getAllByText("Unavailable")).toHaveLength(3);

    const logs = screen.getByRole("region", { name: "Developer logs unavailable" });
    expect(
      within(logs).getByText("Developer logs are unavailable because this backend does not expose a log source."),
    ).toBeVisible();
    expect(logs).not.toHaveTextContent(/tok\/s|latency|memory|gpu/i);
  });

  it("automatically ensures the node and renders unloaded as a successful state", async () => {
    const navigate = vi.fn();
    const { api } = renderNode(services(), navigate);
    expect(await screen.findByText("Node ready — no model loaded")).toBeInTheDocument();
    expect(api.bootstrap.start).toHaveBeenCalledWith({ endpoint });
    expect(screen.getByText("App-owned node")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop node" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: /attach/i })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Browse verified models" }));
    expect(navigate).toHaveBeenCalledTimes(1);
  });

  it("renders starting and recovery-required as live state", async () => {
    const pending = new Promise<BootstrapSnapshot>(() => undefined);
    const first = renderNode(
      services({
        bootstrap: { ...services().bootstrap, start: vi.fn(() => pending) },
      }),
    );
    expect(await findStatusBadge("Starting")).toBeVisible();
    expectActiveModelUnavailable();
    expect(screen.queryByRole("button", { name: "Browse verified models" })).not.toBeInTheDocument();
    first.unmount();

    renderNode(
      services({
        bootstrap: {
          ...services().bootstrap,
          start: vi.fn().mockRejectedValue(new Error("Recovery required after unsafe child exit.")),
        },
      }),
    );
    const status = await findStatusBadge("Recovery required");
    expect(status).toHaveTextContent("Recovery required");
    expect(status).not.toHaveTextContent("unsafe child exit");
    expect(status).toHaveAttribute("data-variant", "danger");
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Recovery required after unsafe child exit.");
    expect(alert).toHaveClass("bg-danger-surface");
    expect(alert.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    expectActiveModelUnavailable();
    expect(screen.queryByRole("button", { name: "Browse verified models" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry node startup" })).not.toBeInTheDocument();
  });

  it("fails model truth closed while reconciling", async () => {
    const api = services();
    render(
      <NodeSessionProvider services={api} endpoint={endpoint}>
        <NodeScreen services={api} />
        <ReconcileControl />
      </NodeSessionProvider>,
    );
    const user = userEvent.setup();
    expect(await screen.findByText("Node ready — no model loaded")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Invalidate model truth" }));

    expect(statusBadge("Updating model status")).toBeVisible();
    expectActiveModelUnavailable();
    expect(screen.queryByRole("button", { name: "Browse verified models" })).not.toBeInTheDocument();
  });

  it("shows ready only from authoritative status and exposes technical fields", async () => {
    const control = scriptedV2Control(
      controlSnapshot({ slot: { status: "ready", model_id: "gemma-3-4b-it-q4", operation_id: null } }),
    );
    renderNode({
      ...servicesWithControl(control),
      copyText: vi.fn(),
      bootstrap: {
        ...services().bootstrap,
        start: vi.fn().mockResolvedValue(snapshot({ ownership: "attached" })),
      },
    });
    expect(await findStatusBadge("Ready")).toBeVisible();
    expect(screen.getByText("Externally attached")).toBeInTheDocument();
    for (const value of [endpoint, v2Ids.node, "gemma-3-4b-it-q4"]) {
      expect(screen.getAllByText(value).every((element) => element.classList.contains("technical-value"))).toBe(true);
    }
    expect(screen.queryByRole("button", { name: "Stop node" })).not.toBeInTheDocument();
  });

  it("stops only the app-owned node", async () => {
    const user = userEvent.setup();
    const { api } = renderNode();
    await screen.findByText("Node ready — no model loaded");
    await user.click(screen.getByRole("button", { name: "Stop node" }));
    expect(api.bootstrap.stop).toHaveBeenCalledTimes(1);
    expect(await findStatusBadge("Stopped")).toBeVisible();
    expectActiveModelUnavailable();
    expect(screen.queryByRole("button", { name: "Browse verified models" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry node startup" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start node" })).toBeEnabled();
    expect(screen.queryByText("Error", { selector: '[data-slot="status-badge"]' })).not.toBeInTheDocument();
  });

  it("keeps safe owned-child recovery available when the public probe fails", async () => {
    renderNode(services({ proveV2ControlPeer: vi.fn().mockRejectedValue(new Error("Public status unavailable.")) }));
    const status = await findStatusBadge("Error");
    expect(status).toHaveTextContent("Error");
    expect(status).not.toHaveTextContent("Public status unavailable.");
    expect(status).toHaveAttribute("data-variant", "danger");
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Public status unavailable.");
    expect(alert).toHaveClass("bg-danger-surface");
    expect(alert.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    expect(screen.getByText("App-owned node")).toBeInTheDocument();
    expectActiveModelUnavailable();
    expect(screen.queryByRole("button", { name: "Browse verified models" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry node startup" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Stop node" })).toBeEnabled();
  });

  it("copies the stable endpoint and announces feedback", async () => {
    const user = userEvent.setup();
    const { api } = renderNode();
    await screen.findByText("Node ready — no model loaded");
    await user.click(screen.getByRole("button", { name: "Copy endpoint" }));
    expect(api.copyText).toHaveBeenCalledWith(endpoint);
    expect(screen.getByText("Endpoint copied")).toHaveAttribute("aria-live", "polite");
  });

  it("applies the canonical 44px target contract", async () => {
    renderNode();
    expect(await screen.findByRole("button", { name: "Stop node" })).toHaveClass("interactive-target");
  });

  it("uses a feature-local canonical responsive and contrast contract", () => {
    const screenCss = readFileSync(resolve(process.cwd(), "src/node/NodeScreen.module.css"), "utf8");
    const tableCss = readFileSync(resolve(process.cwd(), "src/node/NodeTable.module.css"), "utf8");
    const tableSource = readFileSync(resolve(process.cwd(), "src/components/ui/table.tsx"), "utf8");
    const css = `${screenCss}\n${tableCss}`;
    expect(css).toContain("var(--loxa-component-minimum-interactive-target)");
    expect(css).toContain("@media (max-width:");
    expect(css).toContain("@media (prefers-contrast: more)");
    expect(css).toContain("@media (forced-colors: active)");
    expect(css).toContain("@media (prefers-reduced-motion: reduce)");
    expect(css).not.toMatch(/#[0-9a-f]{3,8}\b/i);
    expect(tableCss).toContain("overflow-wrap: anywhere");
    expect(tableCss).toMatch(
      /\.actions button\s*{[^}]*min-width:\s*var\(--loxa-component-minimum-interactive-target\)/s,
    );
    expect(tableCss).toMatch(
      /\.actions button\s*{[^}]*min-height:\s*var\(--loxa-component-minimum-interactive-target\)/s,
    );
    expect(tableCss).toContain("@media (max-width: 760px)");
    expect(tableSource).toContain("overflow-x-auto");
  });

  it("uses only variables defined by the distributed canonical Loxa tokens", () => {
    const canonical = readFileSync(resolve(process.cwd(), "src/styles/loxa.css"), "utf8");
    const definitions = new Set(Array.from(canonical.matchAll(/(--loxa-[a-z0-9-]+)\s*:/gi), ([, name]) => name));
    const modules = [
      "src/node/NodeScreen.module.css",
      "src/node/NodeTable.module.css",
      "src/node/NodeRuntimePanels.module.css",
    ];
    const undefinedReferences = modules.flatMap((file) => {
      const css = readFileSync(resolve(process.cwd(), file), "utf8");
      return Array.from(css.matchAll(/var\((--loxa-[a-z0-9-]+)/gi), ([, name]) => name)
        .filter((name) => !definitions.has(name))
        .map((name) => `${file}: ${name}`);
    });

    expect(undefinedReferences).toEqual([]);
  });
});
