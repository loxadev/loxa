import { beforeEach, describe, expect, it } from "vitest";
import type { StateStorage } from "zustand/middleware";

import {
  COLLAPSED_SIDEBAR_WIDTH,
  DEFAULT_EXPANDED_SIDEBAR_WIDTH,
  MAX_EXPANDED_SIDEBAR_WIDTH,
  MIN_EXPANDED_SIDEBAR_WIDTH,
  SIDEBAR_KEYBOARD_STEP,
  WORKSPACE_STORAGE_KEY,
  WORKSPACE_STORAGE_VERSION,
  createWorkspaceStore,
  selectActiveSettingsPage,
  selectActiveRoute,
  selectEffectiveSidebarWidth,
  selectExpandedSidebarWidth,
  selectResetSidebarWidth,
  selectSetActiveRoute,
  selectSetActiveSettingsPage,
  selectSetExpandedSidebarWidth,
  selectSidebarCollapsed,
  selectToggleSidebar,
} from "./workspace-store";

class MemoryStorage implements StateStorage {
  readonly values = new Map<string, string>();

  getItem(name: string) {
    return this.values.get(name) ?? null;
  }

  setItem(name: string, value: string) {
    this.values.set(name, value);
  }

  removeItem(name: string) {
    this.values.delete(name);
  }
}

const persistedEnvelope = (state: unknown, version = WORKSPACE_STORAGE_VERSION) => JSON.stringify({ state, version });

describe("workspace store", () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it("starts on chat with the exact sidebar constants", () => {
    const store = createWorkspaceStore(storage);

    expect(selectActiveRoute(store.getState())).toBe("chat");
    expect(selectActiveSettingsPage(store.getState())).toBe("overview");
    expect(selectSidebarCollapsed(store.getState())).toBe(false);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(280);
    expect(selectEffectiveSidebarWidth(store.getState())).toBe(280);
    expect(DEFAULT_EXPANDED_SIDEBAR_WIDTH).toBe(280);
    expect(MIN_EXPANDED_SIDEBAR_WIDTH).toBe(220);
    expect(MAX_EXPANDED_SIDEBAR_WIDTH).toBe(420);
    expect(COLLAPSED_SIDEBAR_WIDTH).toBe(56);
    expect(SIDEBAR_KEYBOARD_STEP).toBe(20);
  });

  it("changes the nested Settings page without persisting it", () => {
    const store = createWorkspaceStore(storage);

    selectSetActiveSettingsPage(store.getState())("runtime");

    expect(selectActiveSettingsPage(store.getState())).toBe("runtime");
    expect(JSON.parse(storage.values.get(WORKSPACE_STORAGE_KEY) ?? "null")).toEqual({
      state: { sidebarCollapsed: false, expandedSidebarWidth: 280 },
      version: WORKSPACE_STORAGE_VERSION,
    });
  });

  it("changes route without persisting it", () => {
    const store = createWorkspaceStore(storage);

    selectSetActiveRoute(store.getState())("models");

    expect(selectActiveRoute(store.getState())).toBe("models");
    expect(JSON.parse(storage.values.get(WORKSPACE_STORAGE_KEY) ?? "null")).toEqual({
      state: { sidebarCollapsed: false, expandedSidebarWidth: 280 },
      version: WORKSPACE_STORAGE_VERSION,
    });
  });

  it("clamps expanded widths and exposes the collapsed effective width", () => {
    const store = createWorkspaceStore(storage);
    const setWidth = selectSetExpandedSidebarWidth(store.getState());

    setWidth(100);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(220);
    setWidth(280);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(280);
    setWidth(999);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(420);

    store.getState().setSidebarCollapsed(true);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(420);
    expect(selectEffectiveSidebarWidth(store.getState())).toBe(56);
  });

  it("collapses and expands without losing the last useful width", () => {
    const store = createWorkspaceStore(storage);

    store.getState().setExpandedSidebarWidth(340);
    selectToggleSidebar(store.getState())();
    expect(selectEffectiveSidebarWidth(store.getState())).toBe(56);
    store.getState().setExpandedSidebarWidth(380);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(380);
    selectToggleSidebar(store.getState())();

    expect(selectSidebarCollapsed(store.getState())).toBe(false);
    expect(selectEffectiveSidebarWidth(store.getState())).toBe(380);
  });

  it("resizes by the keyboard step and resets to 280", () => {
    const store = createWorkspaceStore(storage);

    store.getState().resizeSidebarBy(SIDEBAR_KEYBOARD_STEP);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(300);
    selectResetSidebarWidth(store.getState())();
    expect(selectExpandedSidebarWidth(store.getState())).toBe(280);
  });

  it("persists only the exact versioned sidebar allowlist", () => {
    const store = createWorkspaceStore(storage);

    store.getState().setActiveRoute("settings");
    store.getState().setActiveSettingsPage("runtime");
    store.getState().setSidebarCollapsed(true);
    store.getState().setExpandedSidebarWidth(360);

    const payload = JSON.parse(storage.values.get(WORKSPACE_STORAGE_KEY) ?? "null");
    expect(payload).toEqual({
      state: { sidebarCollapsed: true, expandedSidebarWidth: 360 },
      version: WORKSPACE_STORAGE_VERSION,
    });
    expect(Object.keys(payload.state).sort()).toEqual(["expandedSidebarWidth", "sidebarCollapsed"]);
  });

  it("rehydrates valid sidebar preferences while keeping the route default", () => {
    storage.values.set(WORKSPACE_STORAGE_KEY, persistedEnvelope({ sidebarCollapsed: true, expandedSidebarWidth: 320 }));

    const store = createWorkspaceStore(storage);

    expect(selectActiveRoute(store.getState())).toBe("chat");
    expect(selectActiveSettingsPage(store.getState())).toBe("overview");
    expect(selectSidebarCollapsed(store.getState())).toBe(true);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(320);
    expect(selectEffectiveSidebarWidth(store.getState())).toBe(56);
  });

  it.each([
    ["malformed JSON", "{"],
    ["unknown shape", persistedEnvelope({ collapsed: true, width: 300 })],
    ["extra persisted keys", persistedEnvelope({ sidebarCollapsed: true, expandedSidebarWidth: 300, token: "secret" })],
    ["stale version", persistedEnvelope({ sidebarCollapsed: true, expandedSidebarWidth: 300 }, 0)],
    ["invalid boolean", persistedEnvelope({ sidebarCollapsed: "yes", expandedSidebarWidth: 300 })],
    ["non-finite width", '{"state":{"sidebarCollapsed":true,"expandedSidebarWidth":1e999},"version":1}'],
    ["too-small width", persistedEnvelope({ sidebarCollapsed: true, expandedSidebarWidth: 219 })],
    ["too-large width", persistedEnvelope({ sidebarCollapsed: true, expandedSidebarWidth: 421 })],
  ])("falls back safely for %s", (_label, value) => {
    storage.values.set(WORKSPACE_STORAGE_KEY, value);

    const state = createWorkspaceStore(storage).getState();

    expect(selectActiveRoute(state)).toBe("chat");
    expect(selectActiveSettingsPage(state)).toBe("overview");
    expect(selectSidebarCollapsed(state)).toBe(false);
    expect(selectExpandedSidebarWidth(state)).toBe(280);
  });

  it("falls back safely when storage access throws", () => {
    const unavailableStorage: StateStorage = {
      getItem: () => {
        throw new Error("storage unavailable");
      },
      setItem: () => {
        throw new Error("storage unavailable");
      },
      removeItem: () => {
        throw new Error("storage unavailable");
      },
    };

    const store = createWorkspaceStore(unavailableStorage);
    expect(selectSidebarCollapsed(store.getState())).toBe(false);
    expect(selectExpandedSidebarWidth(store.getState())).toBe(280);
    expect(() => store.getState().setSidebarCollapsed(true)).not.toThrow();
    expect(selectSidebarCollapsed(store.getState())).toBe(true);
  });

  it("contains no forbidden backend or product state", () => {
    const store = createWorkspaceStore(storage);
    const forbiddenKeys = [
      "nodePhase",
      "ownership",
      "endpoint",
      "status",
      "error",
      "proven",
      "services",
      "clients",
      "tokens",
      "credentials",
      "operations",
      "conversations",
      "turns",
      "domNodes",
      "portalTargets",
      "readers",
      "abortControllers",
      "childProcesses",
    ];

    for (const key of forbiddenKeys) {
      expect(store.getState()).not.toHaveProperty(key);
    }
    store.getState().setActiveRoute("node");
    store.getState().setExpandedSidebarWidth(300);
    const persisted = storage.values.get(WORKSPACE_STORAGE_KEY);
    expect(persisted).toBeDefined();
    expect(persisted).not.toMatch(/node|endpoint|token|credential|conversation|turn|process/i);
  });

  it("keeps action and selector references stable across updates", () => {
    const store = createWorkspaceStore(storage);
    const setRoute = selectSetActiveRoute(store.getState());
    const setSettingsPage = selectSetActiveSettingsPage(store.getState());
    const setWidth = selectSetExpandedSidebarWidth(store.getState());

    setRoute("node");
    setSettingsPage("runtime");
    setWidth(360);

    expect(selectSetActiveRoute(store.getState())).toBe(setRoute);
    expect(selectSetActiveSettingsPage(store.getState())).toBe(setSettingsPage);
    expect(selectSetExpandedSidebarWidth(store.getState())).toBe(setWidth);
    expect(selectActiveRoute).toBe(selectActiveRoute);
    expect(selectEffectiveSidebarWidth).toBe(selectEffectiveSidebarWidth);
  });
});
