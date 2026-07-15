import { create } from "zustand";
import { createJSONStorage, persist, type StateStorage } from "zustand/middleware";

export const ACTIVITY_RAIL_WIDTH = 48;
export const DEFAULT_CONVERSATION_RAIL_WIDTH = 280;
export const MIN_CONVERSATION_RAIL_WIDTH = 240;
export const MAX_CONVERSATION_RAIL_WIDTH = 400;

/** @deprecated Prefer the conversation rail constants for new shell code. */
export const DEFAULT_EXPANDED_SIDEBAR_WIDTH = DEFAULT_CONVERSATION_RAIL_WIDTH;
/** @deprecated Prefer the conversation rail constants for new shell code. */
export const MIN_EXPANDED_SIDEBAR_WIDTH = MIN_CONVERSATION_RAIL_WIDTH;
/** @deprecated Prefer the conversation rail constants for new shell code. */
export const MAX_EXPANDED_SIDEBAR_WIDTH = MAX_CONVERSATION_RAIL_WIDTH;
/** @deprecated The fixed activity rail remains visible when conversations collapse. */
export const COLLAPSED_SIDEBAR_WIDTH = ACTIVITY_RAIL_WIDTH;
export const SIDEBAR_KEYBOARD_STEP = 20;

export const WORKSPACE_STORAGE_KEY = "loxa-workspace";
export const WORKSPACE_STORAGE_VERSION = 1;

export type WorkspaceRoute = "chat" | "models" | "node" | "settings";
export type SettingsPage = "overview" | "runtime";

type WorkspacePreferences = {
  sidebarCollapsed: boolean;
  expandedSidebarWidth: number;
};

export type WorkspaceState = WorkspacePreferences & {
  activeRoute: WorkspaceRoute;
  activeSettingsPage: SettingsPage;
  setActiveRoute: (route: WorkspaceRoute) => void;
  setActiveSettingsPage: (page: SettingsPage) => void;
  setSidebarCollapsed: (collapsed: boolean) => void;
  toggleSidebar: () => void;
  setExpandedSidebarWidth: (width: number) => void;
  resizeSidebarBy: (delta: number) => void;
  resetSidebarWidth: () => void;
};

const DEFAULT_PREFERENCES: WorkspacePreferences = {
  sidebarCollapsed: false,
  expandedSidebarWidth: DEFAULT_EXPANDED_SIDEBAR_WIDTH,
};

const clampExpandedWidth = (width: number) => {
  if (Number.isNaN(width)) {
    return DEFAULT_EXPANDED_SIDEBAR_WIDTH;
  }

  return Math.min(MAX_EXPANDED_SIDEBAR_WIDTH, Math.max(MIN_EXPANDED_SIDEBAR_WIDTH, width));
};

const isWorkspacePreferences = (value: unknown): value is WorkspacePreferences => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();

  return (
    keys.length === 2 &&
    keys[0] === "expandedSidebarWidth" &&
    keys[1] === "sidebarCollapsed" &&
    typeof record.sidebarCollapsed === "boolean" &&
    typeof record.expandedSidebarWidth === "number" &&
    Number.isFinite(record.expandedSidebarWidth) &&
    record.expandedSidebarWidth >= MIN_EXPANDED_SIDEBAR_WIDTH &&
    record.expandedSidebarWidth <= MAX_EXPANDED_SIDEBAR_WIDTH
  );
};

const unavailableStorage: StateStorage = {
  getItem: () => null,
  setItem: () => undefined,
  removeItem: () => undefined,
};

const resolveStorage = (): StateStorage => {
  try {
    return globalThis.localStorage ?? unavailableStorage;
  } catch {
    return unavailableStorage;
  }
};

const makeSafeStorage = (storage: StateStorage): StateStorage => ({
  getItem: (name) => {
    try {
      return storage.getItem(name);
    } catch {
      return null;
    }
  },
  setItem: (name, value) => {
    try {
      return storage.setItem(name, value);
    } catch {
      return undefined;
    }
  },
  removeItem: (name) => {
    try {
      return storage.removeItem(name);
    } catch {
      return undefined;
    }
  },
});

export const createWorkspaceStore = (storage: StateStorage = resolveStorage()) =>
  create<WorkspaceState>()(
    persist(
      (set) => ({
        activeRoute: "chat",
        activeSettingsPage: "overview",
        ...DEFAULT_PREFERENCES,
        setActiveRoute: (activeRoute) => set({ activeRoute }),
        setActiveSettingsPage: (activeSettingsPage) => set({ activeSettingsPage }),
        setSidebarCollapsed: (sidebarCollapsed) => set({ sidebarCollapsed }),
        toggleSidebar: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
        setExpandedSidebarWidth: (expandedSidebarWidth) =>
          set({ expandedSidebarWidth: clampExpandedWidth(expandedSidebarWidth) }),
        resizeSidebarBy: (delta) =>
          set((state) => ({
            expandedSidebarWidth: clampExpandedWidth(state.expandedSidebarWidth + delta),
          })),
        resetSidebarWidth: () => set({ expandedSidebarWidth: DEFAULT_EXPANDED_SIDEBAR_WIDTH }),
      }),
      {
        name: WORKSPACE_STORAGE_KEY,
        version: WORKSPACE_STORAGE_VERSION,
        storage: createJSONStorage<WorkspacePreferences>(() => makeSafeStorage(storage)),
        partialize: (state) => ({
          sidebarCollapsed: state.sidebarCollapsed,
          expandedSidebarWidth: state.expandedSidebarWidth,
        }),
        migrate: () => ({ ...DEFAULT_PREFERENCES }),
        merge: (persistedState, currentState) => ({
          ...currentState,
          ...(isWorkspacePreferences(persistedState) ? persistedState : DEFAULT_PREFERENCES),
        }),
      },
    ),
  );

export const useWorkspaceStore = createWorkspaceStore();

export const selectActiveRoute = (state: WorkspaceState) => state.activeRoute;
export const selectActiveSettingsPage = (state: WorkspaceState) => state.activeSettingsPage;
export const selectSidebarCollapsed = (state: WorkspaceState) => state.sidebarCollapsed;
export const selectExpandedSidebarWidth = (state: WorkspaceState) => state.expandedSidebarWidth;
export const selectEffectiveSidebarWidth = (state: WorkspaceState) =>
  state.sidebarCollapsed ? COLLAPSED_SIDEBAR_WIDTH : state.expandedSidebarWidth;
export const selectConversationRailWidth = (state: WorkspaceState) => state.expandedSidebarWidth;
export const selectConversationRailCollapsed = (state: WorkspaceState) => state.sidebarCollapsed;

export const selectSetActiveRoute = (state: WorkspaceState) => state.setActiveRoute;
export const selectSetActiveSettingsPage = (state: WorkspaceState) => state.setActiveSettingsPage;
export const selectSetSidebarCollapsed = (state: WorkspaceState) => state.setSidebarCollapsed;
export const selectToggleSidebar = (state: WorkspaceState) => state.toggleSidebar;
export const selectSetExpandedSidebarWidth = (state: WorkspaceState) => state.setExpandedSidebarWidth;
export const selectResizeSidebarBy = (state: WorkspaceState) => state.resizeSidebarBy;
export const selectResetSidebarWidth = (state: WorkspaceState) => state.resetSidebarWidth;
