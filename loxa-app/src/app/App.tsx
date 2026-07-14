import { useMemo, useState, type ReactNode } from "react";

import { ChatScreen, type ChatScreenServices } from "../chat/ChatScreen";
import { ConversationList } from "../chat/ConversationList";
import type { ConversationHistoryController, ConversationHistoryServices } from "../chat/conversationHistory";
import { useConversationHistory } from "../chat/useConversationHistory";
import { ModelsScreen, type ModelsScreenServices } from "../models/ModelsScreen";
import { NodeScreen, type NodeScreenServices } from "../node/NodeScreen";
import { NodeSessionProvider, useNodeSession, type NodeSessionServices } from "../node/NodeSession";
import { SettingsScreen } from "../settings/SettingsScreen";
import { useThemePreference } from "../settings/theme";
import { selectActiveRoute, selectSetActiveRoute, useWorkspaceStore } from "../stores/workspace-store";
import mark from "../assets/brand/loxa-mark.svg?no-inline";
import { AppShell } from "./AppShell";
import { appServices, DEFAULT_ENDPOINT } from "./services";

export type AppServices = NodeSessionServices &
  NodeScreenServices &
  ChatScreenServices &
  ModelsScreenServices &
  Partial<ConversationHistoryServices> & {
    clearChats?(endpoint: string, token: string, options?: { signal?: AbortSignal }): Promise<{ deleted: number }>;
  };

export function App({ services = appServices }: { services?: AppServices }) {
  return (
    <NodeSessionProvider services={services} endpoint={DEFAULT_ENDPOINT}>
      <AppWorkspace services={services} />
    </NodeSessionProvider>
  );
}

function AppWorkspace({ services }: { services: AppServices }) {
  const historyServices = useMemo(() => conversationHistoryServices(services), [services]);
  return historyServices ? (
    <HistoryWorkspace services={services} historyServices={historyServices} />
  ) : (
    <WorkspaceContents services={services} history={null} />
  );
}

function HistoryWorkspace({
  services,
  historyServices,
}: {
  services: AppServices;
  historyServices: ConversationHistoryServices;
}) {
  const session = useNodeSession();
  const history = useConversationHistory({
    services: historyServices,
    endpoint: session.endpoint,
    enabled: session.proven,
  });
  return <WorkspaceContents services={services} history={history} />;
}

function WorkspaceContents({
  services,
  history,
}: {
  services: AppServices;
  history: ConversationHistoryController | null;
}) {
  const route = useWorkspaceStore(selectActiveRoute);
  const setRoute = useWorkspaceStore(selectSetActiveRoute);
  const [theme, setTheme] = useThemePreference();
  const [chatInteractionLocked, setChatInteractionLocked] = useState(false);
  const session = useNodeSession();
  const health = globalHealthLabel(session.phase);
  const model =
    session.phase === "ready" && session.status?.runtime_model
      ? `Active model ${session.status.runtime_model}`
      : session.phase === "unloaded"
        ? "No active model"
        : "Model status unavailable";

  return (
    <AppShell
      brandMark={mark}
      runtimeHealth={health}
      runtimeModel={model}
      conversationRail={
        history ? (
          <GlobalConversationRail
            history={history}
            interactionLocked={chatInteractionLocked}
            onOpenChat={() => setRoute("chat")}
          />
        ) : undefined
      }
    >
      {route === "node" ? (
        <NodeScreen services={services} onNavigateModels={() => setRoute("models")} />
      ) : route === "models" ? (
        <NodeSessionGate heading="Models">
          <ModelsScreen
            services={services}
            endpoint={session.endpoint}
            onModelMutationStart={session.invalidateModelTruth}
            onModelMutationSettled={session.settleModelMutation}
          />
        </NodeSessionGate>
      ) : route === "chat" ? (
        <ChatScreen
          services={services}
          endpoint={session.endpoint}
          nodeAvailability={{ phase: session.phase, proven: session.proven, error: session.error }}
          onModelMutationStart={session.invalidateModelTruth}
          onModelMutationSettled={session.settleModelMutation}
          history={history}
          onInteractionLockChange={setChatInteractionLocked}
        />
      ) : (
        <SettingsScreen
          theme={theme}
          onThemeChange={setTheme}
          onClearChatHistory={
            services.clearChats
              ? async (signal) => {
                  const token = await services.readControlToken(session.endpoint);
                  if (signal.aborted) throw new DOMException("aborted", "AbortError");
                  const result = await services.clearChats?.(session.endpoint, token, { signal });
                  if (!signal.aborted) history?.clearAfterSettingsDelete();
                  return result?.deleted ?? 0;
                }
              : undefined
          }
          runtime={{
            phase: session.phase,
            endpoint: session.endpoint,
            ownership: session.ownership,
            status: session.status,
          }}
        />
      )}
    </AppShell>
  );
}

function GlobalConversationRail({
  history,
  interactionLocked,
  onOpenChat,
}: {
  history: ConversationHistoryController;
  interactionLocked: boolean;
  onOpenChat(): void;
}) {
  return (
    <ConversationList
      conversations={history.conversations}
      selectedId={history.selectedChatId}
      state={history.state}
      errorMessage={history.errorMessage}
      hasMore={history.hasMore}
      mutationsDisabled={interactionLocked}
      onCreate={async () => {
        await history.create();
        onOpenChat();
      }}
      onSelect={(chatId) => {
        if (interactionLocked) return;
        history.select(chatId);
        onOpenChat();
      }}
      onRename={async (chatId, title) => {
        await history.rename(chatId, title);
      }}
      onDelete={async (chatId) => {
        await history.delete(chatId);
      }}
      onLoadMore={history.loadMore}
      onRetry={history.retry}
    />
  );
}

function conversationHistoryServices(services: AppServices): ConversationHistoryServices | null {
  const { listChats, createChat, getChat, renameChat, deleteChat } = services;
  if (!listChats || !createChat || !getChat || !renameChat || !deleteChat) return null;
  return {
    readControlToken: services.readControlToken,
    listChats,
    createChat,
    getChat,
    renameChat,
    deleteChat,
  };
}

function globalHealthLabel(phase: ReturnType<typeof useNodeSession>["phase"]) {
  if (phase === "checking") return "Checking node";
  if (phase === "starting") return "Starting node";
  if (phase === "unloaded") return "Node online";
  if (phase === "ready") return "Node ready";
  if (phase === "reconciling") return "Updating node";
  if (phase === "stopping") return "Stopping node";
  if (phase === "recovery-required") return "Recovery required";
  if (phase === "error") return "Node error";
  return "Node disconnected";
}

function NodeSessionGate({ children, heading }: { children: ReactNode; heading: string }) {
  const session = useNodeSession();
  if (session.proven) return children;

  const waiting = session.phase === "checking" || session.phase === "starting";
  return (
    <section aria-labelledby="node-session-gate-heading">
      <header className="screen-header">
        <div>
          <p className="eyebrow">Local runtime</p>
          <h1 id="node-session-gate-heading">{heading}</h1>
        </div>
      </header>
      <p role="status" aria-live="polite">
        {waiting ? "Starting the private Loxa node…" : (session.error ?? "The Loxa node is stopped.")}
      </p>
      {!waiting && (
        <button className="primary-button interactive-target" type="button" onClick={() => void session.retry()}>
          Retry node startup
        </button>
      )}
    </section>
  );
}

export default App;
