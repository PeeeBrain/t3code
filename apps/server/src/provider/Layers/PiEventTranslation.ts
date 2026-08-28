/**
 * Pi event translation - a pure reducer from Pi RPC events to T3
 * `ProviderRuntimeEvent`s.
 *
 * The reducer owns no transport and no session registry: it takes the current
 * translation state plus one decoded Pi event and returns the next state with
 * the runtime events to publish. `PiAdapter` drives it from the client's
 * event stream and owns process lifecycle, turn orchestration, and the
 * runtime-event PubSub.
 *
 * @module PiEventTranslation
 */
import {
  EventId,
  type ProviderDriverKind,
  type ProviderRuntimeEvent,
  type ProviderUserInputAnswers,
  RuntimeRequestId,
  type ThreadId,
  type TurnId,
  type UserInputQuestion,
  RuntimeItemId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  PI_EXTENSION_DIALOG_METHODS,
  piUsage,
  type PiEvent,
  type PiExtensionUiRequest,
  type PiRpcCommand,
} from "./PiRpcProtocol.ts";

export interface PiTurnRecord {
  readonly id: TurnId;
  readonly items: ReadonlyArray<unknown>;
}

/** A blocking Pi extension dialog waiting on a T3 user-input answer. */
export interface PiPendingExtensionDialog {
  /** Pi's `extension_ui_request` id; also the T3 request id. */
  readonly requestId: string;
  readonly method: string;
  readonly question: UserInputQuestion;
}

export interface PiTranslationState {
  readonly streaming: boolean;
  readonly activeTurnId: TurnId | undefined;
  readonly assistantItemId: RuntimeItemId | undefined;
  readonly pendingFailureMessage: string | undefined;
  /** Turns aborted locally whose `agent_settled` must land as cancelled. */
  readonly interruptedTurnIds: ReadonlySet<string>;
  readonly turns: ReadonlyArray<PiTurnRecord>;
  readonly pendingDialogs: ReadonlyMap<string, PiPendingExtensionDialog>;
  readonly compactionItemId: RuntimeItemId | undefined;
}

export interface PiTranslationContext {
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
}

export interface PiTranslationDeps {
  /** Mints the stamp for one runtime event. */
  readonly stamp: () => Effect.Effect<{ readonly eventId: EventId; readonly createdAt: string }>;
  /** Mints the id for one newly surfaced item. */
  readonly newItemId: () => Effect.Effect<RuntimeItemId>;
}

export interface PiTurnSettled {
  readonly turnId: TurnId;
  readonly outcome: "completed" | "cancelled" | "failed";
  readonly errorMessage: string | undefined;
}

export interface PiTranslationResult {
  readonly state: PiTranslationState;
  readonly events: ReadonlyArray<ProviderRuntimeEvent>;
  readonly turnSettled: PiTurnSettled | undefined;
}

export const initialPiTranslationState = (): PiTranslationState => ({
  streaming: false,
  activeTurnId: undefined,
  assistantItemId: undefined,
  pendingFailureMessage: undefined,
  interruptedTurnIds: new Set(),
  turns: [],
  pendingDialogs: new Map(),
  compactionItemId: undefined,
});

/** Registers a freshly started T3 turn before its `prompt` command is sent. */
export function beginPiTurn(state: PiTranslationState, turnId: TurnId): PiTranslationState {
  return {
    ...state,
    streaming: true,
    activeTurnId: turnId,
    assistantItemId: undefined,
    pendingFailureMessage: undefined,
    turns: [...state.turns, { id: turnId, items: [] }],
  };
}

/** Marks a turn as locally interrupted so the next `agent_settled` cancels it. */
export function markTurnInterrupted(state: PiTranslationState, turnId: string): PiTranslationState {
  const interruptedTurnIds = new Set(state.interruptedTurnIds);
  interruptedTurnIds.add(turnId);
  return { ...state, interruptedTurnIds };
}

type PiEventDraft = Omit<ProviderRuntimeEvent, "eventId" | "provider" | "threadId" | "createdAt">;

/** Stamps one draft into a publishable runtime event. */
const makeEvent = <T extends PiEventDraft>(
  ctx: PiTranslationContext,
  deps: PiTranslationDeps,
  draft: T,
): Effect.Effect<
  T & {
    readonly eventId: EventId;
    readonly provider: ProviderDriverKind;
    readonly threadId: ThreadId;
    readonly createdAt: string;
  }
> =>
  Effect.map(deps.stamp(), (stamp) => ({
    ...draft,
    eventId: stamp.eventId,
    provider: ctx.provider,
    threadId: ctx.threadId,
    createdAt: stamp.createdAt,
  }));

/** Maps pi tool names onto T3's canonical item types; unknown names degrade gracefully. */
function itemTypeFromPiToolName(
  toolName: string,
): "command_execution" | "file_change" | "mcp_tool_call" | "dynamic_tool_call" {
  switch (toolName) {
    case "bash":
      return "command_execution";
    case "write":
    case "edit":
      return "file_change";
    default:
      return toolName.startsWith("mcp") ? "mcp_tool_call" : "dynamic_tool_call";
  }
}

const trimmedOr = (value: unknown, fallback: string): string =>
  typeof value === "string" && value.trim() ? value.trim() : fallback;

/**
 * Builds the terminal `turn.completed` event and clears turn-scoped state.
 * Shared by `agent_settled` and the adapter's forced settle paths (stop,
 * process exit) so every path emits the same shape.
 */
export const piSettleTurn = (
  state: PiTranslationState,
  outcome: PiTurnSettled["outcome"],
  ctx: PiTranslationContext,
  deps: PiTranslationDeps,
  failureMessage?: string,
): Effect.Effect<PiTranslationResult> =>
  Effect.gen(function* () {
    const turnId = state.activeTurnId;
    const nextState: PiTranslationState = {
      ...state,
      streaming: false,
      activeTurnId: undefined,
      assistantItemId: undefined,
      pendingFailureMessage: undefined,
    };
    if (turnId === undefined) {
      return { state: nextState, events: [], turnSettled: undefined };
    }
    const errorMessage =
      outcome === "failed"
        ? (failureMessage ?? state.pendingFailureMessage ?? "Pi turn failed.")
        : undefined;
    const settled: PiTurnSettled =
      outcome === "failed" && errorMessage !== undefined
        ? { turnId, outcome, errorMessage }
        : { turnId, outcome, errorMessage: undefined };
    const event = yield* makeEvent(ctx, deps, {
      type: "turn.completed",
      turnId,
      payload:
        settled.errorMessage !== undefined
          ? { state: outcome, errorMessage: settled.errorMessage }
          : { state: outcome },
    });
    return { state: nextState, events: [event], turnSettled: settled };
  });

const ensureAssistantItem = (
  state: PiTranslationState,
  deps: PiTranslationDeps,
  ctx: PiTranslationContext,
): Effect.Effect<{ state: PiTranslationState; events: ReadonlyArray<ProviderRuntimeEvent> }> =>
  Effect.gen(function* () {
    if (state.assistantItemId !== undefined || state.activeTurnId === undefined) {
      return { state, events: [] };
    }
    const itemId = yield* deps.newItemId();
    const event = yield* makeEvent(ctx, deps, {
      type: "item.started",
      turnId: state.activeTurnId,
      itemId,
      payload: { itemType: "assistant_message", status: "inProgress" },
    });
    return { state: { ...state, assistantItemId: itemId }, events: [event] };
  });

function piDialogQuestion(request: PiExtensionUiRequest): UserInputQuestion | undefined {
  const header = request.title ?? "Pi extension";
  switch (request.method) {
    case "select":
      return {
        id: request.id,
        header,
        question: request.title ?? "Choose an option",
        options: request.options.map((option) => ({ label: option, description: "" })),
        multiSelect: false,
      };
    case "confirm":
      return {
        id: request.id,
        header,
        question: request.message ?? request.title ?? "Confirm?",
        options: [
          { label: "Yes", description: "" },
          { label: "No", description: "" },
        ],
        multiSelect: false,
      };
    case "input":
      return {
        id: request.id,
        header,
        question: request.placeholder
          ? `${request.title ?? "Enter a value"}\n\nPlaceholder: ${request.placeholder}`
          : (request.title ?? "Enter a value"),
        options: [],
        multiSelect: false,
      };
    case "editor":
      return {
        id: request.id,
        header,
        question: request.prefill
          ? `${request.title ?? "Edit text"}\n\nInitial text:\n${request.prefill}`
          : (request.title ?? "Edit text"),
        options: [],
        multiSelect: false,
      };
    default:
      return undefined;
  }
}

const dialogResponseFor = (
  dialog: PiPendingExtensionDialog,
  answers: ProviderUserInputAnswers,
): PiRpcCommand & { type: "extension_ui_response" } => {
  const raw = answers[dialog.requestId];
  const answer =
    typeof raw === "string"
      ? raw
      : Array.isArray(raw) && typeof raw[0] === "string"
        ? raw[0]
        : undefined;
  if (answer === undefined) {
    return { type: "extension_ui_response", id: dialog.requestId, cancelled: true };
  }
  if (dialog.method === "confirm") {
    return { type: "extension_ui_response", id: dialog.requestId, confirmed: answer === "Yes" };
  }
  return { type: "extension_ui_response", id: dialog.requestId, value: answer };
};

/**
 * Answers a pending dialog from a T3 user-input response. Returns the
 * `extension_ui_response` command to send plus the canonical resolved event;
 * unknown request ids leave the state untouched.
 */
export const resolvePiExtensionDialog = (
  state: PiTranslationState,
  requestId: string,
  answers: ProviderUserInputAnswers,
  ctx: PiTranslationContext,
  deps: PiTranslationDeps,
): Effect.Effect<{
  state: PiTranslationState;
  events: ReadonlyArray<ProviderRuntimeEvent>;
  response: (PiRpcCommand & { type: "extension_ui_response" }) | undefined;
}> =>
  Effect.gen(function* () {
    const dialog = state.pendingDialogs.get(requestId);
    if (dialog === undefined) {
      return { state, events: [], response: undefined };
    }
    const pendingDialogs = new Map(state.pendingDialogs);
    pendingDialogs.delete(requestId);
    const resolved = yield* makeEvent(ctx, deps, {
      type: "user-input.resolved",
      requestId: RuntimeRequestId.make(dialog.requestId),
      ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
      payload: { answers },
    });
    return {
      state: { ...state, pendingDialogs },
      events: [resolved],
      response: dialogResponseFor(dialog, answers),
    };
  });

/**
 * Cancels every pending dialog (turn interrupted, session stopped, process
 * gone) so extensions blocked on `ctx.ui` never hang waiting for an answer
 * that can no longer arrive.
 */
export const cancelPendingExtensionDialogs = (
  state: PiTranslationState,
  ctx: PiTranslationContext,
  deps: PiTranslationDeps,
): Effect.Effect<{
  state: PiTranslationState;
  events: ReadonlyArray<ProviderRuntimeEvent>;
  responses: ReadonlyArray<PiRpcCommand & { type: "extension_ui_response" }>;
}> =>
  Effect.gen(function* () {
    if (state.pendingDialogs.size === 0) {
      return { state, events: [], responses: [] };
    }
    const events: ProviderRuntimeEvent[] = [];
    const responses: Array<PiRpcCommand & { type: "extension_ui_response" }> = [];
    let nextState = state;
    for (const dialog of state.pendingDialogs.values()) {
      const resolved = yield* makeEvent(ctx, deps, {
        type: "user-input.resolved",
        requestId: RuntimeRequestId.make(dialog.requestId),
        ...(nextState.activeTurnId ? { turnId: nextState.activeTurnId } : {}),
        payload: { answers: {} },
      });
      events.push(resolved);
      responses.push({ type: "extension_ui_response", id: dialog.requestId, cancelled: true });
    }
    return { state: { ...nextState, pendingDialogs: new Map() }, events, responses };
  });

/**
 * Translates one decoded Pi event. Turn completion waits for `agent_settled`:
 * `agent_end` alone is not enough because retries, compaction retries, and
 * queued steering can continue past it.
 */
export const reducePiEvent = (
  state: PiTranslationState,
  event: PiEvent,
  ctx: PiTranslationContext,
  deps: PiTranslationDeps,
): Effect.Effect<PiTranslationResult> =>
  Effect.gen(function* () {
    switch (event.type) {
      case "agent_start":
        return { state: { ...state, streaming: true }, events: [], turnSettled: undefined };

      case "agent_settled": {
        const wasInterrupted =
          state.activeTurnId !== undefined && state.interruptedTurnIds.has(state.activeTurnId);
        let nextState = state;
        if (state.activeTurnId !== undefined) {
          const interruptedTurnIds = new Set(state.interruptedTurnIds);
          interruptedTurnIds.delete(state.activeTurnId);
          nextState = { ...state, interruptedTurnIds };
        }
        const outcome: PiTurnSettled["outcome"] = wasInterrupted
          ? "cancelled"
          : state.pendingFailureMessage !== undefined
            ? "failed"
            : "completed";
        return yield* piSettleTurn(nextState, outcome, ctx, deps);
      }

      case "agent_end":
      case "turn_start":
      case "turn_end":
      // T3 synthesizes its own turn lifecycle at the `agent_settled` boundary.
      // fallthrough
      case "queue_update":
      // T3 renders its own send queue; Pi's queue state is provider-internal.
      // fallthrough
      case "bash_execution_update":
        // T3 never issues direct RPC `bash` commands, so there are no
        // correlated updates to surface.
        return { state, events: [], turnSettled: undefined };

      case "message_start": {
        if (state.activeTurnId === undefined || event.message["role"] !== "assistant") {
          return { state, events: [], turnSettled: undefined };
        }
        const itemId = yield* deps.newItemId();
        const started = yield* makeEvent(ctx, deps, {
          type: "item.started",
          turnId: state.activeTurnId,
          itemId,
          payload: { itemType: "assistant_message", status: "inProgress" },
        });
        return {
          state: { ...state, assistantItemId: itemId },
          events: [started],
          turnSettled: undefined,
        };
      }

      case "message_update": {
        let nextState = state;
        const events: ProviderRuntimeEvent[] = [];
        const deltaEvent = event.assistantMessageEvent;
        if (deltaEvent !== undefined) {
          const deltaType = deltaEvent["type"];
          if (deltaType === "text_delta" || deltaType === "thinking_delta") {
            const delta = deltaEvent["delta"];
            if (typeof delta === "string" && delta.length > 0) {
              const ensured = yield* ensureAssistantItem(nextState, deps, ctx);
              nextState = ensured.state;
              events.push(...ensured.events);
              if (nextState.assistantItemId !== undefined) {
                events.push(
                  yield* makeEvent(ctx, deps, {
                    type: "content.delta",
                    ...(nextState.activeTurnId ? { turnId: nextState.activeTurnId } : {}),
                    itemId: nextState.assistantItemId,
                    payload: {
                      streamKind: deltaType === "text_delta" ? "assistant_text" : "reasoning_text",
                      delta,
                    },
                    raw: {
                      source: "pi.rpc",
                      messageType: "message_update",
                      payload: deltaEvent,
                    },
                  }),
                );
              }
            }
          }
        }
        const usage = piUsage(event.usage);
        if (usage !== undefined && nextState.activeTurnId !== undefined) {
          events.push(
            yield* makeEvent(ctx, deps, {
              type: "thread.token-usage.updated",
              turnId: nextState.activeTurnId,
              payload: { usage },
            }),
          );
        }
        return { state: nextState, events, turnSettled: undefined };
      }

      case "message_end": {
        if (event.message["role"] !== "assistant") {
          return { state, events: [], turnSettled: undefined };
        }
        const turnId = state.activeTurnId;
        if (turnId === undefined) {
          return { state, events: [], turnSettled: undefined };
        }
        let itemId = state.assistantItemId;
        const events: ProviderRuntimeEvent[] = [];
        if (itemId === undefined) {
          itemId = yield* deps.newItemId();
          events.push(
            yield* makeEvent(ctx, deps, {
              type: "item.started",
              turnId,
              itemId,
              payload: { itemType: "assistant_message", status: "inProgress" },
            }),
          );
        }
        const turns = state.turns.map((turn) =>
          turn.id === turnId ? { ...turn, items: [...turn.items, event.message] } : turn,
        );
        const stopReason = event.message["stopReason"];
        const errorMessage = event.message["errorMessage"];
        const failed = stopReason === "error";
        const nextState: PiTranslationState = {
          ...state,
          turns,
          assistantItemId: undefined,
          pendingFailureMessage: failed
            ? trimmedOr(errorMessage, "Pi assistant response failed.")
            : state.pendingFailureMessage,
        };
        events.push(
          yield* makeEvent(ctx, deps, {
            type: "item.completed",
            turnId,
            itemId,
            payload: {
              itemType: "assistant_message",
              status: failed ? "failed" : "completed",
            },
            raw: { source: "pi.rpc", messageType: "message_end", payload: event.message },
          }),
        );
        return { state: nextState, events, turnSettled: undefined };
      }

      case "tool_execution_start":
      case "tool_execution_update":
      case "tool_execution_end": {
        if (state.activeTurnId === undefined) {
          return { state, events: [], turnSettled: undefined };
        }
        const itemType = itemTypeFromPiToolName(event.toolName);
        const data = {
          toolName: event.toolName,
          ...(event.args !== undefined ? { input: event.args } : {}),
          ...(event.partialResult !== undefined ? { partialResult: event.partialResult } : {}),
          ...(event.result !== undefined ? { result: event.result } : {}),
        };
        const lifecycleEvent = yield* makeEvent(ctx, deps, {
          type:
            event.type === "tool_execution_start"
              ? "item.started"
              : event.type === "tool_execution_end"
                ? "item.completed"
                : "item.updated",
          turnId: state.activeTurnId,
          itemId: RuntimeItemId.make(event.toolCallId),
          payload: {
            itemType,
            status:
              event.type === "tool_execution_end"
                ? event.isError
                  ? ("failed" as const)
                  : ("completed" as const)
                : ("inProgress" as const),
            title: event.toolName,
            data,
          },
          raw: { source: "pi.rpc", messageType: event.type, payload: event },
        });
        return { state, events: [lifecycleEvent], turnSettled: undefined };
      }

      case "auto_retry_start": {
        const notice = yield* makeEvent(ctx, deps, {
          type: "session.state.changed",
          payload: { state: "running", reason: "Pi auto-retry started" },
        });
        return {
          state: { ...state, pendingFailureMessage: undefined },
          events: [notice],
          turnSettled: undefined,
        };
      }

      case "auto_retry_end": {
        if (event.success) {
          return {
            state: { ...state, pendingFailureMessage: undefined },
            events: [],
            turnSettled: undefined,
          };
        }
        return {
          state: {
            ...state,
            pendingFailureMessage: trimmedOr(
              event.finalError,
              "Pi exhausted its automatic retries.",
            ),
          },
          events: [],
          turnSettled: undefined,
        };
      }

      case "compaction_start": {
        const itemId = yield* deps.newItemId();
        const started = yield* makeEvent(ctx, deps, {
          type: "item.started",
          ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
          itemId,
          payload: {
            itemType: "context_compaction",
            status: "inProgress",
            title: "Compacting context",
            data: { reason: event.reason },
          },
        });
        return {
          state: { ...state, compactionItemId: itemId },
          events: [started],
          turnSettled: undefined,
        };
      }

      case "compaction_end": {
        const itemId = state.compactionItemId;
        if (itemId === undefined) {
          return { state, events: [], turnSettled: undefined };
        }
        const failed = !event.aborted && !event.willRetry && event.errorMessage !== undefined;
        const completed = yield* makeEvent(ctx, deps, {
          type: "item.completed",
          ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
          itemId,
          payload: {
            itemType: "context_compaction",
            status: failed ? "failed" : event.aborted ? "declined" : "completed",
            title: "Compacting context",
            data: {
              reason: event.reason,
              ...(event.aborted ? { aborted: true } : {}),
              ...(event.willRetry ? { willRetry: true } : {}),
              ...(event.result?.["summary"] !== undefined
                ? { summary: event.result["summary"] }
                : {}),
              ...(event.errorMessage !== undefined ? { errorMessage: event.errorMessage } : {}),
            },
          },
        });
        return {
          state: {
            ...state,
            compactionItemId: undefined,
            // A compaction failure (with no retry pending) fails the turn at
            // settle, matching Pi's own terminal state for the prompt.
            pendingFailureMessage: failed ? event.errorMessage : state.pendingFailureMessage,
          },
          events: [completed],
          turnSettled: undefined,
        };
      }

      case "summarization_retry_scheduled": {
        const notice = yield* makeEvent(ctx, deps, {
          type: "runtime.warning",
          ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
          payload: {
            message: "Pi is retrying context summarization after a transient error.",
            ...(event.errorMessage !== undefined ? { detail: event.errorMessage } : {}),
          },
        });
        return { state, events: [notice], turnSettled: undefined };
      }

      case "summarization_retry_attempt_start":
      case "summarization_retry_finished":
        return { state, events: [], turnSettled: undefined };

      case "extension_error": {
        const error = yield* makeEvent(ctx, deps, {
          type: "runtime.error",
          ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
          payload: {
            message: `Pi extension failed: ${event.error}`,
            class: "provider_error",
            detail: {
              ...(event.extensionPath !== undefined ? { extensionPath: event.extensionPath } : {}),
              ...(event.event !== undefined ? { event: event.event } : {}),
            },
          },
        });
        return { state, events: [error], turnSettled: undefined };
      }

      case "extension_ui_request": {
        if (PI_EXTENSION_DIALOG_METHODS.has(event.method)) {
          const question = piDialogQuestion(event);
          if (question === undefined) {
            return { state, events: [], turnSettled: undefined };
          }
          const dialog: PiPendingExtensionDialog = {
            requestId: event.id,
            method: event.method,
            question,
          };
          const pendingDialogs = new Map(state.pendingDialogs);
          pendingDialogs.set(event.id, dialog);
          const requested = yield* makeEvent(ctx, deps, {
            type: "user-input.requested",
            ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
            requestId: RuntimeRequestId.make(event.id),
            payload: { questions: [question] },
            raw: { source: "pi.rpc", messageType: "extension_ui_request", payload: event.raw },
          });
          return {
            state: { ...state, pendingDialogs },
            events: [requested],
            turnSettled: undefined,
          };
        }
        if (event.method === "notify") {
          const notice = yield* makeEvent(ctx, deps, {
            type: "runtime.warning",
            ...(state.activeTurnId ? { turnId: state.activeTurnId } : {}),
            payload: {
              message: trimmedOr(event.message, trimmedOr(event.title, "Pi extension notice")),
              detail: { notifyType: event.notifyType ?? "info" },
            },
          });
          return { state, events: [notice], turnSettled: undefined };
        }
        // setStatus / setWidget / setTitle / set_editor_text are chrome-level
        // TUI affordances with no T3 surface; they expect no response.
        return { state, events: [], turnSettled: undefined };
      }
    }
  });
