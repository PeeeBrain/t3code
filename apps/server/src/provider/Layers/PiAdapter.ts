import {
  ApprovalRequestId,
  EventId,
  type PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import { makePiRpcSessionRuntime, type PiRpcSessionRuntime } from "./PiRpcSessionRuntime.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_RESUME_VERSION = 1 as const;

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface PiSessionContext {
  readonly threadId: ThreadId;
  /** The `--session-id` handed to the CLI; also the resume cursor payload. */
  readonly piSessionId: string;
  readonly runtime: PiRpcSessionRuntime;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  activeTurnId: TurnId | undefined;
  /** Turns aborted locally whose `agent_settled` must land as cancelled. */
  readonly interruptedTurnIds: Set<TurnId>;
  streaming: boolean;
  currentModelId: string | undefined;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  stopped: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parsePiResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== PI_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

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

/** Extracts `{toolCallId, toolName}` from a `tool_execution_*` event. */
function piToolCallFromEvent(
  value: Record<string, unknown>,
): { readonly toolCallId: string; readonly toolName: string } | undefined {
  const toolCallId = value["toolCallId"];
  const toolName = value["toolName"];
  if (typeof toolCallId !== "string" || typeof toolName !== "string") return undefined;
  return { toolCallId, toolName };
}

function piModelIdFromState(data: Record<string, unknown>): string | undefined {
  const model = data["model"];
  if (!isRecord(model)) return undefined;
  const id = model["id"];
  return typeof id === "string" && id.trim() ? id.trim() : undefined;
}

/** Extracts `{kind, delta}` from a `message_update` carrying a text/thinking delta. */
function piContentDelta(
  value: Record<string, unknown>,
): { readonly kind: "assistant_text" | "reasoning_text"; readonly delta: string } | undefined {
  const event = value["assistantMessageEvent"];
  if (!isRecord(event)) return undefined;
  const eventType = event["type"];
  if (eventType !== "text_delta" && eventType !== "thinking_delta") return undefined;
  if (typeof event["delta"] !== "string" || event["delta"].length === 0) return undefined;
  return {
    kind: eventType === "text_delta" ? "assistant_text" : "reasoning_text",
    delta: event["delta"],
  };
}

/**
 * Token usage from a `message_update`. Pi reports cumulative usage on every
 * update; fields are optional so partial provider reporting round-trips.
 */
function piUsageFromUpdate(value: Record<string, unknown>) {
  const usage = value["usage"];
  if (!isRecord(usage)) return undefined;
  const int = (input: unknown): number | undefined =>
    typeof input === "number" && Number.isFinite(input) && input >= 0
      ? Math.trunc(input)
      : undefined;
  const input = int(usage["input"]);
  const output = int(usage["output"]);
  const totalTokens = int(usage["totalTokens"]);
  const cachedInputTokens = int(usage["cacheRead"]);
  const reasoning = int(usage["reasoning"]);
  if (input === undefined && output === undefined && totalTokens === undefined) return undefined;
  return {
    usedTokens: totalTokens ?? 0,
    lastUsedTokens: totalTokens ?? 0,
    inputTokens: input ?? 0,
    lastInputTokens: input ?? 0,
    cachedInputTokens: cachedInputTokens ?? 0,
    outputTokens: output ?? 0,
    lastOutputTokens: output ?? 0,
    reasoningOutputTokens: reasoning ?? 0,
  };
}

export function makePiAdapter(
  piSettings: PiSettings,
  options?: PiAdapterLiveOptions,
): Effect.Effect<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ServerConfig
  | Scope.Scope
> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const sessions = new Map<ThreadId, PiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(Effect.orDie);
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<PiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const settleTurn = (
      ctx: PiSessionContext,
      state: "completed" | "cancelled" | "failed",
      errorMessage?: string,
    ) =>
      Effect.gen(function* () {
        const turnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
        ctx.streaming = false;
        ctx.activeTurnId = undefined;
        const updatedAt = yield* nowIso;
        const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
        ctx.session = { ...readySession, status: "ready", updatedAt };
        if (!turnId) return;
        if (state === "failed") {
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId,
            payload: { state: "failed", errorMessage: errorMessage ?? "Pi turn failed." },
          });
          return;
        }
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: { state },
        });
      });

    /** Translates one decoded pi RPC line into runtime events. Unknown lines are ignored. */
    const translateEvent = (ctx: PiSessionContext, value: Record<string, unknown>) =>
      Effect.gen(function* () {
        const eventType = value["type"];
        // Turn completion. agent_end alone is not enough: retries, compaction
        // retries and queued follow-ups may continue past it.
        if (eventType === "agent_settled") {
          const wasInterrupted =
            ctx.activeTurnId !== undefined && ctx.interruptedTurnIds.has(ctx.activeTurnId);
          if (ctx.activeTurnId !== undefined) ctx.interruptedTurnIds.delete(ctx.activeTurnId);
          yield* settleTurn(ctx, wasInterrupted ? "cancelled" : "completed");
          return;
        }
        if (eventType === "agent_start") {
          ctx.streaming = true;
          return;
        }
        if (eventType === "message_update") {
          const delta = piContentDelta(value);
          if (delta) {
            yield* offerRuntimeEvent({
              type: "content.delta",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
              payload: { streamKind: delta.kind, delta: delta.delta },
              raw: { source: "pi.rpc", messageType: "message_update", payload: value },
            });
          }
          const usage = piUsageFromUpdate(value);
          if (usage && ctx.activeTurnId) {
            yield* offerRuntimeEvent({
              type: "thread.token-usage.updated",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
              payload: { usage },
            });
          }
          return;
        }
        if (eventType === "message_end") {
          const message = value["message"];
          if (!isRecord(message) || message["role"] !== "assistant") return;
          const itemId = typeof message["id"] === "string" ? message["id"] : undefined;
          const turnId = ctx.activeTurnId;
          if (itemId && turnId) {
            const turnRecord = ctx.turns.find((turn) => turn.id === turnId);
            if (turnRecord) turnRecord.items.push(message);
            yield* offerRuntimeEvent({
              type: "item.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId,
              itemId: RuntimeItemId.make(itemId),
              payload: { itemType: "assistant_message", status: "completed" },
              raw: { source: "pi.rpc", messageType: "message_end", payload: value },
            });
          }
          return;
        }
        if (
          eventType === "tool_execution_start" ||
          eventType === "tool_execution_update" ||
          eventType === "tool_execution_end"
        ) {
          const call = piToolCallFromEvent(value);
          if (!call || !ctx.activeTurnId) return;
          const itemType = itemTypeFromPiToolName(call.toolName);
          const isError = eventType === "tool_execution_end" && value["isError"] === true;
          yield* offerRuntimeEvent({
            type: eventType === "tool_execution_end" ? "item.completed" : "item.updated",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: ctx.activeTurnId,
            itemId: RuntimeItemId.make(call.toolCallId),
            payload: {
              itemType,
              status:
                eventType === "tool_execution_end"
                  ? isError
                    ? ("failed" as const)
                    : ("completed" as const)
                  : ("inProgress" as const),
              title: call.toolName,
              ...(isRecord(value["args"]) && Object.keys(value["args"]).length > 0
                ? { data: value["args"] }
                : {}),
            },
            raw: { source: "pi.rpc", messageType: eventType, payload: value },
          });
          return;
        }
        if (eventType === "auto_retry_start") {
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            payload: { state: "running", reason: "Pi auto-retry started" },
          });
        }
      });

    const stopSessionInternal = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* Effect.ignore(ctx.runtime.notify({ type: "abort" }));
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber).pipe(Effect.ignore);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = path.resolve(input.cwd.trim());
          const modelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          // Fresh threads get a durable session id up front; resumed threads
          // reopen the exact project session via `--session-id`.
          const resume = parsePiResume(input.resumeCursor);
          const piSessionId = resume?.sessionId ?? (yield* randomUUIDv4);

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          const runtime = yield* makePiRpcSessionRuntime({
            settings: piSettings,
            ...(options?.environment ? { environment: options.environment } : {}),
            cwd,
            sessionId: piSessionId,
            ...(modelSelection?.model ? { modelId: modelSelection.model } : {}),
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, sessionScope),
          );

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "connecting",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(modelSelection?.model ? { model: modelSelection.model } : {}),
            threadId: input.threadId,
            resumeCursor: { schemaVersion: PI_RESUME_VERSION, sessionId: piSessionId },
            createdAt: now,
            updatedAt: now,
          };
          const ctx: PiSessionContext = {
            threadId: input.threadId,
            piSessionId,
            runtime,
            session,
            scope: sessionScope,
            notificationFiber: undefined,
            activeTurnId: undefined,
            interruptedTurnIds: new Set(),
            streaming: false,
            currentModelId: modelSelection?.model,
            turns: [],
            stopped: false,
          };
          sessions.set(input.threadId, ctx);

          // Pump first, then start: pi emits events (extension UI prompts)
          // before any command response arrives.
          ctx.notificationFiber = yield* Stream.runForEach(runtime.events, (event) =>
            isRecord(event)
              ? translateEvent(ctx, event).pipe(
                  Effect.catchCause((cause) =>
                    Effect.logWarning("Failed to process Pi runtime event.", { cause }),
                  ),
                )
              : Effect.void,
          ).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("Failed to process Pi runtime notifications.", { cause }),
            ),
            Effect.forkIn(sessionScope),
          );

          yield* runtime.start().pipe(
            Effect.mapError(
              (cause): ProviderAdapterProcessError =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );

          const stateData = yield* runtime.request({ type: "get_state" }).pipe(
            Effect.mapError(
              (cause): ProviderAdapterRequestError =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "get_state",
                  detail: cause.message,
                  cause,
                }),
            ),
            Effect.catchTag("ProviderAdapterRequestError", () => Effect.succeed({})),
          );
          ctx.currentModelId = piModelIdFromState(stateData) ?? ctx.currentModelId;

          const readySession: ProviderSession = {
            ...ctx.session,
            status: "ready",
            ...(ctx.currentModelId ? { model: ctx.currentModelId } : {}),
            updatedAt: yield* nowIso,
          };
          ctx.session = readySession;
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: resume !== undefined },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Pi RPC session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: piSessionId },
          });

          return readySession;
        }).pipe(Effect.scoped),
      );

    const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const prepared = yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(input.threadId);
            // A sendTurn while pi is streaming is a steer folded into the
            // running turn, mirroring the merged-turn behaviour of the other
            // adapters; only the last queued message settles the turn.
            const steering = ctx.streaming && ctx.activeTurnId !== undefined;
            const turnId = steering
              ? (ctx.activeTurnId as TurnId)
              : TurnId.make(yield* randomUUIDv4);

            const images = yield* Effect.forEach(
              input.attachments ?? [],
              (
                attachment,
              ): Effect.Effect<
                { type: "image"; data: string; mimeType: string },
                ProviderAdapterError
              > =>
                Effect.gen(function* () {
                  const attachmentPath = resolveAttachmentPath({
                    attachmentsDir: serverConfig.attachmentsDir,
                    attachment,
                  });
                  if (!attachmentPath) {
                    return yield* new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "prompt",
                      detail: `Invalid attachment id '${attachment.id}'.`,
                    });
                  }
                  const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                    Effect.mapError(
                      (cause): ProviderAdapterRequestError =>
                        new ProviderAdapterRequestError({
                          provider: PROVIDER,
                          method: "prompt",
                          detail: cause.message,
                          cause,
                        }),
                    ),
                  );
                  return {
                    type: "image",
                    data: Buffer.from(bytes).toString("base64"),
                    mimeType: attachment.mimeType,
                  };
                }),
            );

            const text = input.input?.trim();
            if (!text && images.length === 0) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "sendTurn",
                issue: "Turn requires non-empty text or attachments.",
              });
            }

            const command: Record<string, unknown> = {
              type: "prompt",
              message: text ?? "",
              ...(steering ? { streamingBehavior: "steer" } : {}),
              ...(images.length > 0 ? { images } : {}),
            };

            if (!steering) {
              ctx.activeTurnId = turnId;
              ctx.streaming = true;
              const updatedAt = yield* nowIso;
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: turnId,
                updatedAt,
              };
              const turnRecord = { id: turnId, items: [] as Array<unknown> };
              ctx.turns.push(turnRecord);
              yield* offerRuntimeEvent({
                type: "turn.started",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: ctx.currentModelId !== undefined ? { model: ctx.currentModelId } : {},
              });
            }
            return { ctx, command, turnId, steering };
          }),
        );

        // Acceptance is awaited so a rejected prompt fails sendTurn directly;
        // failures after acceptance arrive through the event stream instead.
        yield* prepared.ctx.runtime.request(prepared.command).pipe(
          Effect.mapError(
            (cause): ProviderAdapterError =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "prompt",
                detail: cause.message,
                cause,
              }),
          ),
          Effect.tapError((error) =>
            withThreadLock(input.threadId, settleTurn(prepared.ctx, "failed", error.message)).pipe(
              Effect.ignore,
            ),
          ),
        );

        const resumeCursor = prepared.ctx.session.resumeCursor;
        return {
          threadId: input.threadId,
          turnId: prepared.turnId,
          ...(resumeCursor !== undefined ? { resumeCursor } : {}),
        } satisfies ProviderTurnStartResult;
      });

    const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (
      threadId,
      turnId,
    ) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          const target = turnId ?? ctx.activeTurnId ?? ctx.session.activeTurnId;
          if (!target || !ctx.streaming) return;
          ctx.interruptedTurnIds.add(target);
          // agent_settled follows the abort and settles the turn as cancelled.
          yield* ctx.runtime.notify({ type: "abort" }).pipe(Effect.ignore);
        }),
      );

    // Full-access mode: pi has no permission gate, so no request can be open.
    const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] = (
      _threadId,
      _requestId: ApprovalRequestId,
      _decision: ProviderApprovalDecision,
    ) => Effect.void;

    // Extension dialogs are auto-cancelled inside the runtime; nothing pending
    // survives here to answer.
    const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] = (
      threadId,
      _requestId,
      _answers: ProviderUserInputAnswers,
    ) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return;
      }).pipe(Effect.asVoid);

    const readThread: ProviderAdapterShape<ProviderAdapterError>["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns } satisfies ProviderThreadSnapshot;
      });

    const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = (
      threadId,
      numTurns,
    ) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "Pi RPC sessions do not support provider-side rollback yet.",
        });
      });

    const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: ProviderAdapterShape<ProviderAdapterError>["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: ProviderAdapterShape<ProviderAdapterError>["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: ProviderAdapterShape<ProviderAdapterError>["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.ignore(stopAll()).pipe(Effect.tap(() => PubSub.shutdown(runtimeEventPubSub))),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      readThread,
      rollbackThread,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}
