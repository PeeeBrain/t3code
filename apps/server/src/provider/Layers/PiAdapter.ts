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
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import { makePiRpcClient, type PiRpcClient } from "./PiRpcClient.ts";
import {
  PI_DEFAULT_THINKING_LEVEL,
  piForkMessagesFromData,
  piModelFromState,
  piSessionIdFromState,
  piThinkingLevelFromState,
  piThinkingLevelsFromData,
  type PiEvent,
  type PiThinkingLevel,
} from "./PiRpcProtocol.ts";
import {
  beginPiTurn,
  cancelPendingExtensionDialogs,
  initialPiTranslationState,
  markTurnInterrupted,
  piSettleTurn,
  reducePiEvent,
  resolvePiExtensionDialog,
  type PiTranslationDeps,
  type PiTranslationState,
} from "./PiEventTranslation.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_RESUME_VERSION = 1 as const;

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
  readonly requestTimeoutMs?: number;
}

interface PiSessionContext {
  readonly threadId: ThreadId;
  /** The `--session-id` handed to the CLI; also the resume cursor payload. */
  piSessionId: string;
  readonly runtime: PiRpcClient;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  exitFiber: Fiber.Fiber<void, never> | undefined;
  translation: PiTranslationState;
  currentModelId: string | undefined;
  currentThinkingLevel: PiThinkingLevel | undefined;
  /** Levels the current model supports, from `get_available_thinking_levels`. */
  availableThinkingLevels: ReadonlyArray<PiThinkingLevel>;
  /**
   * Pi user messages sent per T3 turn for turns started by this process
   * (1 + steering messages). Turns from before the process started are
   * assumed to carry exactly one user message; rollback targets the first
   * user message of the turn being rolled back to via Pi's `fork`.
   */
  userMessagesPerTurn: number[];
  stopped: boolean;
}

const UnknownRecord = Schema.Record(Schema.String, Schema.Unknown);
const isRecord = Schema.is(UnknownRecord);

function parsePiResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== PI_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function piModelParts(model: string): { provider: string; modelId: string } | undefined {
  const separator = model.indexOf("/");
  if (separator <= 0 || separator === model.length - 1) return undefined;
  return { provider: model.slice(0, separator), modelId: model.slice(separator + 1) };
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

    const translationDeps: PiTranslationDeps = {
      stamp: makeEventStamp,
      newItemId: () => Effect.map(randomUUIDv4, (id) => RuntimeItemId.make(id)),
    };

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

    const markSessionReady = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        const updatedAt = yield* nowIso;
        const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
        ctx.session = { ...readySession, status: "ready", updatedAt };
      });

    /** Applies a Pi event through the pure reducer and settles the session on turn completion. */
    const ingestPiEvent = (ctx: PiSessionContext, event: PiEvent) =>
      withThreadLock(
        ctx.threadId,
        Effect.gen(function* () {
          const result = yield* reducePiEvent(
            ctx.translation,
            event,
            { provider: PROVIDER, threadId: ctx.threadId },
            translationDeps,
          );
          ctx.translation = result.state;
          yield* Effect.forEach(result.events, offerRuntimeEvent, { discard: true });
          if (result.turnSettled !== undefined) yield* markSessionReady(ctx);
        }),
      );

    /** Cancels open extension dialogs and pushes the cancellation responses to Pi. */
    const cancelDialogs = (ctx: PiSessionContext, sendResponses: boolean) =>
      Effect.gen(function* () {
        const result = yield* cancelPendingExtensionDialogs(
          ctx.translation,
          { provider: PROVIDER, threadId: ctx.threadId },
          translationDeps,
        );
        ctx.translation = result.state;
        yield* Effect.forEach(result.events, offerRuntimeEvent, { discard: true });
        if (sendResponses) {
          yield* Effect.forEach(
            result.responses,
            (response) => Effect.ignore(ctx.runtime.notify(response)),
            { discard: true },
          );
        }
      });

    /** Force-settles an active turn outside the event stream (stop, process exit). */
    const forceSettleTurn = (
      ctx: PiSessionContext,
      outcome: "cancelled" | "failed",
      errorMessage?: string,
    ) =>
      Effect.gen(function* () {
        const result = yield* piSettleTurn(
          ctx.translation,
          outcome,
          { provider: PROVIDER, threadId: ctx.threadId },
          translationDeps,
          errorMessage,
        );
        ctx.translation = result.state;
        yield* Effect.forEach(result.events, offerRuntimeEvent, { discard: true });
        if (result.turnSettled !== undefined) yield* markSessionReady(ctx);
      });

    const stopSessionInternal = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        if (ctx.translation.streaming) yield* forceSettleTurn(ctx, "cancelled");
        ctx.stopped = true;
        yield* cancelDialogs(ctx, true);
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

    /**
     * Applies an explicit reasoning-effort selection, or deliberately leaves
     * Pi's configured level alone for the `default` sentinel.
     */
    const applyThinkingLevel = (
      ctx: PiSessionContext,
      requestedThinkingLevel: string | undefined,
      operation: "startSession" | "sendTurn",
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        if (
          requestedThinkingLevel === undefined ||
          requestedThinkingLevel === PI_DEFAULT_THINKING_LEVEL
        ) {
          return;
        }
        const level = ctx.availableThinkingLevels.find(
          (candidate) => candidate === requestedThinkingLevel,
        );
        if (level === undefined) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation,
            issue: `Pi model '${ctx.currentModelId ?? "unknown"}' does not support reasoning level '${requestedThinkingLevel}'. Available: ${ctx.availableThinkingLevels.join(", ") || "none"}.`,
          });
        }
        if (level === ctx.currentThinkingLevel) return;
        yield* ctx.runtime.request({ type: "set_thinking_level", level }).pipe(
          Effect.mapError(
            (cause): ProviderAdapterError =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "set_thinking_level",
                detail: cause.message,
                cause,
              }),
          ),
        );
        ctx.currentThinkingLevel = level;
      });

    /** Refreshes model, thinking level, and per-model level availability from the session. */
    const syncSessionModelState = (
      ctx: PiSessionContext,
    ): Effect.Effect<Record<string, unknown>, ProviderAdapterError> =>
      Effect.gen(function* () {
        const stateData = yield* ctx.runtime.request({ type: "get_state" }).pipe(
          Effect.mapError(
            (cause): ProviderAdapterError =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "get_state",
                detail: cause.message,
                cause,
              }),
          ),
        );
        ctx.currentModelId = piModelFromState(stateData) ?? ctx.currentModelId;
        ctx.currentThinkingLevel = piThinkingLevelFromState(stateData);
        const levelsData = yield* ctx.runtime
          .request({ type: "get_available_thinking_levels" })
          .pipe(
            Effect.mapError(
              (cause): ProviderAdapterError =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "get_available_thinking_levels",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
        ctx.availableThinkingLevels = piThinkingLevelsFromData(levelsData);
        return stateData;
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

          const runtime = yield* makePiRpcClient({
            settings: piSettings,
            ...(options?.environment ? { environment: options.environment } : {}),
            cwd,
            address: {
              kind: "session",
              sessionId: piSessionId,
              ...(modelSelection?.model ? { modelId: modelSelection.model } : {}),
            },
            ...(options?.requestTimeoutMs ? { requestTimeoutMs: options.requestTimeoutMs } : {}),
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
            exitFiber: undefined,
            translation: initialPiTranslationState(),
            currentModelId: modelSelection?.model,
            currentThinkingLevel: undefined,
            availableThinkingLevels: [],
            userMessagesPerTurn: [],
            stopped: false,
          };
          // Pump first, then start: pi emits events (extension UI prompts)
          // before any command response arrives.
          ctx.notificationFiber = yield* Stream.runForEach(runtime.events, (event) =>
            ingestPiEvent(ctx, event).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to process Pi runtime event.", { cause }),
              ),
            ),
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

          yield* syncSessionModelState(ctx);
          yield* applyThinkingLevel(
            ctx,
            getModelSelectionStringOptionValue(modelSelection, "reasoningEffort"),
            "startSession",
          );

          const readySession: ProviderSession = {
            ...ctx.session,
            status: "ready",
            ...(ctx.currentModelId ? { model: ctx.currentModelId } : {}),
            updatedAt: yield* nowIso,
          };
          ctx.session = readySession;
          sessions.set(input.threadId, ctx);
          sessionScopeTransferred = true;

          ctx.exitFiber = yield* runtime.exitCode.pipe(
            Effect.flatMap((exitCode) =>
              withThreadLock(
                input.threadId,
                Effect.gen(function* () {
                  if (ctx.stopped) return;
                  const reason = `Pi CLI exited unexpectedly with code ${exitCode}.`;
                  if (ctx.translation.streaming) yield* forceSettleTurn(ctx, "failed", reason);
                  ctx.stopped = true;
                  ctx.session = {
                    ...ctx.session,
                    status: "error",
                    updatedAt: yield* nowIso,
                  };
                  sessions.delete(input.threadId);
                  yield* cancelDialogs(ctx, false);
                  yield* offerRuntimeEvent({
                    type: "session.state.changed",
                    ...(yield* makeEventStamp()),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    payload: { state: "error", reason },
                  });
                  yield* offerRuntimeEvent({
                    type: "session.exited",
                    ...(yield* makeEventStamp()),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    payload: { exitKind: "error", reason, recoverable: true },
                  });
                  yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
                }),
              ),
            ),
            Effect.catchCause((cause) => Effect.logError("Pi exit watcher failed.", { cause })),
            Effect.forkDetach,
          );

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
            const activeTurnId = ctx.translation.activeTurnId;
            const steering = ctx.translation.streaming && activeTurnId !== undefined;
            const turnId =
              steering && activeTurnId !== undefined
                ? activeTurnId
                : TurnId.make(yield* randomUUIDv4);

            const selection =
              input.modelSelection?.instanceId === boundInstanceId
                ? input.modelSelection
                : undefined;
            const selectedModel = selection?.model.trim();
            if (!steering && selectedModel && selectedModel !== ctx.currentModelId) {
              const model = piModelParts(selectedModel);
              if (!model) {
                return yield* new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "sendTurn",
                  issue: `Pi model '${selectedModel}' must use provider/model-id format.`,
                });
              }
              yield* ctx.runtime
                .request({ type: "set_model", provider: model.provider, modelId: model.modelId })
                .pipe(
                  Effect.mapError(
                    (cause): ProviderAdapterError =>
                      new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "set_model",
                        detail: cause.message,
                        cause,
                      }),
                  ),
                );
              // Thinking levels are model-scoped; re-read them plus the
              // session's effective model state after the switch.
              yield* syncSessionModelState(ctx);
              ctx.currentModelId = selectedModel;
              ctx.session = { ...ctx.session, model: selectedModel, updatedAt: yield* nowIso };
            }

            const selectedThinkingLevel =
              selection !== undefined
                ? getModelSelectionStringOptionValue(selection, "reasoningEffort")
                : undefined;
            if (!steering) {
              yield* applyThinkingLevel(ctx, selectedThinkingLevel, "sendTurn");
            }

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

            const command = {
              type: "prompt" as const,
              message: text ?? "",
              ...(steering ? { streamingBehavior: "steer" as const } : {}),
              ...(images.length > 0 ? { images } : {}),
            };

            if (!steering) {
              ctx.translation = beginPiTurn(ctx.translation, turnId);
              ctx.userMessagesPerTurn.push(1);
              const updatedAt = yield* nowIso;
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: turnId,
                updatedAt,
              };
              yield* offerRuntimeEvent({
                type: "turn.started",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: {
                  ...(ctx.currentModelId !== undefined ? { model: ctx.currentModelId } : {}),
                  ...(ctx.currentThinkingLevel !== undefined
                    ? { effort: ctx.currentThinkingLevel }
                    : {}),
                },
              });
            }
            // Wait only for command acceptance. The full turn remains event-driven
            // and completes at Pi's `agent_settled` boundary.
            yield* ctx.runtime.request(command).pipe(
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
                error.message.includes("did not respond")
                  ? stopSessionInternal(ctx)
                  : steering
                    ? Effect.void
                    : Effect.gen(function* () {
                        yield* forceSettleTurn(ctx, "failed", error.message);
                        ctx.userMessagesPerTurn.pop();
                        ctx.translation = {
                          ...ctx.translation,
                          turns: ctx.translation.turns.filter((turn) => turn.id !== turnId),
                        };
                      }),
              ),
            );
            if (steering) {
              const lastTurnIndex = ctx.userMessagesPerTurn.length - 1;
              ctx.userMessagesPerTurn[lastTurnIndex] =
                (ctx.userMessagesPerTurn[lastTurnIndex] ?? 0) + 1;
            }
            return { ctx, turnId };
          }),
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
          const target = turnId ?? ctx.translation.activeTurnId ?? ctx.session.activeTurnId;
          if (!target || !ctx.translation.streaming) return;
          ctx.translation = markTurnInterrupted(ctx.translation, target);
          // Open extension dialogs can no longer be answered once the turn
          // is interrupted; release them before aborting.
          yield* cancelDialogs(ctx, true);
          // Pi continues queued steering after abort unless the queue is
          // cleared first. agent_settled then settles the turn as cancelled.
          yield* ctx.runtime.request({ type: "clear_queue" }, 1_000).pipe(Effect.ignore);
          yield* ctx.runtime.notify({ type: "abort" }).pipe(
            Effect.mapError(
              (cause): ProviderAdapterError =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "abort",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
        }),
      );

    // Full-access mode: pi has no permission gate, so no request can be open.
    const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] = (
      _threadId,
      _requestId: ApprovalRequestId,
      _decision: ProviderApprovalDecision,
    ) => Effect.void;

    /**
     * Answers a Pi extension dialog. Blocking `extension_ui_request`s are
     * surfaced as `user-input.requested`; this maps the canonical answers
     * back into a correlated `extension_ui_response`.
     */
    const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] = (
      threadId,
      requestId: ApprovalRequestId,
      answers: ProviderUserInputAnswers,
    ) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          const result = yield* resolvePiExtensionDialog(
            ctx.translation,
            requestId,
            answers,
            { provider: PROVIDER, threadId: ctx.threadId },
            translationDeps,
          );
          if (result.response === undefined) return;
          ctx.translation = result.state;
          yield* Effect.forEach(result.events, offerRuntimeEvent, { discard: true });
          yield* ctx.runtime.notify(result.response).pipe(
            Effect.mapError(
              (cause): ProviderAdapterError =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "extension_ui_response",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
        }),
      );

    const readThread: ProviderAdapterShape<ProviderAdapterError>["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.translation.turns } satisfies ProviderThreadSnapshot;
      });

    /**
     * Rolls back N turns by forking the Pi session at the user message that
     * started the oldest dropped turn. Pi implements rollback as a branch
     * fork, which mints a new session id; the resume cursor follows so thread
     * resume after a server restart lands on the rolled-back branch.
     */
    const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = (
      threadId,
      numTurns,
    ) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          if (!Number.isInteger(numTurns) || numTurns < 1) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "numTurns must be an integer >= 1.",
            });
          }
          if (ctx.translation.streaming) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "fork",
              detail: "Cannot roll back a Pi thread while a turn is still running.",
            });
          }
          const forkData = yield* ctx.runtime.request({ type: "get_fork_messages" }).pipe(
            Effect.mapError(
              (cause): ProviderAdapterError =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "get_fork_messages",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const forkMessages = piForkMessagesFromData(forkData);
          // Turns from before this process started are assumed to carry one
          // user message each; turns this process started may carry steering
          // messages beyond the first.
          const trackedMessages = ctx.userMessagesPerTurn.reduce((sum, count) => sum + count, 0);
          const resumedMessageCount = Math.max(0, forkMessages.length - trackedMessages);
          const turnCount = resumedMessageCount + ctx.userMessagesPerTurn.length;
          if (numTurns > turnCount) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "fork",
              detail: `Cannot roll back ${numTurns} turns; the Pi session only has ${turnCount}.`,
            });
          }
          // First user message of turn (turnCount - numTurns + 1), 1-based.
          const targetTurn = turnCount - numTurns + 1;
          let targetIndex = targetTurn; // pre-process turns contribute 1 each
          if (targetTurn > resumedMessageCount) {
            targetIndex =
              resumedMessageCount +
              ctx.userMessagesPerTurn
                .slice(0, targetTurn - resumedMessageCount - 1)
                .reduce((sum, count) => sum + count, 0) +
              1;
          }
          const target = forkMessages[targetIndex - 1];
          if (target === undefined) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "fork",
              detail: "Pi did not report a user message to fork the session back to.",
            });
          }
          const forkResult = yield* ctx.runtime
            .request({ type: "fork", entryId: target.entryId })
            .pipe(
              Effect.mapError(
                (cause): ProviderAdapterError =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "fork",
                    detail: cause.message,
                    cause,
                  }),
              ),
            );
          if (forkResult["cancelled"] === true) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "fork",
              detail: "A Pi extension cancelled the session fork.",
            });
          }
          // Forking mints a new session id; adopt it so the resume cursor and
          // any later restart land on the rolled-back branch.
          const stateData = yield* syncSessionModelState(ctx);
          const newSessionId = piSessionIdFromState(stateData);
          if (newSessionId !== undefined && newSessionId !== ctx.piSessionId) {
            ctx.piSessionId = newSessionId;
            ctx.session = {
              ...ctx.session,
              resumeCursor: { schemaVersion: PI_RESUME_VERSION, sessionId: newSessionId },
              updatedAt: yield* nowIso,
            };
          }
          ctx.userMessagesPerTurn = ctx.userMessagesPerTurn.slice(
            0,
            Math.max(0, targetTurn - resumedMessageCount - 1),
          );
          const keptTurns = ctx.translation.turns.slice(0, ctx.userMessagesPerTurn.length);
          ctx.translation = { ...ctx.translation, turns: keptTurns };
          return { threadId, turns: ctx.translation.turns } satisfies ProviderThreadSnapshot;
        }),
      );

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
