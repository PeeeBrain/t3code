/**
 * Pi RPC session runtime - one long-lived `pi --mode rpc` subprocess per
 * provider session.
 *
 * Pi's RPC mode is strict NDJSON over stdio (split on `\n` only; raw `\r`
 * cannot occur inside valid JSON, so Effect's Ndjson channel is
 * protocol-compliant where Node's `readline` would not be). Commands carry an
 * optional `id`; the matching `response` echoes it. Everything else on stdout
 * is an event belonging to the session's event stream.
 *
 * The runtime owns transport only: spawn/teardown, request correlation, and a
 * decoded event stream. Translating pi events into `ProviderRuntimeEvent`s is
 * the adapter's job.
 *
 * @module PiRpcSessionRuntime
 */
import { type PiSettings } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

export class PiRuntimeError extends Schema.TaggedErrorClass<PiRuntimeError>()("PiRuntimeError", {
  operation: Schema.String,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Pi runtime ${this.operation} failed: ${this.detail}`;
  }
}

export interface PiRpcSpawnInput {
  readonly settings: PiSettings;
  /** Merged instance + process environment handed to the CLI. Defaults to `process.env`. */
  readonly environment?: NodeJS.ProcessEnv | undefined;
  readonly cwd: string;
  /**
   * Exact project session id passed as `--session-id`. Pi creates the session
   * when missing and reopens it on later starts, which is what makes thread
   * resume work across server restarts.
   */
  readonly sessionId: string;
  /** Optional `provider/model-id` or model pattern for this session. */
  readonly modelId?: string | undefined;
}

export interface PiRpcSessionRuntime {
  /** Spawns the CLI and begins pumping stdout. Idempotent per runtime. */
  readonly start: () => Effect.Effect<void, PiRuntimeError>;
  /**
   * Sends one command and awaits its correlated `response`. Fails when the
   * response reports `success: false`, the process exits first, or pi rejects
   * the command before acceptance.
   */
  readonly request: (
    command: Record<string, unknown>,
  ) => Effect.Effect<Record<string, unknown>, PiRuntimeError>;
  /** Sends a command without awaiting any response (e.g. `abort`). */
  readonly notify: (command: Record<string, unknown>) => Effect.Effect<void, PiRuntimeError>;
  /** Session-level events, responses excluded. */
  readonly events: Stream.Stream<unknown>;
  /** Completes with the CLI's exit code once the process is gone. */
  readonly exitCode: Effect.Effect<number>;
}

/** Commands whose dialogs block pi until answered; everything else is fire-and-forget. */
const BLOCKING_EXTENSION_UI_METHODS = new Set(["select", "confirm", "input", "editor"]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Builds the argv tail for `pi --mode rpc`. Kept in one place so the probe,
 * the adapter, and tests agree on how sessions are addressed.
 */
export function buildPiRpcArgs(input: {
  readonly settings: PiSettings;
  readonly sessionId: string;
  readonly modelId?: string | undefined;
}): ReadonlyArray<string> {
  return [
    "--mode",
    "rpc",
    "--session-id",
    input.sessionId,
    ...(input.modelId ? ["--model", input.modelId] : []),
    // Custom models from T3 settings are appended as extra --model patterns so
    // they join pi's own catalog rather than replacing it.
  ];
}

export const makePiRpcSessionRuntime = (
  input: PiRpcSpawnInput,
): Effect.Effect<
  PiRpcSessionRuntime,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;
    // Captured once so `start` and the pump fibers fork into the caller's
    // scope without leaking a Scope requirement into the runtime's methods.
    const scope = yield* Scope.Scope;

    const eventPubSub = yield* PubSub.unbounded<unknown>();
    // Commands are serialized through a queue feeding one long-lived stdin
    // pump. Piping each write separately would end pi's stdin after the first
    // command, which pi treats as a session end.
    const writeQueue = yield* Queue.unbounded<string>();
    // Pending commands keyed by correlation id; failed wholesale on exit.
    const pendingRef = yield* Ref.make(
      new Map<string, Deferred.Deferred<Record<string, unknown>, PiRuntimeError>>(),
    );
    const startedRef = yield* Ref.make(false);
    const exitDeferred = yield* Deferred.make<number, never>();
    let handle: ChildProcessSpawner.ChildProcessHandle | undefined;

    const randomId = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) => new PiRuntimeError({ operation: "request-id", detail: "UUIDv4 failed", cause }),
      ),
      Effect.orDie,
    );

    const failPending = (detail: string) =>
      Ref.getAndSet(pendingRef, new Map()).pipe(
        Effect.flatMap((pending) =>
          Effect.forEach(
            Array.from(pending.values()),
            (deferred) =>
              Deferred.fail(deferred, new PiRuntimeError({ operation: "request", detail })).pipe(
                Effect.ignore,
              ),
            { discard: true },
          ),
        ),
      );

    const writeLine = (payload: Record<string, unknown>): Effect.Effect<void, PiRuntimeError> => {
      if (!handle) {
        return Effect.fail(
          new PiRuntimeError({ operation: "write", detail: "Pi process is not running." }),
        );
      }
      let serialized: string;
      try {
        serialized = JSON.stringify(payload);
      } catch (cause) {
        return Effect.fail(
          new PiRuntimeError({
            operation: "write",
            detail: "Command is not JSON-serializable.",
            cause,
          }),
        );
      }
      return Queue.offer(writeQueue, `${serialized}\n`).pipe(Effect.asVoid);
    };

    const handleLine = (value: unknown): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!isRecord(value)) return;
        if (value["type"] === "response") {
          const id = typeof value["id"] === "string" ? value["id"] : undefined;
          if (id === undefined) return;
          const pending = (yield* Ref.get(pendingRef)).get(id);
          if (!pending) return;
          if (value["success"] === true) {
            const data = isRecord(value["data"]) ? value["data"] : {};
            yield* Deferred.succeed(pending, data).pipe(Effect.asVoid, Effect.ignore);
          } else {
            const error =
              typeof value["error"] === "string" ? value["error"] : "Pi command failed.";
            yield* Deferred.fail(
              pending,
              new PiRuntimeError({
                operation: String(value["command"] ?? "command"),
                detail: error,
              }),
            ).pipe(Effect.ignore);
          }
          return;
        }
        yield* PubSub.publish(eventPubSub, value).pipe(Effect.asVoid);
      });

    const answerBlockingDialogs = (value: unknown): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!isRecord(value) || value["type"] !== "extension_ui_request") return;
        const method = value["method"];
        if (typeof method !== "string" || !BLOCKING_EXTENSION_UI_METHODS.has(method)) return;
        // Pi has no permission gate; its only blocking dialogs are extension
        // UI prompts. Full-access mode has no one to ask, so decline them and
        // let the agent continue without the dialog's result.
        yield* writeLine({
          type: "extension_ui_response",
          ...(typeof value["id"] === "string" ? { id: value["id"] } : {}),
          cancelled: true,
        }).pipe(Effect.ignore);
      });

    const start = (): Effect.Effect<void, PiRuntimeError> =>
      Effect.gen(function* () {
        if (yield* Ref.get(startedRef)) return;
        yield* Ref.set(startedRef, true);

        const args = buildPiRpcArgs(input);
        const childEnvironment = input.environment ?? process.env;
        const spawnCommand = yield* resolveSpawnCommand(input.settings.binaryPath, args, {
          env: childEnvironment,
        });
        const child = yield* spawner
          .spawn(
            ChildProcess.make(spawnCommand.command, spawnCommand.args, {
              cwd: input.cwd,
              env: childEnvironment,
              shell: spawnCommand.shell,
              stdin: { stream: "pipe", endOnDone: false },
              stdout: "pipe",
              stderr: "pipe",
            }),
          )
          .pipe(
            // The child's lifetime binds to the session scope captured above.
            Effect.provideService(Scope.Scope, scope),
            Effect.mapError(
              (cause) => new PiRuntimeError({ operation: "spawn", detail: cause.message, cause }),
            ),
          );
        handle = child;

        // One stdin pump for the process's whole life: the queue-backed stream
        // never completes, so pi never sees EOF until we kill it.
        yield* Stream.run(Stream.encodeText(Stream.fromQueue(writeQueue)), child.stdin).pipe(
          Effect.ignore,
          Effect.forkIn(scope),
        );

        yield* child.stdout.pipe(
          Stream.pipeThroughChannel(Ndjson.decode({ ignoreEmptyLines: true })),
          Stream.mapEffect((line) =>
            handleLine(line).pipe(
              Effect.andThen(answerBlockingDialogs(line)),
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to process Pi RPC line.", { cause }),
              ),
            ),
          ),
          Stream.runDrain,
          Effect.ignore,
          Effect.forkIn(scope),
        );
        const stderrTailRef = yield* Ref.make("");
        yield* child.stderr.pipe(
          Stream.decodeText(),
          Stream.mapEffect((chunk) =>
            Ref.update(stderrTailRef, (current) => (current + chunk).slice(-2000)),
          ),
          Stream.runDrain,
          Effect.ignore,
          Effect.forkIn(scope),
        );

        yield* child.exitCode.pipe(
          Effect.flatMap((code) =>
            Effect.gen(function* () {
              handle = undefined;
              const stderrTail = (yield* Ref.get(stderrTailRef)).trim();
              if (Number(code) !== 0 || stderrTail.length > 0) {
                yield* Effect.logWarning("Pi CLI process exited.", {
                  exitCode: Number(code),
                  ...(stderrTail ? { stderr: stderrTail.slice(-500) } : {}),
                });
              }
              yield* failPending(`Pi process exited with code ${Number(code)}.`);
              yield* Deferred.succeed(exitDeferred, Number(code)).pipe(Effect.ignore);
            }),
          ),
          Effect.ignore,
          Effect.forkIn(scope),
        );
      });

    const request = (command: Record<string, unknown>) =>
      Effect.gen(function* () {
        const id = yield* randomId;
        const deferred = yield* Deferred.make<Record<string, unknown>, PiRuntimeError>();
        yield* Ref.update(pendingRef, (pending) => {
          const next = new Map(pending);
          next.set(id, deferred);
          return next;
        });
        yield* writeLine({ ...command, id }).pipe(
          Effect.tapError(() =>
            Ref.update(pendingRef, (pending) => {
              const next = new Map(pending);
              next.delete(id);
              return next;
            }),
          ),
        );
        return yield* Deferred.await(deferred);
      });

    const notify = (command: Record<string, unknown>) => writeLine(command);

    const events = Stream.fromPubSub(eventPubSub);

    const exitCode = Deferred.await(exitDeferred);

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        if (handle) {
          yield* handle.kill().pipe(Effect.ignore);
        }
        yield* failPending("Pi runtime was torn down.");
      }),
    );

    return {
      start,
      request,
      notify,
      events,
      exitCode,
    } satisfies PiRpcSessionRuntime;
  });

/**
 * Extracts `{sessionId}` from pi's `get_state` response data, tolerating
 * protocol additions. Returns `undefined` when pi did not report one.
 */
export function piSessionIdFromState(data: Record<string, unknown>): string | undefined {
  const sessionId = data["sessionId"];
  return typeof sessionId === "string" && sessionId.trim() ? sessionId.trim() : undefined;
}

export function piModelFromState(data: Record<string, unknown>): string | undefined {
  const model = data["model"];
  if (!isRecord(model)) return undefined;
  const id = model["id"];
  return typeof id === "string" && id.trim() ? id.trim() : undefined;
}

export function piStreamingFromState(data: Record<string, unknown>): boolean {
  return data["isStreaming"] === true || data["isCompacting"] === true;
}
