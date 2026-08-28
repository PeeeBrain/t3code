/**
 * Pi RPC client - one JSONL RPC transport over a `pi --mode rpc` subprocess.
 *
 * Used for both long-lived per-thread sessions (`--session-id`) and bounded
 * no-session discovery (`--no-session`). The client owns transport only:
 * spawn/teardown, strict NDJSON framing, request correlation, and a typed
 * event stream. Translating Pi events into `ProviderRuntimeEvent`s is
 * `PiEventTranslation`'s job; parsing payload data is `PiRpcProtocol`'s.
 *
 * Pi's RPC mode is strict NDJSON over stdio (split on `\n` only; raw `\r`
 * cannot occur inside valid JSON, so Effect's Ndjson channel is
 * protocol-compliant where Node's `readline` would not be). Commands carry an
 * optional `id`; the matching `response` echoes it. Everything else on stdout
 * is an event belonging to the session's event stream.
 *
 * @module PiRpcClient
 */
import { type PiSettings } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  decodePiEvent,
  decodePiRpcResponse,
  type PiEvent,
  type PiRpcCommand,
} from "./PiRpcProtocol.ts";

export class PiRuntimeError extends Schema.TaggedErrorClass<PiRuntimeError>()("PiRuntimeError", {
  operation: Schema.String,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Pi runtime ${this.operation} failed: ${this.detail}`;
  }
}

/**
 * How the CLI process is addressed. Sessions pass `--session-id` so Pi
 * creates or reopens a durable project session; discovery passes
 * `--no-session` so nothing is persisted.
 */
export type PiRpcAddress =
  | { readonly kind: "session"; readonly sessionId: string; readonly modelId?: string | undefined }
  | { readonly kind: "no-session" };

export interface PiRpcClientInput {
  readonly settings: PiSettings;
  /** Merged instance + process environment handed to the CLI. Defaults to `process.env`. */
  readonly environment?: NodeJS.ProcessEnv | undefined;
  /**
   * Working directory for the CLI process. Pi resolves project-scoped
   * extensions, skills, and prompt templates from this directory.
   */
  readonly cwd: string;
  readonly address: PiRpcAddress;
  /** Request deadline; exposed so transport tests can use a short timeout. */
  readonly requestTimeoutMs?: number | undefined;
}

export interface PiRpcClient {
  /** Spawns the CLI and begins pumping stdout. Idempotent per client. */
  readonly start: () => Effect.Effect<void, PiRuntimeError>;
  /**
   * Sends one command and awaits its correlated `response`. Fails when the
   * response reports `success: false`, the process exits first, or the
   * request deadline passes. A request that never replies fails on its own
   * timeout, so sibling requests still resolve.
   */
  readonly request: (
    command: PiRpcCommand,
    timeoutMs?: number,
  ) => Effect.Effect<Record<string, unknown>, PiRuntimeError>;
  /** Sends a command without awaiting any response (e.g. `abort`). */
  readonly notify: (command: PiRpcCommand) => Effect.Effect<void, PiRuntimeError>;
  /** Session-level events, responses excluded. */
  readonly events: Stream.Stream<PiEvent>;
  /** Completes with the CLI's exit code once the process is gone. */
  readonly exitCode: Effect.Effect<number>;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/**
 * Builds the argv tail for `pi --mode rpc`. Kept in one place so the adapter,
 * discovery, and tests agree on how sessions are addressed.
 */
export function buildPiRpcArgs(address: PiRpcAddress): ReadonlyArray<string> {
  switch (address.kind) {
    case "session":
      return [
        "--mode",
        "rpc",
        "--session-id",
        address.sessionId,
        ...(address.modelId ? ["--model", address.modelId] : []),
      ];
    case "no-session":
      return ["--mode", "rpc", "--no-session"];
  }
}

export const makePiRpcClient = (
  input: PiRpcClientInput,
): Effect.Effect<
  PiRpcClient,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;
    // Captured once so `start` and the pump fibers fork into the caller's
    // scope without leaking a Scope requirement into the client's methods.
    const scope = yield* Scope.Scope;

    const eventPubSub = yield* PubSub.unbounded<PiEvent>();
    // Commands are serialized through a queue feeding one long-lived stdin
    // pump. Piping each write separately would end pi's stdin after the first
    // command, which pi treats as a session end.
    const writeQueue = yield* Queue.unbounded<string>();
    // Pending commands keyed by correlation id; failed wholesale on exit.
    const pendingRef = yield* Ref.make(
      new Map<string, Deferred.Deferred<Record<string, unknown>, PiRuntimeError>>(),
    );
    const lifecycleRef = yield* Ref.make<"idle" | "running" | "closed">("idle");
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

    const writeLine = (payload: object): Effect.Effect<void, PiRuntimeError> => {
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
        const response = decodePiRpcResponse(value);
        if (response !== undefined) {
          const responseId = response.id;
          if (responseId === undefined) return;
          const pending = (yield* Ref.get(pendingRef)).get(responseId);
          if (!pending) return;
          yield* Ref.update(pendingRef, (current) => {
            const next = new Map(current);
            next.delete(responseId);
            return next;
          });
          if (response.kind === "success") {
            yield* Deferred.succeed(pending, response.data).pipe(Effect.asVoid, Effect.ignore);
          } else {
            yield* Deferred.fail(
              pending,
              new PiRuntimeError({ operation: response.command, detail: response.error }),
            ).pipe(Effect.ignore);
          }
          return;
        }
        const event = decodePiEvent(value);
        // Undecodable lines (unknown future event types) are dropped; new Pi
        // versions must not break the transport.
        if (event === undefined) return;
        yield* PubSub.publish(eventPubSub, event).pipe(Effect.asVoid);
      });

    const start = (): Effect.Effect<void, PiRuntimeError> =>
      Effect.gen(function* () {
        const lifecycle = yield* Ref.get(lifecycleRef);
        if (lifecycle === "running") return;
        if (lifecycle === "closed") {
          return yield* new PiRuntimeError({
            operation: "start",
            detail: "Pi runtime has already closed.",
          });
        }

        const args = buildPiRpcArgs(input.address);
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
            // The child's lifetime binds to the scope captured above.
            Effect.provideService(Scope.Scope, scope),
            Effect.mapError(
              (cause) => new PiRuntimeError({ operation: "spawn", detail: cause.message, cause }),
            ),
          );
        handle = child;
        yield* Ref.set(lifecycleRef, "running");

        const failTransport = (detail: string) =>
          Effect.gen(function* () {
            yield* Ref.set(lifecycleRef, "closed");
            yield* failPending(detail);
            if (handle) yield* handle.kill().pipe(Effect.ignore);
          });

        // One stdin pump for the process's whole life: the queue-backed stream
        // never completes, so pi never sees EOF until we kill it.
        yield* Stream.run(Stream.encodeText(Stream.fromQueue(writeQueue)), child.stdin).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : Effect.logError("Pi RPC stdin pump failed.", { cause }).pipe(
                  Effect.andThen(failTransport("Pi stdin transport failed.")),
                ),
          ),
          Effect.forkIn(scope),
        );

        yield* child.stdout.pipe(
          Stream.pipeThroughChannel(Ndjson.decode({ ignoreEmptyLines: true })),
          Stream.mapEffect((line) =>
            handleLine(line).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to process Pi RPC line.", { cause }),
              ),
            ),
          ),
          Stream.runDrain,
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : Effect.logError("Pi RPC stdout pump failed.", { cause }).pipe(
                  Effect.andThen(failTransport("Pi stdout transport failed.")),
                ),
          ),
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
              yield* Ref.set(lifecycleRef, "closed");
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

    const request = (command: PiRpcCommand, timeoutMs?: number) =>
      Effect.gen(function* () {
        if ((yield* Ref.get(lifecycleRef)) !== "running") {
          return yield* new PiRuntimeError({
            operation: "request",
            detail: "Pi process is not running.",
          });
        }
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
        const result = yield* Deferred.await(deferred).pipe(
          Effect.timeoutOption(timeoutMs ?? input.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS),
        );
        if (Option.isSome(result)) return result.value;
        yield* Ref.update(pendingRef, (pending) => {
          const next = new Map(pending);
          next.delete(id);
          return next;
        });
        return yield* new PiRuntimeError({
          operation: command.type,
          detail: `Pi did not respond within ${timeoutMs ?? input.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS}ms.`,
        });
      });

    const notify = (command: PiRpcCommand) => writeLine(command);

    const events = Stream.fromPubSub(eventPubSub);

    const exitCode = Deferred.await(exitDeferred);

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        if (handle) {
          yield* handle.kill().pipe(Effect.ignore);
        }
        yield* Ref.set(lifecycleRef, "closed");
        yield* failPending("Pi runtime was torn down.");
      }),
    );

    return {
      start,
      request,
      notify,
      events,
      exitCode,
    } satisfies PiRpcClient;
  });
