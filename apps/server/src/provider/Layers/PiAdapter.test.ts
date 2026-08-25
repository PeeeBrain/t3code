// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

import { ServerConfig } from "../../config.ts";
import { makePiAdapter } from "./PiAdapter.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const PI_RESUME_VERSION = 1 as const;

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
// Layers -> provider -> src -> server -> apps -> repo root.
const mockAgentPath = NodePath.join(__dirname, "../../../../../scripts/pi-rpc-mock-agent.mjs");
const mockAgentCommand = process.execPath;

async function makeMockPiWrapper(platform: NodeJS.Platform, extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-rpc-mock-"));
  const envExports = Object.entries(extraEnv ?? {});

  if (platform === "win32") {
    // Windows cannot exec POSIX shims; cmd wrappers get shell:true handling
    // from resolveSpawnCommand instead.
    const wrapperPath = NodePath.join(dir, "fake-pi.cmd");
    const envSets = envExports.map(([key, value]) => `set "${key}=${value}"`).join("\r\n");
    const script = [
      "@echo off",
      ...(envSets ? [envSets] : []),
      `"${mockAgentCommand}" "${mockAgentPath}" %*`,
    ].join("\r\n");
    await NodeFSP.writeFile(wrapperPath, script, "utf8");
    return wrapperPath;
  }

  const wrapperPath = NodePath.join(dir, "fake-pi.sh");
  const envLines = envExports
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envLines}
exec ${JSON.stringify(mockAgentCommand)} ${JSON.stringify(mockAgentPath)} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

const piAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (binaryPath: string) =>
  makePiAdapter(decodePiSettings({ binaryPath })).pipe(Effect.orDie);

function collectEventsUntilTurnCompleted(adapter: {
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}) {
  return Effect.gen(function* () {
    const runtimeEvents: ProviderRuntimeEvent[] = [];
    const turnCompleted = yield* Deferred.make<void>();
    const fiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        runtimeEvents.push(event);
      }).pipe(
        Effect.andThen(
          event.type === "turn.completed"
            ? Deferred.succeed(turnCompleted, undefined)
            : Effect.void,
        ),
      ),
    ).pipe(Effect.forkChild);
    return {
      events: runtimeEvents,
      awaitTurnCompleted: () => Deferred.await(turnCompleted),
      interrupt: () => Fiber.interrupt(fiber).pipe(Effect.asVoid),
    } as const;
  });
}

it.layer(piAdapterTestLayer)("PiAdapterLive", (it) => {
  it.effect("starts a session and maps the mock pi prompt flow to runtime events", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-mock-thread");
      const platform = yield* HostProcessPlatform;
      const wrapperPath = yield* Effect.promise(() => makeMockPiWrapper(platform));
      const adapter = yield* makeTestAdapter(wrapperPath);

      const collected = yield* collectEventsUntilTurnCompleted(adapter);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: "mock/mock-model" },
      });

      assert.equal(session.provider, "pi");
      assert.equal(session.status, "ready");
      assert.equal(session.model, "mock/mock-model");
      if (
        typeof session.resumeCursor !== "object" ||
        session.resumeCursor === null ||
        (session.resumeCursor as Record<string, unknown>)["schemaVersion"] !== PI_RESUME_VERSION
      ) {
        throw new Error("Unexpected resume cursor shape.");
      }

      yield* adapter.sendTurn({
        threadId,
        input: "hello pi",
        attachments: [],
      });

      yield* collected.awaitTurnCompleted();
      yield* collected.interrupt();

      const types = collected.events.map((e) => e.type);
      assert.includeMembers(types, [
        "session.started",
        "session.state.changed",
        "thread.started",
        "turn.started",
        "turn.completed",
        "item.completed",
      ]);
      assert.isTrue(types.includes("content.delta"));

      const deltas = collected.events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
          event.type === "content.delta",
      );
      assert.deepEqual(deltas.map((event) => event.payload.delta).join(""), "Hello world");
    }),
  );

  it.effect("aborted turns settle as cancelled", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-mock-thread-abort");
      const platform = yield* HostProcessPlatform;
      const wrapperPath = yield* Effect.promise(() =>
        makeMockPiWrapper(platform, { PI_MOCK_MODE: "hold" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const collected = yield* collectEventsUntilTurnCompleted(adapter);
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "long running work",
        attachments: [],
      });
      yield* adapter.interruptTurn(threadId);

      yield* collected.awaitTurnCompleted();
      yield* collected.interrupt();

      const completions = collected.events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed",
      );
      assert.equal(completions.length, 1);
      assert.equal(completions[0]?.payload.state, "cancelled");
    }),
  );
});
