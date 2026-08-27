import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { PiSettings } from "@t3tools/contracts";

import * as NodeOS from "node:os";

import {
  buildPiReasoningCapabilities,
  checkPiProviderStatus,
  countPiUpstreamProviders,
  filterModelsByConfiguredProviders,
  parsePiAuthProviderStore,
  parsePiAvailableModelsResponse,
  parsePiCommandsResponse,
} from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const encodeUnknownJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

type ProbeProcess = {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly code?: number;
  readonly exitCode?: Effect.Effect<ChildProcessSpawner.ExitCode>;
  /** Makes the spawn itself never settle so the probe's timeout is what fires. */
  readonly hangSpawn?: boolean;
};

function makeProbeHandle(input: ProbeProcess) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: input.exitCode ?? Effect.succeed(ChildProcessSpawner.ExitCode(input.code ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.encodeText(Stream.make(input.stdout ?? "")),
    stderr: Stream.encodeText(Stream.make(input.stderr ?? "")),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function makeProbeSpawner(processes: ReadonlyArray<ProbeProcess>) {
  let nextProcess = 0;
  return ChildProcessSpawner.make(() =>
    Effect.suspend(() => {
      const process = processes[nextProcess];
      nextProcess += 1;
      if (process === undefined) {
        return Effect.sync(() =>
          makeProbeHandle({ code: 1, stderr: "Unexpected extra Pi probe." }),
        );
      }
      if (process.hangSpawn) {
        // Blocks well past the probe timeout; the probe's interrupt lands
        // while the sleep is still pending, without leaving a deadlocked
        // fiber behind for other tests in the same worker.
        return Effect.sleep("2 minutes").pipe(Effect.andThen(Effect.never));
      }
      return Effect.sync(() => makeProbeHandle(process));
    }),
  );
}

const testLayer = NodeServices.layer;

const makePiSettings = (customModels: ReadonlyArray<string> = []) =>
  decodePiSettings({
    enabled: true,
    binaryPath: "pi",
    customModels,
  });

/**
 * Points the auth store at an isolated temp dir so the probe's model filtering
 * is hermetic; tests that exercise the store write their own auth.json there.
 */
let isolatedAuthDirSequence = 0;
const makeIsolatedAuthDir = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dir = path.join(
    NodeOS.tmpdir(),
    `t3-pi-probe-${process.pid}-${(isolatedAuthDirSequence += 1)}`,
  );
  yield* fileSystem.makeDirectory(dir, { recursive: true }).pipe(Effect.ignore);
  return dir;
});

const writeAuthStore = (dir: string, store: Record<string, unknown>) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fileSystem.writeFile(
      path.join(dir, "auth.json"),
      new TextEncoder().encode(encodeUnknownJson(store)),
    );
  });

const runCheck = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  settings = makePiSettings(),
  environment?: NodeJS.ProcessEnv,
  options?: { readonly modelDiscoveryTimeoutMs?: number },
) =>
  Effect.gen(function* () {
    const authDir = yield* makeIsolatedAuthDir;
    const env = environment ?? { PI_CODING_AGENT_DIR: authDir };
    return yield* checkPiProviderStatus(settings, env, options).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
          // The probe is platform-independent; avoid PATH resolution differences
          // between the Windows and POSIX test runners.
          Layer.succeed(HostProcessPlatform, "linux"),
        ),
      ),
    );
  });

const rpcModelRow = (input: {
  provider: string;
  id: string;
  name?: string;
  reasoning?: boolean;
  thinkingLevels?: ReadonlyArray<string>;
}) => {
  const levels = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];
  return {
    id: input.id,
    ...(input.name ? { name: input.name } : {}),
    provider: input.provider,
    ...(input.reasoning === undefined ? {} : { reasoning: input.reasoning }),
    ...(input.thinkingLevels
      ? {
          thinkingLevelMap: Object.fromEntries(
            levels.map((level) => [level, input.thinkingLevels?.includes(level) ? level : null]),
          ),
        }
      : {}),
  };
};

const rpcModelsLine = (models: ReadonlyArray<ReturnType<typeof rpcModelRow>>) =>
  encodeUnknownJson({
    type: "response",
    id: "t3-models",
    command: "get_available_models",
    success: true,
    data: { models },
  });

const rpcCommandsLine = (commands: ReadonlyArray<Record<string, unknown>>) =>
  encodeUnknownJson({
    type: "response",
    id: "t3-inventory",
    command: "get_commands",
    success: true,
    data: { commands },
  });

const rpcInventoryStdout = (
  models: Parameters<typeof rpcModelsLine>[0],
  commands: Parameters<typeof rpcCommandsLine>[0],
) => `${rpcModelsLine(models)}\n${rpcCommandsLine(commands)}\n`;

describe("parsePiAvailableModelsResponse", () => {
  it("parses provider slugs, display names, and per-model thinking levels", () => {
    const models = parsePiAvailableModelsResponse(
      rpcModelsLine([
        rpcModelRow({
          provider: "opencode-go",
          id: "glm-5.3-flash",
          name: "GLM-5.3-Flash (2x usage)",
          reasoning: true,
          thinkingLevels: ["low", "high", "max"],
        }),
        rpcModelRow({ provider: "opencode-go", id: "glm-5.3-flash", name: "duplicate" }),
        rpcModelRow({ provider: "openai", id: "gpt-5", reasoning: false, thinkingLevels: [] }),
      ]) + "\n",
    );

    expect(models.map((model) => model.slug)).toEqual([
      "opencode-go/glm-5.3-flash",
      "openai/gpt-5",
    ]);
    expect(models[0]?.name).toBe("GLM-5.3-Flash (2x usage)");
    const descriptor = models[0]?.capabilities?.optionDescriptors?.[0];
    expect(descriptor?.type).toBe("select");
    if (descriptor?.type === "select") {
      expect(descriptor.options.map((option) => option.id)).toEqual(["low", "high", "max"]);
      expect(descriptor.currentValue).toBe("max");
    }
    expect(models[1]?.capabilities?.optionDescriptors).toEqual([]);
    expect(countPiUpstreamProviders(models)).toBe(2);
  });

  it("falls back to the portable Off–High set when reasoning has no map", () => {
    const models = parsePiAvailableModelsResponse(
      rpcModelsLine([{ provider: "anthropic", id: "claude-opus-4-5", reasoning: true }]) + "\n",
    );

    const descriptor = models[0]?.capabilities?.optionDescriptors?.[0];
    expect(descriptor?.type).toBe("select");
    if (descriptor?.type === "select") {
      expect(descriptor.options.map((option) => option.id)).toEqual([
        "off",
        "minimal",
        "low",
        "medium",
        "high",
      ]);
      expect(descriptor.currentValue).toBe("medium");
    }
  });
});

describe("buildPiReasoningCapabilities", () => {
  it("drops unsupported levels and skips models Pi does not flag as reasoning", () => {
    const map = Object.fromEntries(
      ["off", "minimal", "low", "medium", "high", "xhigh", "max"].map((level) => [
        level,
        level === "medium" || level === "high" ? level : null,
      ]),
    );
    const caps = buildPiReasoningCapabilities(map, true);
    const descriptor = caps.optionDescriptors?.[0];
    expect(descriptor?.type).toBe("select");
    if (descriptor?.type === "select") {
      expect(descriptor.options.map((option) => option.id)).toEqual(["medium", "high"]);
      expect(descriptor.currentValue).toBe("medium");
    }
    expect(buildPiReasoningCapabilities(map, false).optionDescriptors).toEqual([]);
    expect(
      buildPiReasoningCapabilities(Object.fromEntries([["high", null]]), true).optionDescriptors,
    ).toEqual([]);
  });
});

describe("parsePiCommandsResponse", () => {
  it("separates Pi slash commands from installed skills", () => {
    const inventory = parsePiCommandsResponse(
      JSON.stringify({
        type: "response",
        command: "get_commands",
        success: true,
        data: {
          commands: [
            { name: "session-name", description: "Rename", source: "extension" },
            {
              name: "skill:brave-search",
              description: "Search the web",
              source: "skill",
              location: "user",
              path: "/home/user/.pi/agent/skills/brave-search/SKILL.md",
            },
          ],
        },
      }),
    );

    expect(inventory.slashCommands).toEqual([{ name: "session-name", description: "Rename" }]);
    expect(inventory.skills).toEqual([
      {
        name: "brave-search",
        description: "Search the web",
        shortDescription: "Search the web",
        path: "/home/user/.pi/agent/skills/brave-search/SKILL.md",
        scope: "user",
        enabled: true,
      },
    ]);
  });

  it("reads Pi 0.84.x sourceInfo metadata for skills", () => {
    const inventory = parsePiCommandsResponse(
      JSON.stringify({
        type: "response",
        command: "get_commands",
        success: true,
        data: {
          commands: [
            {
              name: "default-header",
              description: "Restore the built-in π header",
              source: "extension",
              sourceInfo: {
                path: "C:\\Users\\user\\.pi\\agent\\extensions\\banner.ts",
                scope: "user",
              },
            },
            {
              name: "skill:ship",
              description: "Ship changes",
              source: "skill",
              sourceInfo: {
                path: "D:\\repo\\.agents\\skills\\ship\\SKILL.md",
                scope: "project",
              },
            },
            { name: "skill:pathless", source: "skill" },
          ],
        },
      }),
    );

    expect(inventory.slashCommands).toEqual([
      { name: "default-header", description: "Restore the built-in π header" },
    ]);
    expect(inventory.skills).toEqual([
      {
        name: "ship",
        description: "Ship changes",
        shortDescription: "Ship changes",
        path: "D:\\repo\\.agents\\skills\\ship\\SKILL.md",
        scope: "project",
        enabled: true,
      },
    ]);
  });
});

describe("configured-provider filtering", () => {
  it("parses auth store provider keys and ignores malformed stores", () => {
    expect([...parsePiAuthProviderStore('{"anthropic":{},"openai-codex":{}}')]).toEqual([
      "anthropic",
      "openai-codex",
    ]);
    expect(parsePiAuthProviderStore("not json").size).toBe(0);
    expect(parsePiAuthProviderStore("[1,2]").size).toBe(0);
  });

  it("keeps only models from configured providers and skips filtering without a store", () => {
    const models = parsePiAvailableModelsResponse(
      rpcModelsLine([
        rpcModelRow({ provider: "anthropic", id: "claude-opus-4-5", reasoning: true }),
        rpcModelRow({ provider: "openai", id: "gpt-5", reasoning: false, thinkingLevels: [] }),
      ]) + "\n",
    );
    expect(filterModelsByConfiguredProviders(models, null)).toHaveLength(2);
    expect(filterModelsByConfiguredProviders(models, new Set(["openai"]))).toHaveLength(1);
    expect(filterModelsByConfiguredProviders(models, new Set(["openai"]))[0]?.slug).toBe(
      "openai/gpt-5",
    );
  });
});

it.layer(testLayer)("checkPiProviderStatus", (it) => {
  it.effect("reports ready/authenticated with the unique upstream provider count", () =>
    Effect.gen(function* () {
      const snapshot = yield* runCheck(
        makeProbeSpawner([
          { stdout: "pi 0.84.3\n" },
          {
            stdout: rpcInventoryStdout(
              [
                rpcModelRow({
                  provider: "anthropic",
                  id: "claude-sonnet-4-5",
                  reasoning: true,
                  thinkingLevels: ["off", "minimal", "low", "medium", "high"],
                }),
                rpcModelRow({ provider: "anthropic", id: "claude-opus-4-5", reasoning: true }),
                rpcModelRow({
                  provider: "openai",
                  id: "gpt-5",
                  reasoning: false,
                  thinkingLevels: [],
                }),
              ],
              [
                { name: "review", description: "Review changes", source: "extension" },
                {
                  name: "skill:ship",
                  description: "Ship changes",
                  source: "skill",
                  location: "user",
                  path: "/skills/ship/SKILL.md",
                },
              ],
            ),
          },
        ]),
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.auth).toEqual({
        status: "authenticated",
        type: "Pi",
        label: "2 available upstream providers",
      });
      expect(snapshot.message).toBe("2 available upstream providers found by Pi.");
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "anthropic/claude-sonnet-4-5",
        "anthropic/claude-opus-4-5",
        "openai/gpt-5",
      ]);
      expect(snapshot.slashCommands).toEqual([{ name: "review", description: "Review changes" }]);
      expect(snapshot.skills.map((skill) => skill.name)).toEqual(["ship"]);
    }),
  );

  it.effect("filters discovered models to providers configured in the auth store", () =>
    Effect.gen(function* () {
      const authDir = yield* makeIsolatedAuthDir;
      yield* writeAuthStore(authDir, { openai: { type: "api_key" } });
      const snapshot = yield* runCheck(
        makeProbeSpawner([
          { stdout: "pi 0.84.3\n" },
          {
            stdout: rpcInventoryStdout(
              [
                rpcModelRow({
                  provider: "anthropic",
                  id: "claude-sonnet-4-5",
                  reasoning: true,
                  thinkingLevels: ["off", "medium", "high"],
                }),
                rpcModelRow({
                  provider: "openai",
                  id: "gpt-5",
                  reasoning: false,
                  thinkingLevels: [],
                }),
              ],
              [],
            ),
          },
        ]),
        makePiSettings(),
        { PI_CODING_AGENT_DIR: authDir },
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.auth).toEqual({
        status: "authenticated",
        type: "Pi",
        label: "1 available upstream provider",
      });
      expect(snapshot.message).toBe("1 available upstream provider found by Pi.");
      expect(snapshot.models.map((model) => model.slug)).toEqual(["openai/gpt-5"]);
    }),
  );

  it.effect("reports warning/unauthenticated when the auth store filters out every model", () =>
    Effect.gen(function* () {
      const authDir = yield* makeIsolatedAuthDir;
      yield* writeAuthStore(authDir, { google: { type: "oauth" } });
      const snapshot = yield* runCheck(
        makeProbeSpawner([
          { stdout: "pi 0.84.3\n" },
          {
            stdout: rpcInventoryStdout(
              [rpcModelRow({ provider: "anthropic", id: "claude-sonnet-4-5", reasoning: true })],
              [],
            ),
          },
        ]),
        makePiSettings(),
        { PI_CODING_AGENT_DIR: authDir },
      );

      expect(snapshot.status).toBe("warning");
      expect(snapshot.auth).toEqual({ status: "unauthenticated" });
      expect(snapshot.message).toBe("Pi is installed, but no authenticated providers were found.");
    }),
  );

  it.effect("reports warning/unauthenticated when model discovery is empty", () =>
    Effect.gen(function* () {
      const snapshot = yield* runCheck(
        makeProbeSpawner([
          { stdout: "pi 0.84.3\n" },
          {
            stdout: rpcInventoryStdout(
              [],
              [
                {
                  name: "skill:offline",
                  source: "skill",
                  path: "/skills/offline/SKILL.md",
                },
              ],
            ),
          },
        ]),
        makePiSettings(["anthropic/claude-sonnet-4-5"]),
      );

      expect(snapshot.status).toBe("warning");
      expect(snapshot.auth).toEqual({ status: "unauthenticated" });
      expect(snapshot.message).toBe("Pi is installed, but no authenticated providers were found.");
      expect(snapshot.models.map((model) => model.slug)).toEqual(["anthropic/claude-sonnet-4-5"]);
      expect(snapshot.skills.map((skill) => skill.name)).toEqual(["offline"]);
    }),
  );

  it.effect("reports error/unknown when model discovery exits unsuccessfully", () =>
    Effect.gen(function* () {
      const snapshot = yield* runCheck(
        makeProbeSpawner([
          { stdout: "pi 0.84.3\n" },
          { code: 1, stderr: "unable to load Pi providers" },
        ]),
      );

      expect(snapshot.status).toBe("error");
      expect(snapshot.auth).toEqual({ status: "unknown" });
      expect(snapshot.message).toBe("Pi CLI model discovery failed.");
      expect(snapshot.models).toEqual([]);
    }),
  );
});

// Live clock: the discovery timeout is injected short, so no TestClock
// scheduling is involved in interrupting the blocked spawn.
it.live("reports error/unknown when Pi model discovery times out", () =>
  Effect.gen(function* () {
    const snapshot = yield* runCheck(
      makeProbeSpawner([{ stdout: "pi 0.84.3\n" }, { hangSpawn: true }]),
      makePiSettings(),
      undefined,
      { modelDiscoveryTimeoutMs: 25 },
    ).pipe(Effect.provide(testLayer));

    expect(snapshot.status).toBe("error");
    expect(snapshot.auth).toEqual({ status: "unknown" });
    expect(snapshot.message).toBe("Pi CLI model discovery timed out after 25ms.");
  }),
);
