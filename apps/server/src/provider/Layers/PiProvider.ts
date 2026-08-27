import {
  type PiSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import * as NodeOS from "node:os";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";

const PI_PRESENTATION = {
  displayName: "Pi",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});
/** Pi's canonical thinking levels, in the order Pi presents them. */
const PI_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;
const piThinkingLevelLabel = (level: string): string =>
  level === "xhigh" ? "XHigh" : `${level.charAt(0).toUpperCase()}${level.slice(1)}`;

/**
 * Builds the reasoning selector from the model's `thinkingLevelMap`: Pi maps
 * each canonical level to the provider parameter value it would send, and a
 * `null` value means the model does not support that level. Models flagged
 * `reasoning` without a usable map keep the historical Off–High set.
 */
export function buildPiReasoningCapabilities(
  thinkingLevelMap: Record<string, unknown> | undefined,
  reasoning: boolean,
): ModelCapabilities {
  if (!reasoning) return EMPTY_CAPABILITIES;
  const levels =
    thinkingLevelMap === undefined
      ? [...PI_THINKING_LEVELS.slice(0, 5)]
      : PI_THINKING_LEVELS.filter((level) => thinkingLevelMap[level] != null);
  if (levels.length === 0) return EMPTY_CAPABILITIES;
  const defaultValue = levels.includes("medium") ? "medium" : levels[levels.length - 1];
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        currentValue: defaultValue,
        options: levels.map((level) => ({
          id: level,
          label: piThinkingLevelLabel(level),
          ...(level === defaultValue ? { isDefault: true } : {}),
        })),
      },
    ],
  });
}

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const MODEL_DISCOVERY_TIMEOUT_MS = 20_000;
/** After the first inventory RPC reply, wait this long for the sibling. */
const PI_RPC_INVENTORY_IDLE_MS = 2_000;
const UnknownRecord = Schema.Record(Schema.String, Schema.Unknown);
const isUnknownRecord = Schema.is(UnknownRecord);
const encodeUnknownJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const readTrimmedText = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value.trim() : undefined;

/**
 * Pi 0.84.x nests command metadata under `sourceInfo`; the documented RPC
 * shape carries top-level `path` / `location`. Prefer the documented shape and
 * fall back to `sourceInfo` so both Pi versions parse.
 */
const commandSourceText = (
  candidate: Record<string, unknown>,
  key: "path" | "location",
): string | undefined => {
  const sourceInfo = isUnknownRecord(candidate["sourceInfo"]) ? candidate["sourceInfo"] : undefined;
  const nestedKey = key === "location" ? "scope" : key;
  return readTrimmedText(candidate[key]) ?? readTrimmedText(sourceInfo?.[nestedKey]);
};

export interface PiCommandInventory {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

/** Converts the documented `get_commands` response into T3 command and skill rows. */
export function parsePiCommandsResponse(stdout: string): PiCommandInventory {
  const slashCommands: ServerProviderSlashCommand[] = [];
  const skills: ServerProviderSkill[] = [];
  const seenCommands = new Set<string>();
  const seenSkills = new Set<string>();

  for (const line of stdout.split("\n")) {
    if (!line.trim()) continue;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      continue;
    }
    if (!isUnknownRecord(value) || value["type"] !== "response" || value["success"] !== true) {
      continue;
    }
    const data = value["data"];
    if (!isUnknownRecord(data) || !Array.isArray(data["commands"])) continue;
    for (const candidate of data["commands"]) {
      if (!isUnknownRecord(candidate) || typeof candidate["name"] !== "string") continue;
      const rawName = candidate["name"].trim();
      if (!rawName) continue;
      const description =
        typeof candidate["description"] === "string" && candidate["description"].trim()
          ? candidate["description"].trim()
          : undefined;
      if (candidate["source"] === "skill") {
        const name = rawName.startsWith("skill:") ? rawName.slice("skill:".length) : rawName;
        const path = commandSourceText(candidate, "path");
        if (!name || !path || seenSkills.has(name.toLowerCase())) continue;
        seenSkills.add(name.toLowerCase());
        const scope = commandSourceText(candidate, "location");
        skills.push({
          name,
          path,
          enabled: true,
          ...(description ? { description, shortDescription: description } : {}),
          ...(scope ? { scope } : {}),
        });
        continue;
      }
      const key = rawName.toLowerCase();
      if (seenCommands.has(key)) continue;
      seenCommands.add(key);
      slashCommands.push({ name: rawName, ...(description ? { description } : {}) });
    }
  }
  return { slashCommands, skills };
}

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(piSettings.customModels);

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Pi CLI availability...",
      },
    });
  });
}

function piModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  discoveredModels: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(discoveredModels, customModels ?? [], EMPTY_CAPABILITIES);
}

/**
 * Parses the `get_available_models` RPC response. Model slugs follow the
 * `provider/id` shape the rest of T3 Code uses for multi-provider catalogs,
 * and reasoning options come from each model's `thinkingLevelMap`.
 */
export function parsePiAvailableModelsResponse(stdout: string): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models: Array<ServerProviderModel> = [];
  for (const line of stdout.split("\n")) {
    if (!line.trim()) continue;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      continue;
    }
    if (!isUnknownRecord(value) || value["type"] !== "response" || value["success"] !== true) {
      continue;
    }
    const data = value["data"];
    if (!isUnknownRecord(data) || !Array.isArray(data["models"])) continue;
    for (const candidate of data["models"]) {
      if (!isUnknownRecord(candidate)) continue;
      const provider = readTrimmedText(candidate["provider"]);
      const modelId = readTrimmedText(candidate["id"]);
      if (!provider || !modelId) continue;
      const slug = `${provider}/${modelId}`;
      if (seen.has(slug)) continue;
      seen.add(slug);
      models.push({
        slug,
        name: readTrimmedText(candidate["name"]) ?? slug,
        isCustom: false,
        capabilities: buildPiReasoningCapabilities(
          isUnknownRecord(candidate["thinkingLevelMap"])
            ? candidate["thinkingLevelMap"]
            : undefined,
          candidate["reasoning"] === true,
        ),
      });
    }
  }
  return models;
}

/** Counts the distinct upstream providers represented by Pi's available model list. */
export function countPiUpstreamProviders(models: ReadonlyArray<ServerProviderModel>): number {
  const providers = new Set<string>();
  for (const model of models) {
    const separator = model.slug.indexOf("/");
    if (separator > 0) {
      providers.add(model.slug.slice(0, separator));
    }
  }
  return providers.size;
}

/**
 * `pi --list-models` (and RPC `get_available_models`) list Pi's whole model
 * catalog, including providers the user never authenticated. Pi considers a
 * provider "ready" when it has stored credentials or a matching ambient env
 * token, so env vars like ANTHROPIC_AUTH_TOKEN leak unrelated models into the
 * picker. The auth store (`<agentDir>/auth.json`) holds exactly the providers
 * the user configured with Pi, so it is the truth for what belongs in the
 * picker. `null` means the store was absent or unreadable and filtering is
 * skipped. An empty set is a readable store with no configured providers.
 */
export function parsePiAuthProviderStore(text: string): ReadonlySet<string> | null {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return null;
  }
  if (!isUnknownRecord(value)) return null;
  const providers = new Set<string>();
  for (const key of Object.keys(value)) {
    if (key.trim()) providers.add(key.trim());
  }
  return providers;
}

export function filterModelsByConfiguredProviders(
  models: ReadonlyArray<ServerProviderModel>,
  providers: ReadonlySet<string> | null,
): ReadonlyArray<ServerProviderModel> {
  if (providers === null) return models;
  return models.filter((model) => {
    const separator = model.slug.indexOf("/");
    return separator > 0 && providers.has(model.slug.slice(0, separator));
  });
}

/**
 * Pi resolves its agent directory from `PI_CODING_AGENT_DIR`, defaulting to
 * `~/.pi/agent` (USERPROFILE on Windows).
 */
const resolvePiAgentDir = (environment: NodeJS.ProcessEnv): string => {
  const override = readTrimmedText(environment["PI_CODING_AGENT_DIR"]);
  if (override) return override;
  const home =
    readTrimmedText(
      process.platform === "win32" ? environment["USERPROFILE"] : environment["HOME"],
    ) ?? NodeOS.homedir();
  return `${home.replace(/[\\/]+$/, "")}/.pi/agent`;
};

const readPiConfiguredProviders = (environment: NodeJS.ProcessEnv = process.env) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const storeText = yield* fileSystem
      .readFile(path.join(resolvePiAgentDir(environment), "auth.json"))
      .pipe(
        Effect.map((bytes) => new TextDecoder().decode(bytes)),
        Effect.orElseSucceed(() => null),
      );
    return storeText === null ? null : parsePiAuthProviderStore(storeText);
  });

const PI_RPC_MODELS_ID = "t3-models";
const PI_RPC_COMMANDS_ID = "t3-inventory";

type PiRpcResult = { stdout: string; stderr: string; code: number };

/**
 * One bounded Pi RPC process answers both inventory requests: the available
 * model objects and the extension/command/skill list. Pi takes a moment to
 * load extensions before replying, so callers should apply a generous timeout.
 */
const discoverPiInventoryViaRpc = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const args = ["--mode", "rpc", "--no-session"];
    const spawnCommand = yield* resolveSpawnCommand(piSettings.binaryPath, args, {
      env: environment,
    });
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: process.cwd(),
        env: environment,
        shell: spawnCommand.shell,
        stdin: { stream: "pipe", endOnDone: false },
        stdout: "pipe",
        stderr: "pipe",
      }),
    );
    yield* Stream.runDrain(child.stderr).pipe(Effect.ignore, Effect.forkScoped);
    yield* Stream.run(
      Stream.encodeText(
        Stream.make(
          `{"id":"${PI_RPC_MODELS_ID}","type":"get_available_models"}\n`,
          `{"id":"${PI_RPC_COMMANDS_ID}","type":"get_commands"}\n`,
        ),
      ),
      child.stdin,
    ).pipe(Effect.ignore, Effect.forkScoped);
    const collected = yield* Ref.make<Array<unknown>>([]);
    const settled = yield* Deferred.make<void>();
    const settle = Deferred.succeed(settled, undefined).pipe(Effect.asVoid, Effect.ignore);
    const isInventoryResponse = (value: unknown): boolean =>
      isUnknownRecord(value) &&
      value["type"] === "response" &&
      (value["id"] === PI_RPC_MODELS_ID || value["id"] === PI_RPC_COMMANDS_ID);
    yield* child.stdout.pipe(
      Stream.pipeThroughChannel(Ndjson.decode({ ignoreEmptyLines: true })),
      Stream.runForEach((value) =>
        Effect.gen(function* () {
          if (!isInventoryResponse(value)) return;
          const next = yield* Ref.updateAndGet(collected, (acc) => [...acc, value]);
          if (next.length === 1) {
            yield* Effect.sleep(`${PI_RPC_INVENTORY_IDLE_MS} millis`).pipe(
              Effect.andThen(settle),
              Effect.forkScoped,
            );
          }
          if (next.length >= 2) yield* settle;
        }),
      ),
      Effect.andThen(settle),
      Effect.forkScoped,
    );
    yield* Deferred.await(settled);
    const responses = yield* Ref.get(collected);
    yield* child.kill().pipe(Effect.ignore);
    const pick = (id: string): PiRpcResult => {
      for (const value of responses) {
        if (isUnknownRecord(value) && value["id"] === id) {
          return { stdout: encodeUnknownJson(value), stderr: "", code: 0 };
        }
      }
      return { stdout: "", stderr: "Pi returned no response for this request.", code: 1 };
    };
    return { models: pick(PI_RPC_MODELS_ID), commands: pick(PI_RPC_COMMANDS_ID) };
  }).pipe(Effect.scoped);

const runPiVersionCommand = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(piSettings.binaryPath, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      piSettings.binaryPath,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

/**
 * Health probe for the Pi CLI. `pi --list-models` lists Pi's whole model
 * catalog, so it is intersected with the providers configured in Pi's auth
 * store to expose only models the user can actually run.
 */
export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  options: { readonly modelDiscoveryTimeoutMs?: number } = {},
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const modelDiscoveryTimeoutMs = options.modelDiscoveryTimeoutMs ?? MODEL_DISCOVERY_TIMEOUT_MS;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = piModelsFromSettings(piSettings.customModels);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runPiVersionCommand(piSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Pi CLI health check failed.", {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Pi CLI (`pi`) is not installed or not on PATH."
          : "Failed to execute Pi CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but timed out while running `pi --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Pi CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but failed to run.",
      },
    });
  }

  const inventoryResult = yield* discoverPiInventoryViaRpc(piSettings, environment).pipe(
    Effect.timeoutOption(modelDiscoveryTimeoutMs),
    Effect.result,
  );

  let discoveredModels: ReadonlyArray<ServerProviderModel> = [];
  let inventory: PiCommandInventory = { slashCommands: [], skills: [] };
  let inventoryFailure: "failed" | "timed-out" | undefined;
  if (Result.isSuccess(inventoryResult) && Option.isSome(inventoryResult.success)) {
    const { models, commands } = inventoryResult.success.value;
    if (models.code === 0) {
      discoveredModels = parsePiAvailableModelsResponse(models.stdout);
    } else {
      inventoryFailure = "failed";
      yield* Effect.logWarning("Pi RPC model discovery failed.", { stderr: models.stderr });
    }
    if (commands.code === 0) {
      inventory = parsePiCommandsResponse(commands.stdout);
    } else {
      yield* Effect.logWarning(
        "Pi RPC command discovery failed; command menus will fall back to models only.",
        { stderr: commands.stderr },
      );
    }
  } else if (Result.isFailure(inventoryResult)) {
    inventoryFailure = "failed";
    yield* Effect.logWarning("Pi RPC inventory discovery failed.", {
      errorTag: inventoryResult.failure._tag,
    });
  } else {
    inventoryFailure = "timed-out";
    yield* Effect.logWarning(
      `Pi RPC inventory discovery timed out after ${modelDiscoveryTimeoutMs}ms.`,
    );
  }

  if (inventoryFailure !== undefined) {
    const message =
      inventoryFailure === "timed-out"
        ? `Pi CLI model discovery timed out after ${modelDiscoveryTimeoutMs}ms.`
        : "Pi CLI model discovery failed.";
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      slashCommands: inventory.slashCommands,
      skills: inventory.skills,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message,
      },
    });
  }

  const configuredProviders = yield* readPiConfiguredProviders(environment);
  const availableModels = filterModelsByConfiguredProviders(discoveredModels, configuredProviders);

  if (availableModels.length === 0) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      slashCommands: inventory.slashCommands,
      skills: inventory.skills,
      probe: {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unauthenticated" },
        message: "Pi is installed, but no authenticated providers were found.",
      },
    });
  }

  const models = piModelsFromSettings(piSettings.customModels, availableModels);
  const upstreamProviderCount = countPiUpstreamProviders(availableModels);
  const providerLabel = `${upstreamProviderCount} available upstream provider${
    upstreamProviderCount === 1 ? "" : "s"
  }`;
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models,
    slashCommands: inventory.slashCommands,
    skills: inventory.skills,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: {
        status: "authenticated",
        type: "Pi",
        label: providerLabel,
      },
      message: `${providerLabel} found by Pi.`,
    },
  });
});

export const enrichPiSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Pi version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
