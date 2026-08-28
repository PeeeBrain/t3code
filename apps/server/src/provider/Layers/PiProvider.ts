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
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

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
import { makePiRpcClient, type PiRuntimeError } from "./PiRpcClient.ts";
import { PI_DEFAULT_THINKING_LEVEL, piSupportedThinkingLevels } from "./PiRpcProtocol.ts";

const PI_PRESENTATION = {
  displayName: "Pi",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});
const piThinkingLevelLabel = (level: string): string =>
  level === "xhigh" ? "XHigh" : `${level.charAt(0).toUpperCase()}${level.slice(1)}`;

/**
 * Builds a selector whose default is "use Pi's configured level". Pi's
 * tristate map keeps omitted standard levels through `high`, disables null
 * entries, and requires explicit non-null entries for `xhigh` / `max`.
 */
export function buildPiReasoningCapabilities(
  thinkingLevelMap: Record<string, unknown> | undefined,
  reasoning: boolean,
): ModelCapabilities {
  if (!reasoning) return EMPTY_CAPABILITIES;
  const levels = piSupportedThinkingLevels(thinkingLevelMap);
  if (levels.length === 0) return EMPTY_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        currentValue: PI_DEFAULT_THINKING_LEVEL,
        options: [
          {
            id: PI_DEFAULT_THINKING_LEVEL,
            label: "Pi default",
            isDefault: true,
          },
          ...levels.map((level) => ({
            id: level,
            label: piThinkingLevelLabel(level),
          })),
        ],
      },
    ],
  });
}

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const MODEL_DISCOVERY_TIMEOUT_MS = 20_000;
/** Commands are optional inventory; do not discard a successful model reply while waiting. */
const COMMAND_DISCOVERY_TIMEOUT_MS = 2_000;
const AUTH_PROBE_TIMEOUT_MS = 4_000;
const UnknownRecord = Schema.Record(Schema.String, Schema.Unknown);
const isUnknownRecord = Schema.is(UnknownRecord);

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

/** Converts `get_commands` response data into T3 command and skill rows. */
export function parsePiCommandsData(data: Record<string, unknown>): PiCommandInventory {
  const slashCommands: ServerProviderSlashCommand[] = [];
  const skills: ServerProviderSkill[] = [];
  const seenCommands = new Set<string>();
  const seenSkills = new Set<string>();
  const commands = data["commands"];
  if (!Array.isArray(commands)) return { slashCommands, skills };

  for (const candidate of commands) {
    if (!isUnknownRecord(candidate) || typeof candidate["name"] !== "string") continue;
    const rawName = candidate["name"].trim();
    if (!rawName) continue;
    const description = readTrimmedText(candidate["description"]);
    if (candidate["source"] === "skill") {
      const name = rawName.startsWith("skill:") ? rawName.slice("skill:".length) : rawName;
      if (!name || seenSkills.has(name.toLowerCase())) continue;
      seenSkills.add(name.toLowerCase());
      const path = commandSourceText(candidate, "path");
      const scope = commandSourceText(candidate, "location");
      skills.push({
        name,
        enabled: true,
        ...(path ? { path } : {}),
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

/** Converts `get_available_models` response data into T3 model rows. */
export function parsePiAvailableModelsData(
  data: Record<string, unknown>,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models: Array<ServerProviderModel> = [];
  const candidates = data["models"];
  if (!Array.isArray(candidates)) return models;
  for (const candidate of candidates) {
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
        isUnknownRecord(candidate["thinkingLevelMap"]) ? candidate["thinkingLevelMap"] : undefined,
        candidate["reasoning"] === true,
      ),
    });
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

/** Keeps only models whose upstream provider Pi reports as ready. */
export function filterModelsByReadyProviders(
  models: ReadonlyArray<ServerProviderModel>,
  readyProviders: ReadonlySet<string>,
): ReadonlyArray<ServerProviderModel> {
  return models.filter((model) => {
    const separator = model.slug.indexOf("/");
    return separator > 0 && readyProviders.has(model.slug.slice(0, separator));
  });
}

const upstreamProviderIds = (models: ReadonlyArray<ServerProviderModel>): ReadonlyArray<string> => {
  const providers = new Set<string>();
  for (const model of models) {
    const separator = model.slug.indexOf("/");
    if (separator > 0) providers.add(model.slug.slice(0, separator));
  }
  return [...providers];
};

const parsePiAuthCheck = (stdout: string): "ready" | "not_ready" | "unknown" => {
  let value: unknown;
  try {
    value = JSON.parse(stdout.trim());
  } catch {
    return "unknown";
  }
  if (!isUnknownRecord(value)) return "unknown";
  return value["status"] === "ready"
    ? "ready"
    : value["status"] === "not_ready"
      ? "not_ready"
      : "unknown";
};

interface PiInventoryResult {
  readonly models: Result.Result<Record<string, unknown>, PiRuntimeError>;
  readonly commands: Result.Result<Record<string, unknown>, PiRuntimeError>;
}

/** One shared typed RPC client answers both inventory requests concurrently. */
const discoverPiInventoryViaRpc = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
  timeoutMs: number,
): Effect.Effect<
  PiInventoryResult,
  PiRuntimeError,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const client = yield* makePiRpcClient({
        settings: piSettings,
        environment,
        cwd,
        address: { kind: "no-session" },
        requestTimeoutMs: timeoutMs,
      });
      yield* client.start();
      const [models, commands] = yield* Effect.all(
        [
          client.request({ type: "get_available_models" }).pipe(Effect.result),
          client
            .request({ type: "get_commands" }, COMMAND_DISCOVERY_TIMEOUT_MS)
            .pipe(Effect.result),
        ],
        { concurrency: "unbounded" },
      );
      return { models, commands };
    }),
  );

const runPiAuthCheck = (piSettings: PiSettings, environment: NodeJS.ProcessEnv, provider: string) =>
  Effect.gen(function* () {
    const args = ["auth", "check", "--json", "--no-refresh", "--provider", provider];
    const spawnCommand = yield* resolveSpawnCommand(piSettings.binaryPath, args, {
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

const discoverPiProviderReadiness = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  providers: ReadonlyArray<string>,
) =>
  Effect.gen(function* () {
    const checks = yield* Effect.forEach(
      providers,
      (provider) =>
        runPiAuthCheck(piSettings, environment, provider).pipe(
          Effect.timeoutOption(AUTH_PROBE_TIMEOUT_MS),
          Effect.result,
          Effect.map((result) => ({ provider, result })),
        ),
      { concurrency: 4 },
    );
    const ready = new Set<string>();
    const unknown = new Set<string>();
    for (const check of checks) {
      if (Result.isFailure(check.result) || Option.isNone(check.result.success)) {
        unknown.add(check.provider);
        continue;
      }
      const output = check.result.success.value;
      const status = output.code === 0 ? parsePiAuthCheck(output.stdout) : "not_ready";
      if (status === "ready") ready.add(check.provider);
      if (status === "unknown") unknown.add(check.provider);
    }
    return { ready, unknown };
  });

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
 * Health probe for the Pi CLI. Inventory runs in the server workspace so Pi
 * loads the same project resources as live sessions. Provider readiness comes
 * from Pi's own auth runtime, including instance-injected environment tokens.
 */
export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd: string = process.cwd(),
  options: { readonly modelDiscoveryTimeoutMs?: number } = {},
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
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

  const inventoryResult = yield* discoverPiInventoryViaRpc(
    piSettings,
    environment,
    cwd,
    modelDiscoveryTimeoutMs,
  ).pipe(Effect.timeoutOption(modelDiscoveryTimeoutMs), Effect.result);

  let discoveredModels: ReadonlyArray<ServerProviderModel> = [];
  let inventory: PiCommandInventory = { slashCommands: [], skills: [] };
  let inventoryFailure: "failed" | "timed-out" | undefined;
  if (Result.isSuccess(inventoryResult) && Option.isSome(inventoryResult.success)) {
    const { models, commands } = inventoryResult.success.value;
    if (Result.isSuccess(models)) {
      discoveredModels = parsePiAvailableModelsData(models.success);
    } else {
      inventoryFailure = "failed";
      yield* Effect.logWarning("Pi RPC model discovery failed.", {
        error: models.failure.message,
      });
    }
    if (Result.isSuccess(commands)) {
      inventory = parsePiCommandsData(commands.success);
    } else {
      yield* Effect.logWarning(
        "Pi RPC command discovery failed; command menus will fall back to models only.",
        { error: commands.failure.message },
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

  const readiness = yield* discoverPiProviderReadiness(
    piSettings,
    environment,
    upstreamProviderIds(discoveredModels),
  );
  const usableProviders = new Set([...readiness.ready, ...readiness.unknown]);
  const availableModels = filterModelsByReadyProviders(discoveredModels, usableProviders);

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
  if (readiness.unknown.size > 0) {
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
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi models were found, but provider readiness could not be fully confirmed.",
      },
    });
  }

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
