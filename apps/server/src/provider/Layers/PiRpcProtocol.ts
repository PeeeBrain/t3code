/**
 * Pi RPC protocol types - the single typed authority for the `pi --mode rpc`
 * wire format used by both persistent sessions and no-session discovery.
 *
 * Shapes follow Pi's `rpc-types.ts` (see https://pi.dev/docs/latest/rpc).
 * Decoders are lenient: unknown event types and added fields decode to
 * `undefined`/pass through so newer Pi versions do not break the transport.
 *
 * @module PiRpcProtocol
 */
import * as Schema from "effect/Schema";

const UnknownRecord = Schema.Record(Schema.String, Schema.Unknown);
const isRecord = Schema.is(UnknownRecord);

export type PiThinkingLevel = "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";

/** Selection sentinel meaning "keep Pi's configured thinking level". */
export const PI_DEFAULT_THINKING_LEVEL = "default";

/** Pi's canonical thinking levels in presentation order. */
export const PI_THINKING_LEVELS: ReadonlyArray<PiThinkingLevel> = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
];

/** Levels that keep Pi's default provider mapping when omitted from a map. */
const STANDARD_THINKING_LEVELS: ReadonlySet<PiThinkingLevel> = new Set([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
]);

/** Levels that require an explicit non-null map entry to be supported. */
const EXTENDED_THINKING_LEVELS: ReadonlySet<PiThinkingLevel> = new Set(["xhigh", "max"]);

const PI_THINKING_LEVEL_SET: ReadonlySet<string> = new Set(PI_THINKING_LEVELS);

export const isPiThinkingLevel = (value: unknown): value is PiThinkingLevel =>
  typeof value === "string" && PI_THINKING_LEVEL_SET.has(value);

/**
 * Derives the supported thinking levels from a model's `thinkingLevelMap`.
 *
 * Pi's tristate semantics: an omitted standard level (through `high`) keeps
 * the provider's default mapping and stays supported; a string overrides the
 * mapping; `null` disables the level. `xhigh` and `max` are unsupported
 * unless explicitly mapped to a non-null value. Models flagged `reasoning`
 * without any map keep the portable Off–High set.
 */
export function piSupportedThinkingLevels(
  thinkingLevelMap: Record<string, unknown> | undefined,
): ReadonlyArray<PiThinkingLevel> {
  if (thinkingLevelMap === undefined) {
    return PI_THINKING_LEVELS.filter((level) => STANDARD_THINKING_LEVELS.has(level));
  }
  return PI_THINKING_LEVELS.filter((level) => {
    const mapped = thinkingLevelMap[level];
    return EXTENDED_THINKING_LEVELS.has(level)
      ? mapped !== undefined && mapped !== null
      : mapped !== null;
  });
}

export interface PiImageContent {
  readonly type: "image";
  readonly data: string;
  readonly mimeType: string;
}

export type PiStreamingBehavior = "steer" | "followUp";

export type PiQueueMode = "all" | "one-at-a-time";

export type PiCompactionReason = "manual" | "threshold" | "overflow";

export type PiNotifyType = "info" | "warning" | "error";

/** Every command type documented by Pi's RPC mode. */
export type PiRpcCommand =
  | {
      readonly type: "prompt";
      readonly message: string;
      readonly images?: ReadonlyArray<PiImageContent>;
      readonly streamingBehavior?: PiStreamingBehavior;
    }
  | {
      readonly type: "steer";
      readonly message: string;
      readonly images?: ReadonlyArray<PiImageContent>;
    }
  | {
      readonly type: "follow_up";
      readonly message: string;
      readonly images?: ReadonlyArray<PiImageContent>;
    }
  | { readonly type: "abort" }
  | { readonly type: "clear_queue" }
  | { readonly type: "new_session"; readonly parentSession?: string }
  | { readonly type: "get_state" }
  | { readonly type: "get_messages" }
  | { readonly type: "set_model"; readonly provider: string; readonly modelId: string }
  | { readonly type: "cycle_model" }
  | { readonly type: "get_available_models" }
  | { readonly type: "set_thinking_level"; readonly level: PiThinkingLevel }
  | { readonly type: "cycle_thinking_level" }
  | { readonly type: "get_available_thinking_levels" }
  | { readonly type: "set_steering_mode"; readonly mode: PiQueueMode }
  | { readonly type: "set_follow_up_mode"; readonly mode: PiQueueMode }
  | { readonly type: "compact"; readonly customInstructions?: string }
  | { readonly type: "set_auto_compaction"; readonly enabled: boolean }
  | { readonly type: "set_auto_retry"; readonly enabled: boolean }
  | { readonly type: "abort_retry" }
  | { readonly type: "bash"; readonly command: string }
  | { readonly type: "abort_bash" }
  | { readonly type: "get_session_stats" }
  | { readonly type: "export_html"; readonly outputPath?: string }
  | { readonly type: "switch_session"; readonly sessionPath: string }
  | { readonly type: "fork"; readonly entryId: string }
  | { readonly type: "clone" }
  | { readonly type: "get_fork_messages" }
  | { readonly type: "get_entries"; readonly since?: string }
  | { readonly type: "get_tree" }
  | { readonly type: "get_last_assistant_text" }
  | { readonly type: "set_session_name"; readonly name: string }
  | { readonly type: "get_commands" }
  | {
      readonly type: "extension_ui_response";
      readonly id: string;
      readonly value?: string;
      readonly confirmed?: boolean;
      readonly cancelled?: boolean;
    };

export interface PiRpcSuccessResponse {
  readonly kind: "success";
  readonly id: string | undefined;
  readonly command: string;
  readonly data: Record<string, unknown>;
}

export interface PiRpcFailureResponse {
  readonly kind: "failure";
  readonly id: string | undefined;
  readonly command: string;
  readonly error: string;
}

export type PiRpcResponse = PiRpcSuccessResponse | PiRpcFailureResponse;

export function decodePiRpcResponse(value: unknown): PiRpcResponse | undefined {
  if (!isRecord(value) || value["type"] !== "response") return undefined;
  const command = typeof value["command"] === "string" ? value["command"] : "command";
  const id = typeof value["id"] === "string" ? value["id"] : undefined;
  if (value["success"] === true) {
    return {
      kind: "success",
      id,
      command,
      data: isRecord(value["data"]) ? value["data"] : {},
    };
  }
  return {
    kind: "failure",
    id,
    command,
    error: typeof value["error"] === "string" ? value["error"] : "Pi command failed.",
  };
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export interface PiAgentStartEvent {
  readonly type: "agent_start";
}
export interface PiAgentEndEvent {
  readonly type: "agent_end";
  readonly willRetry: boolean;
}
export interface PiAgentSettledEvent {
  readonly type: "agent_settled";
}
export interface PiTurnStartEvent {
  readonly type: "turn_start";
}
export interface PiTurnEndEvent {
  readonly type: "turn_end";
}
export interface PiMessageStartEvent {
  readonly type: "message_start";
  readonly message: Record<string, unknown>;
}
export interface PiMessageUpdateEvent {
  readonly type: "message_update";
  readonly assistantMessageEvent: Record<string, unknown> | undefined;
  readonly usage: Record<string, unknown> | undefined;
}
export interface PiMessageEndEvent {
  readonly type: "message_end";
  readonly message: Record<string, unknown>;
}
export interface PiBashExecutionUpdateEvent {
  readonly type: "bash_execution_update";
  readonly id: string | undefined;
  readonly delta: string;
}
export interface PiToolExecutionEvent {
  readonly type: "tool_execution_start" | "tool_execution_update" | "tool_execution_end";
  readonly toolCallId: string;
  readonly toolName: string;
  readonly args: Record<string, unknown> | undefined;
  readonly partialResult: unknown;
  readonly result: unknown;
  readonly isError: boolean;
}
export interface PiQueueUpdateEvent {
  readonly type: "queue_update";
  readonly steering: ReadonlyArray<string>;
  readonly followUp: ReadonlyArray<string>;
}
export interface PiCompactionStartEvent {
  readonly type: "compaction_start";
  readonly reason: PiCompactionReason | string;
}
export interface PiCompactionEndEvent {
  readonly type: "compaction_end";
  readonly reason: PiCompactionReason | string;
  readonly result: Record<string, unknown> | undefined;
  readonly aborted: boolean;
  readonly willRetry: boolean;
  readonly errorMessage: string | undefined;
}
export interface PiAutoRetryStartEvent {
  readonly type: "auto_retry_start";
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly delayMs: number;
  readonly errorMessage: string | undefined;
}
export interface PiAutoRetryEndEvent {
  readonly type: "auto_retry_end";
  readonly success: boolean;
  readonly attempt: number;
  readonly finalError: string | undefined;
}
export interface PiSummarizationRetryEvent {
  readonly type:
    | "summarization_retry_scheduled"
    | "summarization_retry_attempt_start"
    | "summarization_retry_finished";
  readonly source: string | undefined;
  readonly reason: string | undefined;
  readonly attempt: number | undefined;
  readonly errorMessage: string | undefined;
}
export interface PiExtensionErrorEvent {
  readonly type: "extension_error";
  readonly extensionPath: string | undefined;
  readonly event: string | undefined;
  readonly error: string;
}

export type PiExtensionUiMethod =
  | "select"
  | "confirm"
  | "input"
  | "editor"
  | "notify"
  | "setStatus"
  | "setWidget"
  | "setTitle"
  | "set_editor_text";

/** Dialog methods block Pi until a correlated `extension_ui_response` arrives. */
export const PI_EXTENSION_DIALOG_METHODS: ReadonlySet<string> = new Set([
  "select",
  "confirm",
  "input",
  "editor",
]);

/** Fire-and-forget methods publish information and expect no response. */
export const PI_EXTENSION_NOTIFY_METHODS: ReadonlySet<string> = new Set([
  "notify",
  "setStatus",
  "setWidget",
  "setTitle",
  "set_editor_text",
]);

export interface PiExtensionUiRequest {
  readonly type: "extension_ui_request";
  readonly id: string;
  readonly method: PiExtensionUiMethod | string;
  readonly title: string | undefined;
  readonly message: string | undefined;
  readonly placeholder: string | undefined;
  readonly prefill: string | undefined;
  readonly options: ReadonlyArray<string>;
  readonly timeout: number | undefined;
  readonly notifyType: PiNotifyType | string | undefined;
  readonly raw: Record<string, unknown>;
}

export type PiEvent =
  | PiAgentStartEvent
  | PiAgentEndEvent
  | PiAgentSettledEvent
  | PiTurnStartEvent
  | PiTurnEndEvent
  | PiMessageStartEvent
  | PiMessageUpdateEvent
  | PiMessageEndEvent
  | PiBashExecutionUpdateEvent
  | PiToolExecutionEvent
  | PiQueueUpdateEvent
  | PiCompactionStartEvent
  | PiCompactionEndEvent
  | PiAutoRetryStartEvent
  | PiAutoRetryEndEvent
  | PiSummarizationRetryEvent
  | PiExtensionErrorEvent
  | PiExtensionUiRequest;

const trimmed = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value : undefined;

const stringArray = (value: unknown): ReadonlyArray<string> =>
  Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];

/**
 * Decodes one Pi stdout line into a typed event. Response lines and unknown
 * event types return `undefined`; the transport correlates responses itself
 * and unknown events are deliberately ignored.
 */
export function decodePiEvent(value: unknown): PiEvent | undefined {
  if (!isRecord(value)) return undefined;
  const eventType = value["type"];
  if (typeof eventType !== "string") return undefined;
  switch (eventType) {
    case "agent_start":
      return { type: "agent_start" };
    case "agent_end":
      return { type: "agent_end", willRetry: value["willRetry"] === true };
    case "agent_settled":
      return { type: "agent_settled" };
    case "turn_start":
      return { type: "turn_start" };
    case "turn_end":
      return { type: "turn_end" };
    case "message_start":
    case "message_end":
      return isRecord(value["message"])
        ? { type: eventType, message: value["message"] }
        : undefined;
    case "message_update":
      return {
        type: "message_update",
        assistantMessageEvent: isRecord(value["assistantMessageEvent"])
          ? value["assistantMessageEvent"]
          : undefined,
        usage: isRecord(value["usage"]) ? value["usage"] : undefined,
      };
    case "bash_execution_update":
      return {
        type: "bash_execution_update",
        id: typeof value["id"] === "string" ? value["id"] : undefined,
        delta: typeof value["delta"] === "string" ? value["delta"] : "",
      };
    case "tool_execution_start":
    case "tool_execution_update":
    case "tool_execution_end": {
      const toolCallId = value["toolCallId"];
      const toolName = value["toolName"];
      if (typeof toolCallId !== "string" || typeof toolName !== "string") return undefined;
      return {
        type: eventType,
        toolCallId,
        toolName,
        args: isRecord(value["args"]) ? value["args"] : undefined,
        partialResult: value["partialResult"],
        result: value["result"],
        isError: value["isError"] === true,
      };
    }
    case "queue_update":
      return {
        type: "queue_update",
        steering: stringArray(value["steering"]),
        followUp: stringArray(value["followUp"]),
      };
    case "compaction_start":
      return {
        type: "compaction_start",
        reason: typeof value["reason"] === "string" ? value["reason"] : "manual",
      };
    case "compaction_end":
      return {
        type: "compaction_end",
        reason: typeof value["reason"] === "string" ? value["reason"] : "manual",
        result: isRecord(value["result"]) ? value["result"] : undefined,
        aborted: value["aborted"] === true,
        willRetry: value["willRetry"] === true,
        errorMessage: trimmed(value["errorMessage"]),
      };
    case "auto_retry_start":
      return {
        type: "auto_retry_start",
        attempt: typeof value["attempt"] === "number" ? value["attempt"] : 0,
        maxAttempts: typeof value["maxAttempts"] === "number" ? value["maxAttempts"] : 0,
        delayMs: typeof value["delayMs"] === "number" ? value["delayMs"] : 0,
        errorMessage: trimmed(value["errorMessage"]),
      };
    case "auto_retry_end":
      return {
        type: "auto_retry_end",
        success: value["success"] === true,
        attempt: typeof value["attempt"] === "number" ? value["attempt"] : 0,
        finalError: trimmed(value["finalError"]),
      };
    case "summarization_retry_scheduled":
    case "summarization_retry_attempt_start":
    case "summarization_retry_finished":
      return {
        type: eventType,
        source: trimmed(value["source"]),
        reason: trimmed(value["reason"]),
        attempt: typeof value["attempt"] === "number" ? value["attempt"] : undefined,
        errorMessage: trimmed(value["errorMessage"]),
      };
    case "extension_error":
      return {
        type: "extension_error",
        extensionPath: trimmed(value["extensionPath"]),
        event: trimmed(value["event"]),
        error:
          trimmed(value["error"]) ?? "A Pi extension failed without reporting an error message.",
      };
    case "extension_ui_request": {
      const id = value["id"];
      const method = value["method"];
      if (typeof id !== "string" || typeof method !== "string") return undefined;
      return {
        type: "extension_ui_request",
        id,
        method,
        title: trimmed(value["title"]),
        message: trimmed(value["message"]),
        placeholder: trimmed(value["placeholder"]),
        prefill: trimmed(value["prefill"]),
        options: stringArray(value["options"]),
        timeout: typeof value["timeout"] === "number" ? value["timeout"] : undefined,
        notifyType: trimmed(value["notifyType"]),
        raw: value,
      };
    }
    default:
      return undefined;
  }
}

// ---------------------------------------------------------------------------
// Response payload accessors
// ---------------------------------------------------------------------------

const nonNegativeInt = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.trunc(value) : undefined;

/** Token usage carried by `message_update` events and session stats. */
export function piUsage(value: Record<string, unknown> | undefined) {
  if (value === undefined) return undefined;
  const input = nonNegativeInt(value["input"]);
  const output = nonNegativeInt(value["output"]);
  const totalTokens = nonNegativeInt(value["totalTokens"]);
  const cachedInputTokens = nonNegativeInt(value["cacheRead"]);
  const reasoning = nonNegativeInt(value["reasoning"]);
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

export function piSessionIdFromState(data: Record<string, unknown>): string | undefined {
  return trimmed(data["sessionId"]);
}

export function piModelFromState(data: Record<string, unknown>): string | undefined {
  const model = data["model"];
  if (!isRecord(model)) return undefined;
  const provider = model["provider"];
  const id = model["id"];
  if (typeof id !== "string" || !id.trim()) return undefined;
  return typeof provider === "string" && provider.trim()
    ? `${provider.trim()}/${id.trim()}`
    : id.trim();
}

export function piThinkingLevelFromState(
  data: Record<string, unknown>,
): PiThinkingLevel | undefined {
  const level = data["thinkingLevel"];
  return isPiThinkingLevel(level) ? level : undefined;
}

/** Decodes `get_available_thinking_levels` response data. */
export function piThinkingLevelsFromData(
  data: Record<string, unknown>,
): ReadonlyArray<PiThinkingLevel> {
  const levels = data["levels"];
  if (!Array.isArray(levels)) return [];
  return levels.filter(isPiThinkingLevel);
}

/** Decodes `get_fork_messages` response data into ordered `{entryId, text}` pairs. */
export function piForkMessagesFromData(
  data: Record<string, unknown>,
): ReadonlyArray<{ readonly entryId: string; readonly text: string }> {
  const messages = data["messages"];
  if (!Array.isArray(messages)) return [];
  const forkMessages: Array<{ entryId: string; text: string }> = [];
  for (const candidate of messages) {
    if (!isRecord(candidate)) continue;
    const entryId = candidate["entryId"];
    const text = candidate["text"];
    if (typeof entryId !== "string" || !entryId.trim()) continue;
    forkMessages.push({ entryId, text: typeof text === "string" ? text : "" });
  }
  return forkMessages;
}
