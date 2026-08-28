// Mock Pi CLI speaking the `pi --mode rpc` NDJSON protocol for adapter tests.
// Usage: node pi-rpc-mock-agent.mjs [ignored args]
// Set PI_MOCK_MODE=hold to defer all prompt events until an `abort` arrives.
import * as NodeReadline from "node:readline";
import * as NodeProcess from "node:process";

const holdMode = NodeProcess.env["PI_MOCK_MODE"] === "hold";
const exitMode = NodeProcess.env["PI_MOCK_MODE"] === "exit";
const ignorePromptMode = NodeProcess.env["PI_MOCK_MODE"] === "ignore-prompt";
const retryMode = NodeProcess.env["PI_MOCK_MODE"] === "retry";
const dialogMode = NodeProcess.env["PI_MOCK_MODE"] === "dialog";
const compactionFailureMode = NodeProcess.env["PI_MOCK_MODE"] === "compaction-failure";
const extensionErrorMode = NodeProcess.env["PI_MOCK_MODE"] === "extension-error";
const expectedThinkingLevel = NodeProcess.env["PI_EXPECT_THINKING"];
const availableThinkingLevels = (
  NodeProcess.env["PI_THINKING_LEVELS"] ?? "off,minimal,low,medium,high,xhigh,max"
).split(",");
const lineEnding = NodeProcess.env["PI_OUTPUT_CRLF"] === "1" ? "\r\n" : "\n";
const unicodeDelta = NodeProcess.env["PI_UNICODE_SEPARATORS"] === "1" ? "\u2028middle\u2029" : " ";
let currentThinkingLevel = "medium";
let currentSessionId = "mock-pi-session";
const userMessages = [];
const out = (value) => NodeProcess.stdout.write(`${JSON.stringify(value)}${lineEnding}`);

const respond = (id, command, success, data) =>
  out({
    type: "response",
    command,
    success,
    ...(success ? { data: data ?? {} } : { error: "mock failure" }),
    ...(id !== undefined ? { id } : {}),
  });

function emitPromptFlow() {
  out({ type: "agent_start" });
  out({ type: "message_start", message: { role: "assistant", content: [] } });
  out({
    type: "message_update",
    usage: { input: 10, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 11 },
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: `Hello${unicodeDelta}` },
  });
  out({
    type: "message_update",
    usage: { input: 10, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 12 },
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "world" },
  });
  out({
    type: "tool_execution_start",
    toolCallId: "call_mock_1",
    toolName: "bash",
    args: { command: "echo hi" },
  });
  out({
    type: "tool_execution_end",
    toolCallId: "call_mock_1",
    toolName: "bash",
    result: { content: [{ type: "text", text: "hi" }] },
    isError: false,
  });
  out({
    type: "message_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "Hello world" }],
    },
  });
  out({ type: "message_start", message: { role: "assistant", content: [] } });
  out({
    type: "message_end",
    message: { role: "assistant", content: [{ type: "text", text: "Done" }] },
  });
  out({ type: "agent_settled" });
}

function emitRetryFlow() {
  out({ type: "agent_start" });
  out({ type: "message_start", message: { role: "assistant", content: [] } });
  out({
    type: "message_end",
    message: { role: "assistant", content: [], stopReason: "error", errorMessage: "overloaded" },
  });
  out({ type: "auto_retry_start", attempt: 1, maxAttempts: 3, errorMessage: "overloaded" });
  out({ type: "auto_retry_end", success: true, attempt: 2 });
  out({ type: "message_start", message: { role: "assistant", content: [] } });
  out({
    type: "message_end",
    message: { role: "assistant", content: [{ type: "text", text: "Recovered" }] },
  });
  out({ type: "agent_settled" });
}

const rl = NodeReadline.createInterface({ input: NodeProcess.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch {
    return;
  }
  const id = typeof parsed.id === "string" ? parsed.id : undefined;
  switch (parsed.type) {
    case "get_state":
      respond(id, "get_state", true, {
        model: { provider: "mock", id: "mock-model" },
        thinkingLevel: currentThinkingLevel,
        isStreaming: false,
        isCompacting: false,
        sessionId: currentSessionId,
      });
      return;
    case "get_available_thinking_levels":
      respond(id, "get_available_thinking_levels", true, { levels: availableThinkingLevels });
      return;
    case "get_fork_messages":
      respond(id, "get_fork_messages", true, {
        messages: userMessages.map((text, index) => ({ entryId: `user-${index}`, text })),
      });
      return;
    case "get_entries":
      respond(id, "get_entries", true, {
        entries: userMessages.map((text, index) => ({
          id: `user-${index}`,
          type: "user_message",
          text,
        })),
      });
      return;
    case "get_tree":
      respond(id, "get_tree", true, {
        tree: { root: "session-root", entries: userMessages.length },
      });
      return;
    case "get_last_assistant_text":
      respond(id, "get_last_assistant_text", true, { text: "Hello world" });
      return;
    case "fork": {
      const targetIndex = userMessages.findIndex((_, index) => `user-${index}` === parsed.entryId);
      if (targetIndex < 0) {
        respond(id, "fork", false);
        return;
      }
      userMessages.splice(targetIndex);
      currentSessionId = "mock-forked-session";
      respond(id, "fork", true, { text: "forked", cancelled: false });
      return;
    }
    case "prompt":
      if (ignorePromptMode) return;
      if (expectedThinkingLevel && currentThinkingLevel !== expectedThinkingLevel) {
        respond(id, "prompt", false);
        return;
      }
      if (parsed.streamingBehavior !== undefined) {
        // Steered messages produce no new lifecycle of their own.
        if (NodeProcess.env["PI_FAIL_STEER"] === "1") {
          respond(id, "prompt", false);
          return;
        }
        respond(id, "prompt", true);
        return;
      }
      respond(id, "prompt", true);
      userMessages.push(String(parsed.message ?? ""));
      if (dialogMode) {
        out({
          type: "extension_ui_request",
          id: "mock-dialog",
          method: "select",
          title: "Choose",
          options: ["Allow", "Block"],
        });
        return;
      }
      if (exitMode) {
        setImmediate(() => NodeProcess.exit(17));
        return;
      }
      if (holdMode) return; // events wait for abort
      if (retryMode) {
        emitRetryFlow();
        return;
      }
      if (compactionFailureMode) {
        out({ type: "agent_start" });
        out({ type: "compaction_start", reason: "threshold" });
        out({
          type: "compaction_end",
          reason: "threshold",
          result: null,
          aborted: false,
          willRetry: false,
          errorMessage: "summary failed",
        });
        out({ type: "agent_settled" });
        return;
      }
      if (extensionErrorMode) {
        out({
          type: "extension_error",
          extensionPath: "/mock/extension.ts",
          event: "tool_call",
          error: "extension exploded",
        });
      }
      emitPromptFlow();
      return;
    case "abort":
      respond(id, "abort", true);
      if (holdMode) emitPromptFlow();
      return;
    case "set_model":
      respond(id, "set_model", true, { provider: parsed.provider, id: parsed.modelId });
      return;
    case "set_thinking_level":
      currentThinkingLevel = String(parsed.level ?? "");
      respond(id, "set_thinking_level", true);
      return;
    case "extension_ui_response":
      if (dialogMode && parsed.id === "mock-dialog") {
        if (parsed.value === "Allow" || parsed.confirmed === true) {
          emitPromptFlow();
        }
      }
      return;
    default:
      respond(id, String(parsed.type ?? "unknown"), true);
  }
});
