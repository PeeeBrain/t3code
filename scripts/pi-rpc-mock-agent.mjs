// Mock Pi CLI speaking the `pi --mode rpc` NDJSON protocol for adapter tests.
// Usage: node pi-rpc-mock-agent.mjs [ignored args]
// Set PI_MOCK_MODE=hold to defer all prompt events until an `abort` arrives.
import * as NodeReadline from "node:readline";
import * as NodeProcess from "node:process";

const holdMode = NodeProcess.env["PI_MOCK_MODE"] === "hold";
const exitMode = NodeProcess.env["PI_MOCK_MODE"] === "exit";
const ignorePromptMode = NodeProcess.env["PI_MOCK_MODE"] === "ignore-prompt";
const retryMode = NodeProcess.env["PI_MOCK_MODE"] === "retry";
const expectedThinkingLevel = NodeProcess.env["PI_EXPECT_THINKING"];
let currentThinkingLevel = "medium";
const out = (value) => NodeProcess.stdout.write(`${JSON.stringify(value)}\n`);

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
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Hello " },
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
        sessionId: "mock-pi-session",
      });
      return;
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
      if (exitMode) {
        setImmediate(() => NodeProcess.exit(17));
        return;
      }
      if (holdMode) return; // events wait for abort
      if (retryMode) {
        emitRetryFlow();
        return;
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
    default:
      respond(id, String(parsed.type ?? "unknown"), true);
  }
});
