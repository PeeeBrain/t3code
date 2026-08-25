// Mock Pi CLI speaking the `pi --mode rpc` NDJSON protocol for adapter tests.
// Usage: node pi-rpc-mock-agent.mjs [ignored args]
// Set PI_MOCK_MODE=hold to defer all prompt events until an `abort` arrives.
import * as NodeReadline from "node:readline";
import * as NodeProcess from "node:process";

const holdMode = NodeProcess.env["PI_MOCK_MODE"] === "hold";
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
      id: "mock-msg-1",
      content: [{ type: "text", text: "Hello world" }],
    },
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
        model: { id: "mock/mock-model" },
        thinkingLevel: "medium",
        isStreaming: false,
        isCompacting: false,
        sessionId: "mock-pi-session",
      });
      return;
    case "prompt":
      if (parsed.streamingBehavior !== undefined) {
        // Steered messages produce no new lifecycle of their own.
        respond(id, "prompt", true);
        return;
      }
      respond(id, "prompt", true);
      if (holdMode) return; // events wait for abort
      emitPromptFlow();
      return;
    case "abort":
      respond(id, "abort", true);
      if (holdMode) emitPromptFlow();
      return;
    default:
      respond(id, String(parsed.type ?? "unknown"), true);
  }
});
