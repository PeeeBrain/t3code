import { assert, describe, it } from "@effect/vitest";

import {
  decodePiEvent,
  decodePiRpcResponse,
  isPiThinkingLevel,
  PI_DEFAULT_THINKING_LEVEL,
  PI_EXTENSION_DIALOG_METHODS,
  PI_EXTENSION_NOTIFY_METHODS,
  piForkMessagesFromData,
  piModelFromState,
  piSessionIdFromState,
  piSupportedThinkingLevels,
  piThinkingLevelFromState,
  piThinkingLevelsFromData,
  piUsage,
} from "./PiRpcProtocol.ts";

describe("PiRpcProtocol", () => {
  describe("thinking levels and tri-state map", () => {
    it("recognizes valid thinking levels", () => {
      assert.isTrue(isPiThinkingLevel("off"));
      assert.isTrue(isPiThinkingLevel("minimal"));
      assert.isTrue(isPiThinkingLevel("low"));
      assert.isTrue(isPiThinkingLevel("medium"));
      assert.isTrue(isPiThinkingLevel("high"));
      assert.isTrue(isPiThinkingLevel("xhigh"));
      assert.isTrue(isPiThinkingLevel("max"));
      assert.isFalse(isPiThinkingLevel("default"));
      assert.isFalse(isPiThinkingLevel("ultra"));
      assert.isFalse(isPiThinkingLevel(123));
      assert.isFalse(isPiThinkingLevel(null));
      assert.strictEqual(PI_DEFAULT_THINKING_LEVEL, "default");
    });

    it("returns standard levels (off through high) when thinkingLevelMap is undefined", () => {
      const levels = piSupportedThinkingLevels(undefined);
      assert.deepStrictEqual(levels, ["off", "minimal", "low", "medium", "high"]);
    });

    it("handles tri-state thinkingLevelMap correctly", () => {
      // Omitted standard levels remain supported; null disables them; xhigh/max need explicit non-null
      const map1 = {
        low: null, // explicitly disabled
        medium: "budget:2048", // mapped to string -> supported
        xhigh: "budget:8192", // explicitly enabled extended level
        max: null, // extended level disabled
      };
      const levels1 = piSupportedThinkingLevels(map1);
      assert.deepStrictEqual(levels1, ["off", "minimal", "medium", "high", "xhigh"]);

      const map2 = {
        off: null,
        minimal: null,
        low: null,
        medium: null,
        high: null,
        max: true,
      };
      const levels2 = piSupportedThinkingLevels(map2);
      assert.deepStrictEqual(levels2, ["max"]);
    });
  });

  describe("decodePiRpcResponse", () => {
    it("decodes successful responses with id and data", () => {
      const res = decodePiRpcResponse({
        type: "response",
        id: "req_1",
        command: "get_state",
        success: true,
        data: { sessionId: "sess-123" },
      });
      assert.isDefined(res);
      assert.strictEqual(res?.kind, "success");
      if (res?.kind === "success") {
        assert.strictEqual(res.id, "req_1");
        assert.strictEqual(res.command, "get_state");
        assert.deepStrictEqual(res.data, { sessionId: "sess-123" });
      }
    });

    it("decodes failed responses with error", () => {
      const res = decodePiRpcResponse({
        type: "response",
        id: "req_2",
        command: "prompt",
        success: false,
        error: "Process terminated",
      });
      assert.isDefined(res);
      assert.strictEqual(res?.kind, "failure");
      if (res?.kind === "failure") {
        assert.strictEqual(res.id, "req_2");
        assert.strictEqual(res.command, "prompt");
        assert.strictEqual(res.error, "Process terminated");
      }
    });

    it("returns undefined for non-responses or invalid records", () => {
      assert.isUndefined(decodePiRpcResponse(null));
      assert.isUndefined(decodePiRpcResponse("not an object"));
      assert.isUndefined(decodePiRpcResponse({ type: "agent_start" }));
    });
  });

  describe("decodePiEvent", () => {
    it("decodes agent lifecycle events", () => {
      assert.deepStrictEqual(decodePiEvent({ type: "agent_start" }), { type: "agent_start" });
      assert.deepStrictEqual(decodePiEvent({ type: "agent_end", willRetry: true }), {
        type: "agent_end",
        willRetry: true,
      });
      assert.deepStrictEqual(decodePiEvent({ type: "agent_settled" }), { type: "agent_settled" });
      assert.deepStrictEqual(decodePiEvent({ type: "turn_start" }), { type: "turn_start" });
      assert.deepStrictEqual(decodePiEvent({ type: "turn_end" }), { type: "turn_end" });
    });

    it("decodes message events", () => {
      const msgStart = decodePiEvent({
        type: "message_start",
        message: { role: "assistant", id: "msg_1" },
      });
      assert.deepStrictEqual(msgStart, {
        type: "message_start",
        message: { role: "assistant", id: "msg_1" },
      });

      const msgUpdate = decodePiEvent({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "hello" },
        usage: { input: 10, output: 5, totalTokens: 15 },
      });
      assert.deepStrictEqual(msgUpdate, {
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "hello" },
        usage: { input: 10, output: 5, totalTokens: 15 },
      });

      const msgEnd = decodePiEvent({
        type: "message_end",
        message: { role: "assistant", stopReason: "stop" },
      });
      assert.deepStrictEqual(msgEnd, {
        type: "message_end",
        message: { role: "assistant", stopReason: "stop" },
      });
    });

    it("decodes tool execution events", () => {
      const toolStart = decodePiEvent({
        type: "tool_execution_start",
        toolCallId: "call_1",
        toolName: "bash",
        args: { command: "ls" },
      });
      assert.deepStrictEqual(toolStart, {
        type: "tool_execution_start",
        toolCallId: "call_1",
        toolName: "bash",
        args: { command: "ls" },
        partialResult: undefined,
        result: undefined,
        isError: false,
      });

      const toolEnd = decodePiEvent({
        type: "tool_execution_end",
        toolCallId: "call_1",
        toolName: "bash",
        result: "output",
        isError: true,
      });
      assert.deepStrictEqual(toolEnd, {
        type: "tool_execution_end",
        toolCallId: "call_1",
        toolName: "bash",
        args: undefined,
        partialResult: undefined,
        result: "output",
        isError: true,
      });
    });

    it("decodes compaction and retry events", () => {
      const compStart = decodePiEvent({ type: "compaction_start", reason: "threshold" });
      assert.deepStrictEqual(compStart, { type: "compaction_start", reason: "threshold" });

      const compEnd = decodePiEvent({
        type: "compaction_end",
        reason: "threshold",
        aborted: false,
        willRetry: false,
        errorMessage: "failed",
      });
      assert.deepStrictEqual(compEnd, {
        type: "compaction_end",
        reason: "threshold",
        result: undefined,
        aborted: false,
        willRetry: false,
        errorMessage: "failed",
      });

      const retryStart = decodePiEvent({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 1000,
        errorMessage: "rate limit",
      });
      assert.deepStrictEqual(retryStart, {
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 1000,
        errorMessage: "rate limit",
      });

      const retryEnd = decodePiEvent({
        type: "auto_retry_end",
        success: false,
        attempt: 3,
        finalError: "exhausted",
      });
      assert.deepStrictEqual(retryEnd, {
        type: "auto_retry_end",
        success: false,
        attempt: 3,
        finalError: "exhausted",
      });
    });

    it("decodes extension errors and UI requests", () => {
      const extError = decodePiEvent({
        type: "extension_error",
        extensionPath: "/path/to/ext.ts",
        event: "on_init",
        error: "crash",
      });
      assert.deepStrictEqual(extError, {
        type: "extension_error",
        extensionPath: "/path/to/ext.ts",
        event: "on_init",
        error: "crash",
      });

      const rawUi = {
        type: "extension_ui_request",
        id: "ui_1",
        method: "select",
        title: "Pick one",
        options: ["A", "B"],
      };
      const uiReq = decodePiEvent(rawUi);
      assert.deepStrictEqual(uiReq, {
        type: "extension_ui_request",
        id: "ui_1",
        method: "select",
        title: "Pick one",
        message: undefined,
        placeholder: undefined,
        prefill: undefined,
        options: ["A", "B"],
        timeout: undefined,
        notifyType: undefined,
        raw: rawUi,
      });
    });

    it("returns undefined for unknown event types or malformed events", () => {
      assert.isUndefined(decodePiEvent(null));
      assert.isUndefined(decodePiEvent({ type: "unknown_custom_event" }));
      assert.isUndefined(decodePiEvent({}));
    });
  });

  describe("payload accessors", () => {
    it("extracts token usage correctly", () => {
      assert.isUndefined(piUsage(undefined));
      assert.isUndefined(piUsage({}));

      const usage = piUsage({
        input: 100,
        output: 50,
        totalTokens: 150,
        cacheRead: 20,
        reasoning: 30,
      });
      assert.deepStrictEqual(usage, {
        usedTokens: 150,
        lastUsedTokens: 150,
        inputTokens: 100,
        lastInputTokens: 100,
        cachedInputTokens: 20,
        outputTokens: 50,
        lastOutputTokens: 50,
        reasoningOutputTokens: 30,
      });
    });

    it("extracts session ID, model, and thinking level from state", () => {
      const state = {
        sessionId: "session-abc",
        model: { provider: "anthropic", id: "claude-3-7-sonnet" },
        thinkingLevel: "high",
      };
      assert.strictEqual(piSessionIdFromState(state), "session-abc");
      assert.strictEqual(piModelFromState(state), "anthropic/claude-3-7-sonnet");
      assert.strictEqual(piThinkingLevelFromState(state), "high");

      const stateNoProvider = {
        model: { id: "custom-model" },
        thinkingLevel: "invalid_level",
      };
      assert.strictEqual(piModelFromState(stateNoProvider), "custom-model");
      assert.isUndefined(piThinkingLevelFromState(stateNoProvider));
    });

    it("decodes thinking levels from get_available_thinking_levels response data", () => {
      assert.deepStrictEqual(
        piThinkingLevelsFromData({ levels: ["off", "low", "medium", "invalid"] }),
        ["off", "low", "medium"],
      );
      assert.deepStrictEqual(piThinkingLevelsFromData({}), []);
    });

    it("decodes fork messages from get_fork_messages response data", () => {
      const forkData = {
        messages: [
          { entryId: "entry_1", text: "first message" },
          { entryId: "entry_2", text: "second message" },
          { entryId: "", text: "ignored" },
          { text: "no entry id" },
        ],
      };
      assert.deepStrictEqual(piForkMessagesFromData(forkData), [
        { entryId: "entry_1", text: "first message" },
        { entryId: "entry_2", text: "second message" },
      ]);
      assert.deepStrictEqual(piForkMessagesFromData({}), []);
    });

    it("maintains extension dialog and notify method classifications", () => {
      assert.isTrue(PI_EXTENSION_DIALOG_METHODS.has("select"));
      assert.isTrue(PI_EXTENSION_DIALOG_METHODS.has("confirm"));
      assert.isTrue(PI_EXTENSION_DIALOG_METHODS.has("input"));
      assert.isTrue(PI_EXTENSION_DIALOG_METHODS.has("editor"));
      assert.isFalse(PI_EXTENSION_DIALOG_METHODS.has("notify"));

      assert.isTrue(PI_EXTENSION_NOTIFY_METHODS.has("notify"));
      assert.isTrue(PI_EXTENSION_NOTIFY_METHODS.has("setStatus"));
      assert.isTrue(PI_EXTENSION_NOTIFY_METHODS.has("setWidget"));
      assert.isTrue(PI_EXTENSION_NOTIFY_METHODS.has("setTitle"));
      assert.isTrue(PI_EXTENSION_NOTIFY_METHODS.has("set_editor_text"));
      assert.isFalse(PI_EXTENSION_NOTIFY_METHODS.has("select"));
    });
  });
});
