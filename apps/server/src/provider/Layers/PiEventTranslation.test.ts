import { assert, describe, it } from "@effect/vitest";
import { EventId, ProviderDriverKind, RuntimeItemId, ThreadId, TurnId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  beginPiTurn,
  cancelPendingExtensionDialogs,
  initialPiTranslationState,
  markTurnInterrupted,
  piSettleTurn,
  reducePiEvent,
  resolvePiExtensionDialog,
  type PiTranslationContext,
  type PiTranslationDeps,
} from "./PiEventTranslation.ts";
import type { PiEvent, PiExtensionUiRequest } from "./PiRpcProtocol.ts";

describe("PiEventTranslation", () => {
  let eventCounter = 0;
  let itemCounter = 0;

  const mockDeps: PiTranslationDeps = {
    stamp: () =>
      Effect.succeed({
        eventId: EventId.make(`evt_${++eventCounter}`),
        createdAt: "2026-08-28T12:00:00.000Z",
      }),
    newItemId: () => Effect.succeed(RuntimeItemId.make(`item_${++itemCounter}`)),
  };

  const mockCtx: PiTranslationContext = {
    provider: ProviderDriverKind.make("pi"),
    threadId: ThreadId.make("thread_1"),
  };

  it("initializes and begins a turn", () => {
    const s0 = initialPiTranslationState();
    assert.isFalse(s0.streaming);
    assert.isUndefined(s0.activeTurnId);
    assert.strictEqual(s0.turns.length, 0);

    const turnId = TurnId.make("turn_1");
    const s1 = beginPiTurn(s0, turnId);
    assert.isTrue(s1.streaming);
    assert.strictEqual(s1.activeTurnId, turnId);
    assert.strictEqual(s1.turns.length, 1);
    assert.strictEqual(s1.turns[0]?.id, turnId);
  });

  it("marks a turn as interrupted", () => {
    const s0 = initialPiTranslationState();
    const s1 = markTurnInterrupted(s0, "turn_1");
    assert.isTrue(s1.interruptedTurnIds.has("turn_1"));
  });

  it("settles turn with different outcomes", async () => {
    const turnId = TurnId.make("turn_1");
    const s0 = beginPiTurn(initialPiTranslationState(), turnId);

    // Completed
    const resCompleted = await Effect.runPromise(piSettleTurn(s0, "completed", mockCtx, mockDeps));
    assert.isFalse(resCompleted.state.streaming);
    assert.isUndefined(resCompleted.state.activeTurnId);
    assert.strictEqual(resCompleted.events.length, 1);
    assert.strictEqual(resCompleted.events[0]?.type, "turn.completed");
    assert.strictEqual((resCompleted.events[0]?.payload as any)?.state, "completed");
    assert.deepStrictEqual(resCompleted.turnSettled, {
      turnId,
      outcome: "completed",
      errorMessage: undefined,
    });

    // Failed
    const resFailed = await Effect.runPromise(
      piSettleTurn(s0, "failed", mockCtx, mockDeps, "Explicit error"),
    );
    assert.strictEqual(resFailed.events[0]?.type, "turn.completed");
    assert.strictEqual((resFailed.events[0]?.payload as any)?.state, "failed");
    assert.strictEqual((resFailed.events[0]?.payload as any)?.errorMessage, "Explicit error");
    assert.deepStrictEqual(resFailed.turnSettled, {
      turnId,
      outcome: "failed",
      errorMessage: "Explicit error",
    });

    // Cancelled
    const resCancelled = await Effect.runPromise(piSettleTurn(s0, "cancelled", mockCtx, mockDeps));
    assert.strictEqual(resCancelled.events[0]?.type, "turn.completed");
    assert.strictEqual((resCancelled.events[0]?.payload as any)?.state, "cancelled");
    assert.deepStrictEqual(resCancelled.turnSettled, {
      turnId,
      outcome: "cancelled",
      errorMessage: undefined,
    });

    // Settling when no active turn
    const resNoTurn = await Effect.runPromise(
      piSettleTurn(initialPiTranslationState(), "completed", mockCtx, mockDeps),
    );
    assert.strictEqual(resNoTurn.events.length, 0);
    assert.isUndefined(resNoTurn.turnSettled);
  });

  it("handles extension UI dialog requests and resolutions", async () => {
    const turnId = TurnId.make("turn_1");
    let state = beginPiTurn(initialPiTranslationState(), turnId);

    // Select dialog
    const selectReq: PiExtensionUiRequest = {
      type: "extension_ui_request",
      id: "req_select",
      method: "select",
      title: "Select Option",
      message: undefined,
      placeholder: undefined,
      prefill: undefined,
      options: ["Option A", "Option B"],
      timeout: undefined,
      notifyType: undefined,
      raw: {},
    };

    const resReq = await Effect.runPromise(reducePiEvent(state, selectReq, mockCtx, mockDeps));
    state = resReq.state;
    assert.strictEqual(resReq.events.length, 1);
    assert.strictEqual(resReq.events[0]?.type, "user-input.requested");
    assert.isTrue(state.pendingDialogs.has("req_select"));

    // Resolve select dialog
    const resResolve = await Effect.runPromise(
      resolvePiExtensionDialog(state, "req_select", { req_select: "Option A" }, mockCtx, mockDeps),
    );
    state = resResolve.state;
    assert.isFalse(state.pendingDialogs.has("req_select"));
    assert.strictEqual(resResolve.events.length, 1);
    assert.strictEqual(resResolve.events[0]?.type, "user-input.resolved");
    assert.deepStrictEqual(resResolve.response, {
      type: "extension_ui_response",
      id: "req_select",
      value: "Option A",
    });

    // Confirm dialog
    const confirmReq: PiExtensionUiRequest = {
      type: "extension_ui_request",
      id: "req_confirm",
      method: "confirm",
      title: "Confirm Action",
      message: "Are you sure?",
      placeholder: undefined,
      prefill: undefined,
      options: [],
      timeout: undefined,
      notifyType: undefined,
      raw: {},
    };
    const resConfirmReq = await Effect.runPromise(
      reducePiEvent(state, confirmReq, mockCtx, mockDeps),
    );
    state = resConfirmReq.state;
    assert.isTrue(state.pendingDialogs.has("req_confirm"));

    const resResolveConfirm = await Effect.runPromise(
      resolvePiExtensionDialog(state, "req_confirm", { req_confirm: "Yes" }, mockCtx, mockDeps),
    );
    state = resResolveConfirm.state;
    assert.deepStrictEqual(resResolveConfirm.response, {
      type: "extension_ui_response",
      id: "req_confirm",
      confirmed: true,
    });
  });

  it("cancels all pending extension dialogs", async () => {
    let state = beginPiTurn(initialPiTranslationState(), TurnId.make("turn_1"));
    const selectReq: PiExtensionUiRequest = {
      type: "extension_ui_request",
      id: "req_1",
      method: "select",
      title: "Select",
      message: undefined,
      placeholder: undefined,
      prefill: undefined,
      options: ["A"],
      timeout: undefined,
      notifyType: undefined,
      raw: {},
    };
    const resReq = await Effect.runPromise(reducePiEvent(state, selectReq, mockCtx, mockDeps));
    state = resReq.state;
    assert.strictEqual(state.pendingDialogs.size, 1);

    const resCancel = await Effect.runPromise(
      cancelPendingExtensionDialogs(state, mockCtx, mockDeps),
    );
    assert.strictEqual(resCancel.state.pendingDialogs.size, 0);
    assert.strictEqual(resCancel.events.length, 1);
    assert.strictEqual(resCancel.events[0]?.type, "user-input.resolved");
    assert.strictEqual(resCancel.responses.length, 1);
    assert.deepStrictEqual(resCancel.responses[0], {
      type: "extension_ui_response",
      id: "req_1",
      cancelled: true,
    });
  });

  it("translates message deltas, tool executions, and usage updates", async () => {
    const turnId = TurnId.make("turn_1");
    let state = beginPiTurn(initialPiTranslationState(), turnId);

    // message_start
    const msgStart: PiEvent = {
      type: "message_start",
      message: { role: "assistant" },
    };
    let res = await Effect.runPromise(reducePiEvent(state, msgStart, mockCtx, mockDeps));
    state = res.state;
    assert.isDefined(state.assistantItemId);
    assert.strictEqual(res.events[0]?.type, "item.started");

    // message_update with text delta and usage
    const msgUpdate: PiEvent = {
      type: "message_update",
      assistantMessageEvent: { type: "text_delta", delta: "Hello world" },
      usage: { input: 10, output: 5, totalTokens: 15 },
    };
    res = await Effect.runPromise(reducePiEvent(state, msgUpdate, mockCtx, mockDeps));
    state = res.state;
    assert.strictEqual(res.events.length, 2);
    assert.strictEqual(res.events[0]?.type, "content.delta");
    assert.strictEqual((res.events[0]?.payload as any)?.delta, "Hello world");
    assert.strictEqual(res.events[1]?.type, "thread.token-usage.updated");

    // tool execution start/end
    const toolStart: PiEvent = {
      type: "tool_execution_start",
      toolCallId: "call_1",
      toolName: "bash",
      args: { command: "echo ok" },
      partialResult: undefined,
      result: undefined,
      isError: false,
    };
    res = await Effect.runPromise(reducePiEvent(state, toolStart, mockCtx, mockDeps));
    assert.strictEqual(res.events.length, 1);
    assert.strictEqual(res.events[0]?.type, "item.started");
    assert.strictEqual((res.events[0]?.payload as any)?.itemType, "command_execution");

    const toolEnd: PiEvent = {
      type: "tool_execution_end",
      toolCallId: "call_1",
      toolName: "bash",
      args: undefined,
      partialResult: undefined,
      result: "ok\n",
      isError: false,
    };
    res = await Effect.runPromise(reducePiEvent(state, toolEnd, mockCtx, mockDeps));
    assert.strictEqual(res.events.length, 1);
    assert.strictEqual(res.events[0]?.type, "item.completed");
    assert.strictEqual((res.events[0]?.payload as any)?.status, "completed");

    // message_end
    const msgEnd: PiEvent = {
      type: "message_end",
      message: { role: "assistant", stopReason: "stop" },
    };
    res = await Effect.runPromise(reducePiEvent(state, msgEnd, mockCtx, mockDeps));
    state = res.state;
    assert.isUndefined(state.assistantItemId);
    assert.strictEqual(res.events[0]?.type, "item.completed");

    // agent_settled settles completed
    res = await Effect.runPromise(
      reducePiEvent(state, { type: "agent_settled" }, mockCtx, mockDeps),
    );
    assert.isFalse(res.state.streaming);
    assert.strictEqual(res.turnSettled?.outcome, "completed");
  });

  it("handles compaction and retry events", async () => {
    const turnId = TurnId.make("turn_1");
    let state = beginPiTurn(initialPiTranslationState(), turnId);

    // Compaction start and failed end
    const compStart: PiEvent = { type: "compaction_start", reason: "overflow" };
    let res = await Effect.runPromise(reducePiEvent(state, compStart, mockCtx, mockDeps));
    state = res.state;
    assert.isDefined(state.compactionItemId);
    assert.strictEqual(res.events[0]?.type, "item.started");

    const compEnd: PiEvent = {
      type: "compaction_end",
      reason: "overflow",
      result: undefined,
      aborted: false,
      willRetry: false,
      errorMessage: "Compaction context exceeded limit",
    };
    res = await Effect.runPromise(reducePiEvent(state, compEnd, mockCtx, mockDeps));
    state = res.state;
    assert.isUndefined(state.compactionItemId);
    assert.strictEqual(state.pendingFailureMessage, "Compaction context exceeded limit");
    assert.strictEqual(res.events[0]?.type, "item.completed");
    assert.strictEqual((res.events[0]?.payload as any)?.status, "failed");

    // agent_settled settles failed because of pending compaction failure
    res = await Effect.runPromise(
      reducePiEvent(state, { type: "agent_settled" }, mockCtx, mockDeps),
    );
    assert.strictEqual(res.turnSettled?.outcome, "failed");
    assert.strictEqual(res.turnSettled?.errorMessage, "Compaction context exceeded limit");
  });

  it("handles extension errors and notify requests", async () => {
    const turnId = TurnId.make("turn_1");
    const state = beginPiTurn(initialPiTranslationState(), turnId);

    // extension_error -> runtime.error
    const extErr: PiEvent = {
      type: "extension_error",
      extensionPath: "/plugins/custom.ts",
      event: "command",
      error: "Boom",
    };
    let res = await Effect.runPromise(reducePiEvent(state, extErr, mockCtx, mockDeps));
    assert.strictEqual(res.events.length, 1);
    assert.strictEqual(res.events[0]?.type, "runtime.error");

    // extension_ui_request with notify -> runtime.warning
    const notifyReq: PiExtensionUiRequest = {
      type: "extension_ui_request",
      id: "note_1",
      method: "notify",
      title: "Notice title",
      message: "Important notice",
      placeholder: undefined,
      prefill: undefined,
      options: [],
      timeout: undefined,
      notifyType: "warning",
      raw: {},
    };
    res = await Effect.runPromise(reducePiEvent(state, notifyReq, mockCtx, mockDeps));
    assert.strictEqual(res.events.length, 1);
    assert.strictEqual(res.events[0]?.type, "runtime.warning");

    // summarization_retry_scheduled -> runtime.warning
    const sumRetry: PiEvent = {
      type: "summarization_retry_scheduled",
      source: "auto",
      reason: "rate_limit",
      attempt: 1,
      errorMessage: "Too many requests",
    };
    res = await Effect.runPromise(reducePiEvent(state, sumRetry, mockCtx, mockDeps));
    assert.strictEqual(res.events.length, 1);
    assert.strictEqual(res.events[0]?.type, "runtime.warning");
  });
});
