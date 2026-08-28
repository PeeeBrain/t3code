# Pi provider

> For maintainers. User setup is in [Pi](../user/providers-pi.md).

Pi is an external CLI provider. `PiDriver` owns the provider instance settings and creates a
`PiAdapter`. The adapter starts one `pi --mode rpc` process for each T3 thread session. It passes the
configured binary path, working directory, merged provider environment, session ID, and optional
model selection to that process.

## Why the adapter uses RPC

The RPC boundary keeps Pi's process separate from the T3 server. It also lets users run the Pi
version they installed, with its own credentials, extensions, skills, and model catalog. This matters
for remote T3 environments and for provider instances that use different binary paths or environment
variables.

Pi's SDK has richer typed discovery APIs, including `ModelRuntime.getProviders()`,
`checkAuth(provider.id)`, `getAvailable()`, and `ResourceLoader` access to skills and prompts. Using
the SDK directly would tie the server to one Pi package version and would require another process
boundary to preserve the current provider architecture. Keep SDK use to a separate discovery helper
only if T3 needs auth detail that RPC cannot provide.

See Pi's [SDK documentation](https://pi.dev/docs/latest/sdk) and [RPC documentation](https://pi.dev/docs/latest/rpc).

## Discovery contract

The health probe runs `pi --version`, then starts one short-lived no-session RPC client that sends
`get_available_models` and `get_commands` concurrently. Discovery uses the same typed JSONL client
as live sessions; responses are decoded once and never serialized back to JSON for a second parse.
The client runs in `ServerConfig.cwd`, matching the workspace root used by the server and the
project-scoped resource convention used by other providers.

The model parser keeps the `provider/id` slug, display name, `reasoning` flag, and
`thinkingLevelMap`. Pi's map is tristate: omitted standard levels through `high` keep their default
mapping, strings override it, and `null` disables it. Extended `xhigh` and `max` require explicit
non-null entries. Every reasoning selector starts on **Pi default**; that sentinel does not send
`set_thinking_level`. Live sessions query `get_available_thinking_levels` after startup and after a
model switch, then validate explicit selections against that authoritative result.

`get_available_models` can contain providers without usable credentials. The probe asks Pi itself
with `pi auth check --json --no-refresh --provider <id>` under the provider instance's merged
environment. This honors stored credentials and instance-injected environment credentials without
reconstructing Pi's private credential-store semantics. Failed or timed-out readiness checks keep
their models visible but downgrade provider health to unknown instead of deleting potentially
usable models.

`get_commands` returns extension commands, prompt templates, and skills. Pi 0.84.x nests source
metadata under `sourceInfo`; newer documented top-level fields are also accepted. Skill paths are
optional because Pi documents them as optional. Built-in interactive TUI commands remain excluded.

## Scoped-model limitation

Pi has no T3 default model catalog. The provider snapshot currently falls back to the configured
`customModels` entries when CLI discovery fails or returns no models. Those entries are user-authored
slugs, not evidence that Pi authenticated or that the model is in Pi's current scope.

The instance model selector must stay keyed by the Pi provider instance. It must not fall back to a
global built-in catalog when Pi's scoped discovery is empty. RPC discovery already preserves Pi's
full model metadata; if Pi later exposes an explicit scoped/enabled-model list, prefer it over the
readiness-filtered catalog and make the fallback state visible rather than presenting all models as
configured.

## Runtime lifecycle

Pi emits JSONL responses and asynchronous events on stdout. The runtime owns process startup,
newline-delimited parsing, request IDs, stdin writes, and teardown. The adapter translates Pi events
into T3 runtime events.

`agent_end` is not completion. Pi can retry, compact, summarize, or process queued steering and
follow-up messages after it. The adapter should settle a T3 turn only at `agent_settled`, while
retaining failure details from retry, compaction, extension-error, and process-exit events.

The runtime must also reject pending requests when Pi exits or its pipes fail, remove request IDs from
the pending map after every response, and keep one stdout reader and one serialized stdin writer for
the lifetime of the process. Pi's protocol requires strict JSONL framing. A parser must split on LF,
accept CRLF by removing the trailing CR, and must not use a line reader that treats valid JSON
separator characters as delimiters.

Pi's `abort` command is request/response based. A stop operation may terminate the process after the
abort path, but it should wait for the process exit or use a bounded SIGTERM/SIGKILL sequence so an
active T3 turn cannot remain marked as streaming.

## Extension UI

Blocking `select`, `confirm`, `input`, and `editor` requests become canonical
`user-input.requested` events. `respondToUserInput` converts the client answer into the matching
`extension_ui_response`, then emits `user-input.resolved`. Interrupt and stop paths cancel pending
dialogs so extensions cannot remain blocked. `notify` becomes a visible `runtime.warning` activity
row. Status, widget, title, and editor-text updates are terminal-UI chrome with no cross-client T3
surface, so they remain fire-and-forget and are intentionally ignored.

`extension_error` becomes `runtime.error`. Compaction is represented as a `context_compaction` item;
a terminal compaction error contributes to turn failure. Retry and summarization-retry events keep
the turn active until `agent_settled`.

## Rollback

T3 checkpoint restore calls `rollbackThread`. Pi implements the provider-context half as
`get_fork_messages` plus `fork` at the user message that started the oldest removed turn. A fork
mints a new Pi session id, so the adapter updates its resume cursor before returning. Steering
messages are counted inside their owning T3 turn when choosing the fork boundary.

## RPC coverage

| Pi RPC surface                                         | T3 adaptation                                                                                     |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `prompt` with images                                   | Implemented. Mid-turn T3 input uses `prompt.streamingBehavior = "steer"`.                         |
| `steer` / `follow_up`                                  | Semantically mapped to T3's merged-turn steer behavior; direct queue commands are not exposed.    |
| `abort` / `clear_queue`                                | Implemented for interruption.                                                                     |
| `new_session`                                          | T3 creates a new process/session id instead; direct switching is intentionally provider-internal. |
| `get_state`                                            | Implemented for model, thinking level, and forked session-id synchronization.                     |
| `get_messages`                                         | Not used; T3's event store owns transcript projection.                                            |
| `set_model` / `get_available_models`                   | Implemented.                                                                                      |
| `cycle_model`                                          | Not exposed; T3 selects explicit model ids.                                                       |
| `set_thinking_level` / `get_available_thinking_levels` | Implemented.                                                                                      |
| `cycle_thinking_level`                                 | Not exposed; T3 selects explicit levels.                                                          |
| Steering/follow-up queue modes                         | Provider-internal; T3 owns its send queue semantics.                                              |
| `compact` / `set_auto_compaction`                      | No direct T3 command; manual and automatic compaction events are fully projected.                 |
| `set_auto_retry` / `abort_retry`                       | Provider-internal configuration; retry events are projected.                                      |
| `bash` / `abort_bash`                                  | Not used; agent tool executions flow through canonical tool lifecycle events.                     |
| `get_session_stats`                                    | Not polled; streaming usage events update T3 token state.                                         |
| `export_html`                                          | No T3 product surface.                                                                            |
| `switch_session` / `clone`                             | T3 owns thread lifecycle and checkpoints; direct provider session switching is not exposed.       |
| `fork` / `get_fork_messages`                           | Implemented as T3 rollback.                                                                       |
| `get_entries` / `get_tree`                             | Provider-internal; rollback uses the narrower fork-message query.                                 |
| `get_last_assistant_text`                              | Not used; T3 projects assistant items from events.                                                |
| `set_session_name`                                     | T3 owns thread titles.                                                                            |
| `get_commands`                                         | Implemented for extensions, prompt templates, and skills.                                         |
| Extension UI responses                                 | Implemented for all blocking dialog methods.                                                      |
| Agent/message/tool lifecycle events                    | Implemented; turn completion occurs only at `agent_settled`.                                      |
| Queue and direct-bash updates                          | Intentionally ignored because T3 does not issue the corresponding direct commands.                |
| Compaction, retry, summarization retry                 | Projected into canonical item/session/error state.                                                |
| `extension_error` and extension UI requests            | Implemented as runtime errors, notices, and canonical user input.                                 |
