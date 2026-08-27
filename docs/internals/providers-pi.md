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

The health probe runs `pi --version`, then starts one short-lived no-session RPC process that
answers both `get_available_models` and `get_commands`. The model parser keeps the `provider/id`
slug, Pi's display name, the `reasoning` flag, and `thinkingLevelMap`. Models are intersected with
the providers configured in Pi's auth store before they reach the snapshot; a non-empty filtered
list is reported as authenticated, an empty one as unauthenticated. RPC cannot distinguish missing
credentials from a configured provider with no available models.

Reasoning-capable models receive a `reasoningEffort` descriptor containing exactly the levels Pi
maps for that model. The adapter applies that option through `set_thinking_level` at session start
and before a non-steering turn. `get_available_thinking_levels` remains the session-scoped
authoritative query if a future path ever needs to re-check levels mid-session.

A future session-scoped discovery path can request:

- `get_available_models` for Pi's available, authentication-filtered model objects
- `get_commands` for extension, prompt-template, and skill commands
- `get_state` for the session's current model and lifecycle state

`get_available_models` is the source for the Pi model picker: one bounded RPC process answers both
`get_available_models` and `get_commands`. Each model object carries `reasoning` and a
`thinkingLevelMap` (canonical level → provider parameter, `null` = unsupported), which becomes the
per-model reasoning selector; the default is `medium` when supported, otherwise the highest
supported level. Neither RPC nor the CLI table filters by authentication, so the probe intersects
the discovered model set with the provider keys stored in Pi's credential store
(`<PI_CODING_AGENT_DIR or ~/.pi/agent>/auth.json`) and counts the distinct providers that survive.
Ambient environment tokens (for example `ANTHROPIC_AUTH_TOKEN`) make `pi auth check` report a
provider ready without a store entry, so the store — not `pi auth check` — is the truth for what
belongs in the picker. RPC still does not expose SDK-style `getProviders()` or `checkAuth()`, so
distinguishing an unauthenticated provider from one with no models still needs an additional
Pi-side discovery helper.

`get_commands` returns `name`, optional `description`, `source`, and optional location/path data.
The source is one of `extension`, `prompt`, or `skill`; Pi prefixes skill commands with `skill:`.
Pi 0.84.x nests metadata under `sourceInfo` (`path`, `scope`); the documented top-level `path` /
`location` shape is parsed as a fallback. Built-in interactive TUI commands are intentionally
excluded. Normalize this response once at the provider boundary and reuse it for the slash and
skill menus. The current probe uses the server process working directory, so project-local
inventory is not yet truly thread-scoped; move this query onto the live thread session before
claiming exact project-level discovery.

## Scoped-model limitation

Pi has no T3 default model catalog. The provider snapshot currently falls back to the configured
`customModels` entries when CLI discovery fails or returns no models. Those entries are user-authored
slugs, not evidence that Pi authenticated or that the model is in Pi's current scope.

The instance model selector must stay keyed by the Pi provider instance. It must not fall back to a
global built-in catalog when Pi's scoped discovery is empty. RPC discovery already preserves Pi's
full model metadata; if Pi later exposes an explicit scoped/enabled-model list, prefer it over the
auth-store intersection and make the fallback state visible rather than presenting all models as
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
