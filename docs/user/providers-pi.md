# Pi

Pi is an optional provider in T3 Code. T3 starts the Pi CLI on the machine that runs the T3 Code
server. The browser or phone does not need Pi installed.

## Install and enable Pi

Install Pi using the [Pi documentation](https://pi.dev/docs/latest/), then open **Settings** in T3
Code and enable the Pi provider. Leave **Binary path** as `pi` when the binary is on the server's
`PATH`. Set the full path when Pi was installed by a version manager or in another location.

Run Pi's login flow, or configure the provider credentials, on the server. Pi keeps its own provider
configuration and credentials. See Pi's [provider and authentication guide](https://pi.dev/docs/latest/providers).

If you connect to a remote T3 Code server, install and authenticate Pi there. Installing it on the
device that opens T3 Code does not configure the remote server.

## Models and provider count

Pi owns the model catalog. T3 asks Pi which models are currently available, then groups those models
by their `provider/model` identifier. A model is available when Pi can resolve its provider and
credentials. Models from providers that are not configured or authenticated should not appear in the
Pi model picker.

Models that Pi marks as reasoning-capable show a **Reasoning** selector with exactly the levels Pi
maps for that model (from `thinkingLevelMap` on Pi's model objects) — for example Low, High, and Max
on models that support them. The selected level is applied with `set_thinking_level` before the
turn. When Pi's own default level is unsupported for a model, T3 falls back to the highest level the
model does support.

One Pi card in T3 Code represents one Pi CLI setup. That setup can expose models from several Pi
providers, so the number of Pi cards is not the number of model providers in Pi.

The provider card reports the number of upstream providers represented by Pi's available model
list, intersected with the providers configured in Pi's credential store (`auth.json`). Providers
that only have an ambient environment token (for example `ANTHROPIC_AUTH_TOKEN`) are not counted
and their models stay out of the picker. If the filtered list is empty, T3 reports Pi as
unauthenticated because RPC cannot distinguish missing credentials from a configured provider that
currently exposes no models.

## Commands and skills

Pi can load commands from extensions, prompt templates, and skills. T3 Code uses Pi's RPC command
list for the composer menus:

- Type `/` to browse Pi extension and prompt-template commands.
- Type `$` to browse installed skills when the client exposes the skills menu.
- Skill commands use Pi's `/skill:<name>` form.

Pi's built-in interactive TUI commands are not part of its RPC command list. A command such as
`/settings` may therefore remain available only inside Pi's own terminal UI. See Pi's [skills
guide](https://pi.dev/docs/latest/skills) for discovery locations and command names.

User-level commands and skills are available in every project. Project-level Pi commands are
working-directory-specific; until command inventory is refreshed per thread, a newly opened project
may require a provider-status refresh before its project-local entries appear.

## Current limitations

Pi support is still being completed. Model discovery, authentication reporting, and command
discovery depend on the Pi process that T3 Code starts. If that process cannot return its available
models or commands, T3 Code cannot safely infer them from Pi's full built-in catalog.

Custom model entries are a local fallback, not proof that Pi can use those models. Verify the model
in Pi before adding it to T3 Code settings.
