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

Models that Pi marks as reasoning-capable show a **Reasoning** selector. **Pi default** keeps the
level configured by Pi and does not overwrite it. Explicit levels are checked against Pi's
session-scoped available-level query before T3 applies them. Pi's model map can disable a level with
`null`; omitted standard levels through High keep Pi's default mapping, while XHigh and Max require
explicit support.

One Pi card in T3 Code represents one Pi CLI setup. That setup can expose models from several Pi
providers, so the number of Pi cards is not the number of model providers in Pi.

The provider card reports the number of upstream providers Pi says are ready. Readiness checks run
with that Pi provider instance's environment, so credentials injected through environment variables
work without a matching global credential-file entry. If a readiness check cannot complete, T3
keeps the affected models visible and reports authentication status as unknown instead of removing
models that may still work.

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
working-directory-specific. T3 runs discovery from the server workspace root, matching the working
directory used when that environment starts Pi.

## Extension prompts and current limitations

Pi extensions can ask selection, confirmation, text, and editor questions. T3 shows those prompts in
the chat composer on web, desktop, and mobile, then returns the answer to the extension. Extension
notifications and failures appear in thread activity.

Model discovery, authentication reporting, and command discovery depend on the Pi process that T3
Code starts. Custom model entries remain a local fallback, not proof that Pi can use those models.
Verify a custom model in Pi before adding it to T3 Code settings.
