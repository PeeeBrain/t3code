import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings, ProviderInstanceId } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { createModelSelection } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";

import { makePiTextGenerationService } from "./PiTextGeneration.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  Layer.succeed(HostProcessPlatform, "linux"),
);

function makeFakePiBinary(dir: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stubPath = path.join(dir, "pi-stub.mjs");
    const binPath = path.join(dir, "pi");
    yield* fs.writeFileString(
      stubPath,
      [
        "const args = process.argv.slice(2);",
        "let stdin = '';",
        "for await (const chunk of process.stdin) stdin += chunk;",
        "if (args.some((arg) => arg.includes('UNIQUE_STDIN_MARKER'))) process.exit(3);",
        "if (!stdin.includes('UNIQUE_STDIN_MARKER')) process.exit(4);",
        'process.stdout.write(JSON.stringify({ title: "From stdin" }));',
        "",
      ].join("\n"),
    );
    yield* fs.writeFileString(
      binPath,
      ["#!/bin/sh", 'exec node "$(dirname "$0")/pi-stub.mjs" "$@"', ""].join("\n"),
    );
    yield* fs.chmod(binPath, 0o755);
    return binPath;
  });
}

it.layer(testLayer)("PiTextGeneration", (it) => {
  it.effect("sends the prompt on stdin instead of argv", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-pi-text-" });
      const binaryPath = yield* makeFakePiBinary(dir);
      const textGeneration = makePiTextGenerationService(
        decodePiSettings({ enabled: true, binaryPath, customModels: [] }),
        process.env,
      );

      const generated = yield* textGeneration.generateThreadTitle({
        cwd: dir,
        message: "UNIQUE_STDIN_MARKER please title this",
        modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "ignored"),
      });

      expect(generated.title).toBe("From stdin");
    }).pipe(Effect.scoped),
  );
});
