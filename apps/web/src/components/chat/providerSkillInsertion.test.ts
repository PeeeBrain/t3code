import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind } from "@t3tools/contracts";

import { formatProviderSkillInsertion } from "./providerSkillInsertion";

describe("formatProviderSkillInsertion", () => {
  it("uses Pi's native skill command while keeping the trailing boundary to the caller", () => {
    expect(formatProviderSkillInsertion(ProviderDriverKind.make("pi"), "browser")).toBe(
      "/skill:browser",
    );
  });

  it("keeps the dollar syntax for non-Pi providers", () => {
    expect(formatProviderSkillInsertion(ProviderDriverKind.make("claudeAgent"), "browser")).toBe(
      "$browser",
    );
  });
});
