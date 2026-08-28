import { describe, expect, it } from "vite-plus/test";

import { formatComposerSkillInsertion } from "./composerSkillCommand";

describe("formatComposerSkillInsertion", () => {
  it("uses Pi's slash skill command", () => {
    expect(formatComposerSkillInsertion({ providerDriver: "pi", skillName: "browser" })).toBe(
      "/skill:browser ",
    );
  });

  it("keeps dollar skill commands for other providers", () => {
    expect(formatComposerSkillInsertion({ providerDriver: "codex", skillName: "browser" })).toBe(
      "$browser ",
    );
  });

  it("keeps dollar skill commands when no provider is selected", () => {
    expect(formatComposerSkillInsertion({ providerDriver: null, skillName: "browser" })).toBe(
      "$browser ",
    );
  });
});
