export function formatComposerSkillInsertion(input: {
  readonly providerDriver: string | null | undefined;
  readonly skillName: string;
}): string {
  if (input.providerDriver === "pi") {
    return `/skill:${input.skillName} `;
  }
  return `$${input.skillName} `;
}
