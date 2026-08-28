import {
  ProviderDriverKind,
  type ProviderDriverKind as ProviderDriverKindType,
} from "@t3tools/contracts";

const PI_PROVIDER_DRIVER = ProviderDriverKind.make("pi");

/**
 * Return the composer text for a selected provider skill.
 *
 * `$` remains the discovery trigger for every provider. Pi's runtime expects
 * skills as `/skill:name` commands, so Pi selections need to use that native
 * command form when they are inserted into the prompt.
 */
export function formatProviderSkillInsertion(
  provider: ProviderDriverKindType,
  skillName: string,
): string {
  return provider === PI_PROVIDER_DRIVER ? `/skill:${skillName}` : `$${skillName}`;
}
