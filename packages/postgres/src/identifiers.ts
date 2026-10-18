import { ConfigurationError } from '@relaybus/core';

const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;
const MAXIMUM_IDENTIFIER_LENGTH = 63;

/**
 * Throws {@link ConfigurationError} unless `value` is a plain lower-case
 * PostgreSQL identifier. Identifiers are interpolated into SQL text (they
 * cannot be bound as parameters), so this is the injection boundary: only
 * letters, digits and underscores survive, and nothing needs escaping.
 */
export function assertIdentifier(label: string, value: string): void {
  if (!IDENTIFIER_PATTERN.test(value) || value.length > MAXIMUM_IDENTIFIER_LENGTH) {
    throw new ConfigurationError(
      `${label} '${value}' must match ${String(IDENTIFIER_PATTERN)} and be at most ${MAXIMUM_IDENTIFIER_LENGTH} characters`,
    );
  }
}

/** Double-quotes an identifier that already passed {@link assertIdentifier}. */
export function quoteIdentifier(value: string): string {
  return `"${value}"`;
}
