import { ConfigurationError } from '@relaybus/core';

const COLLECTION_PREFIX_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,39}$/;
const DATABASE_NAME_PATTERN = /^[A-Za-z0-9_-]{1,63}$/;

/** Rejects prefixes that are not safe, portable collection name starts. */
export function assertCollectionPrefix(prefix: string): void {
  if (!COLLECTION_PREFIX_PATTERN.test(prefix)) {
    throw new ConfigurationError(
      `collectionPrefix '${prefix}' must be 1-40 characters of letters, digits or '_', starting with a letter or '_'`,
    );
  }
}

/** Rejects database names MongoDB would refuse or that need escaping. */
export function assertDatabaseName(name: string): void {
  if (!DATABASE_NAME_PATTERN.test(name)) {
    throw new ConfigurationError(
      `database '${name}' must be 1-63 characters of letters, digits, '_' or '-'`,
    );
  }
}
