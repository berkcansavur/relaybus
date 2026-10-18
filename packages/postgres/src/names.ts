import { ConfigurationError } from '@relaybus/core';
import { assertIdentifier, quoteIdentifier } from './identifiers.js';

/** Where relaybus keeps its tables. */
export interface PostgresSchemaOptions {
  /** Schema that holds the tables. Created by `setup()` when it is not `public`. Default `'public'`. */
  readonly schema?: string;
  /**
   * Prepended to every table and index name, which makes the names
   * `<prefix>events`, `<prefix>deliveries` and `<prefix>key_sequences`.
   * Lower-case letters, digits and underscores. Default `'relaybus_'`.
   */
  readonly tablePrefix?: string;
}

/** Validated, quoted names used to build SQL. */
export interface PostgresNames {
  readonly schema: string;
  readonly quotedSchema: string;
  readonly events: string;
  readonly deliveries: string;
  readonly keySequences: string;
  /** Index names, quoted but not schema-qualified (an index lives in its table's schema). */
  readonly indexes: {
    readonly deliveryEventHandler: string;
    readonly deliveryDue: string;
    readonly deliveryKeyUnfinished: string;
    readonly deliveryKeyProcessing: string;
    readonly deliveryStatusCreated: string;
    readonly deliveryCompleted: string;
    readonly deliveryDead: string;
  };
}

const DEFAULT_SCHEMA = 'public';
const DEFAULT_TABLE_PREFIX = 'relaybus_';
const TABLE_PREFIX_PATTERN = /^[a-z0-9_]*$/;

/**
 * Validates the options and derives every name the store uses. Every name,
 * including the longest index name, is checked against the PostgreSQL 63
 * character limit so that a too-long prefix fails here and not as a
 * silently truncated, colliding name in the database.
 */
export function resolvePostgresNames(options: PostgresSchemaOptions = {}): PostgresNames {
  const schema = options.schema ?? DEFAULT_SCHEMA;
  const tablePrefix = options.tablePrefix ?? DEFAULT_TABLE_PREFIX;
  assertIdentifier('schema', schema);
  if (!TABLE_PREFIX_PATTERN.test(tablePrefix)) {
    throw new ConfigurationError(
      `tablePrefix '${tablePrefix}' must match ${String(TABLE_PREFIX_PATTERN)}`,
    );
  }
  const name = (suffix: string): string => {
    const identifier = `${tablePrefix}${suffix}`;
    assertIdentifier(`tablePrefix '${tablePrefix}' produces the name`, identifier);
    return identifier;
  };
  const quotedSchema = quoteIdentifier(schema);
  const table = (suffix: string): string => `${quotedSchema}.${quoteIdentifier(name(suffix))}`;
  const index = (suffix: string): string => quoteIdentifier(name(suffix));
  return {
    schema,
    quotedSchema,
    events: table('events'),
    deliveries: table('deliveries'),
    keySequences: table('key_sequences'),
    indexes: {
      deliveryEventHandler: index('deliveries_event_handler_unique'),
      deliveryDue: index('deliveries_due_idx'),
      deliveryKeyUnfinished: index('deliveries_key_unfinished_idx'),
      deliveryKeyProcessing: index('deliveries_key_processing_idx'),
      deliveryStatusCreated: index('deliveries_status_created_idx'),
      deliveryCompleted: index('deliveries_completed_idx'),
      deliveryDead: index('deliveries_dead_idx'),
    },
  };
}
