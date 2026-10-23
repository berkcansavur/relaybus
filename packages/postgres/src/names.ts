import { ConfigurationError } from '@relaybus/core';
import { createHash } from 'node:crypto';
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
  /**
   * Keep an append-only record of completed deliveries in
   * `<prefix>completed_deliveries`, so `listDeliveries({ status: 'completed' })`,
   * `statistics().completed` and `retention.completed` keep working. Default
   * `false`: a completed delivery is deleted, which avoids the dead tuples and
   * index churn of updating it. Use the same value in every process.
   */
  readonly completionHistory?: boolean;
}

/** Validated, quoted names used to build SQL. */
export interface PostgresNames {
  readonly schema: string;
  readonly quotedSchema: string;
  readonly events: string;
  readonly deliveries: string;
  readonly keySequences: string;
  /** Lane lease rows: one per lane, see {@link LANE_COUNT}. */
  readonly lanes: string;
  /** One row per event that had a delivery complete: when the last one did. Retention of completed work is measured from it. */
  readonly eventCompletions: string;
  /** Completion history, present only with `completionHistory`. */
  readonly completions: string;
  readonly completionHistory: boolean;
  /**
   * Channel of the notifications that wake dispatchers when deliveries were published:
   * `<schema>.<prefix>deliveries`, or a hash of it when that is longer than a channel name may be.
   */
  readonly notificationChannel: string;
  /** Index names, quoted but not schema-qualified (an index lives in its table's schema). */
  readonly indexes: {
    readonly deliveryEventHandler: string;
    readonly deliveryLaneDue: string;
    readonly deliveryKeyPending: string;
    readonly eventCreated: string;
    readonly deliveryCompleted: string;
    readonly deliveryDead: string;
    readonly completionCompleted: string;
    readonly eventCompletionsCompleted: string;
    /** Indexes of the 0.1.0 schema that the current schema drops. */
    readonly obsolete: readonly string[];
  };
}

/**
 * Number of lanes. Every delivery belongs to the lane of its ordering key (or of
 * its event id when the event has no key); workers lease whole lanes. The count
 * is fixed because the lane of a delivery is stored, and all processes must
 * agree on it.
 */
export const LANE_COUNT = 256;

const DEFAULT_SCHEMA = 'public';
const MAXIMUM_CHANNEL_LENGTH = 63;
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
    lanes: table('lanes'),
    eventCompletions: table('event_completions'),
    completions: table('completed_deliveries'),
    completionHistory: options.completionHistory ?? false,
    notificationChannel: channelName(`${schema}.${tablePrefix}deliveries`),
    indexes: {
      deliveryEventHandler: index('deliveries_event_handler_unique'),
      deliveryLaneDue: index('deliveries_lane_due_idx'),
      deliveryKeyPending: index('deliveries_key_pending_idx'),
      eventCreated: index('events_created_idx'),
      deliveryCompleted: index('deliveries_completed_idx'),
      deliveryDead: index('deliveries_dead_idx'),
      completionCompleted: index('completed_deliveries_completed_idx'),
      eventCompletionsCompleted: index('event_completions_completed_idx'),
      obsolete: [
        index('deliveries_due_idx'),
        index('deliveries_due_unkeyed_idx'),
        index('deliveries_handler_created_idx'),
        index('deliveries_key_unfinished_idx'),
        index('deliveries_key_processing_idx'),
        index('deliveries_status_created_idx'),
      ],
    },
  };
}

/** PostgreSQL truncates names longer than 63 bytes, which could merge channels: those are hashed instead. */
function channelName(readable: string): string {
  return readable.length <= MAXIMUM_CHANNEL_LENGTH
    ? readable
    : `relaybus_${createHash('sha256').update(readable).digest('hex').slice(0, 32)}`;
}
