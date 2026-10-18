import { resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { dueAtExpression } from './sql-fragments.js';

/**
 * Statements that create the schema, tables and indexes, in dependency order.
 * Every statement is idempotent (`IF NOT EXISTS`). Run them in one transaction
 * and serialize concurrent runs (see {@link PostgresEventStore.setup}), or
 * paste {@link createPostgresSchemaSql} into your own migration tool.
 *
 * `events.id` and `deliveries.id` are `text` so that any caller-supplied id
 * (UUID, ULID, deterministic business id) is accepted. `deliveries.id` uses the
 * `C` collation so that ordering by id matches a byte-wise comparison on every
 * database, whatever its locale.
 */
export function createPostgresSchemaStatements(options: PostgresSchemaOptions = {}): string[] {
  const names = resolvePostgresNames(options);
  const unfinished = `status IN ('pending', 'processing')`;
  const statements = [
    `CREATE TABLE IF NOT EXISTS ${names.events} (
  id text PRIMARY KEY,
  type text NOT NULL,
  source text NOT NULL,
  key text,
  sequence bigint,
  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  correlation_id text,
  causation_id text,
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL
)`,
    `CREATE TABLE IF NOT EXISTS ${names.keySequences} (
  key text PRIMARY KEY,
  sequence bigint NOT NULL
)`,
    `CREATE TABLE IF NOT EXISTS ${names.deliveries} (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES ${names.events} (id) ON DELETE CASCADE,
  handler text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'dead')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  last_error jsonb,
  key text,
  sequence bigint,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  completed_at timestamptz,
  CONSTRAINT ${names.indexes.deliveryEventHandler} UNIQUE (event_id, handler)
)`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryDue} ON ${names.deliveries} ((${dueAtExpression()}), id) WHERE ${unfinished}`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryKeyUnfinished} ON ${names.deliveries} (handler, key, sequence) WHERE key IS NOT NULL AND ${unfinished}`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryKeyProcessing} ON ${names.deliveries} (handler, key) WHERE key IS NOT NULL AND status = 'processing'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryStatusCreated} ON ${names.deliveries} (status, created_at, id)`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryCompleted} ON ${names.deliveries} (completed_at, id) WHERE status = 'completed'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryDead} ON ${names.deliveries} (updated_at, id) WHERE status = 'dead'`,
  ];
  if (names.schema !== 'public') {
    statements.unshift(`CREATE SCHEMA IF NOT EXISTS ${names.quotedSchema}`);
  }
  return statements;
}

/**
 * The statements of {@link createPostgresSchemaStatements} as one SQL script,
 * for migration tools. Pair it with `new RelayBus({ setupStore: false })`.
 */
export function createPostgresSchemaSql(options: PostgresSchemaOptions = {}): string {
  return `${createPostgresSchemaStatements(options).join(';\n\n')};\n`;
}
