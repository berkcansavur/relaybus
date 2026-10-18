import { LANE_COUNT, resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { laneExpression } from './sql-fragments.js';

/**
 * Statements that create the schema, tables and indexes, in dependency order,
 * and upgrade a 0.1.0 schema in place. Every statement is idempotent
 * (`IF NOT EXISTS`, or guarded by a check for the 0.1.0 columns). Run them in
 * one transaction and serialize concurrent runs (see
 * {@link PostgresEventStore.setup}), or paste {@link createPostgresSchemaSql}
 * into your own migration tool.
 *
 * `events.id` and `deliveries.id` are `text` so that any caller-supplied id
 * (UUID, ULID, deterministic business id) is accepted. `deliveries.id` uses the
 * `C` collation so that ordering by id matches a byte-wise comparison on every
 * database, whatever its locale.
 *
 * A delivery row is written once (insert) and removed once (delete) on the
 * normal path. The lease lives in the lane row a delivery belongs to, so
 * claiming, renewing and completing never update an indexed column of the
 * delivery. The lane table is tiny, has no index besides its primary key and
 * leaves free space in its pages, so its updates are heap-only (HOT).
 */
export function createPostgresSchemaStatements(options: PostgresSchemaOptions = {}): string[] {
  const names = resolvePostgresNames(options);
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
  created_at timestamptz NOT NULL,
  handlers text[] NOT NULL DEFAULT '{}'
)`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.eventCreated} ON ${names.events} (created_at, id)`,
    `CREATE TABLE IF NOT EXISTS ${names.keySequences} (
  key text PRIMARY KEY,
  sequence bigint NOT NULL
)`,
    `CREATE TABLE IF NOT EXISTS ${names.deliveries} (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES ${names.events} (id) ON DELETE CASCADE,
  handler text NOT NULL,
  lane smallint NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'dead')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL,
  last_error jsonb,
  key text,
  sequence bigint,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  completed_at timestamptz,
  CONSTRAINT ${names.indexes.deliveryEventHandler} UNIQUE (event_id, handler)
) WITH (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05)`,
    upgradeFromVersion010(names),
    `ALTER TABLE ${names.deliveries} SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05)`,
    ...names.indexes.obsolete.map((index) => `DROP INDEX IF EXISTS ${names.quotedSchema}.${index}`),
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryLaneDue} ON ${names.deliveries} (lane, available_at, id) WHERE status = 'pending'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryKeyPending} ON ${names.deliveries} (lane, handler, key, sequence) WHERE key IS NOT NULL AND status = 'pending'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryCompleted} ON ${names.deliveries} (completed_at, id) WHERE status = 'completed'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryDead} ON ${names.deliveries} (updated_at, id) WHERE status = 'dead'`,
    `CREATE TABLE IF NOT EXISTS ${names.lanes} (
  lane smallint PRIMARY KEY CHECK (lane >= 0 AND lane < ${LANE_COUNT}),
  epoch bigint NOT NULL DEFAULT 0,
  lease_owner text,
  lease_expires_at timestamptz,
  in_flight text[] NOT NULL DEFAULT '{}'
) WITH (fillfactor = 50)`,
    `INSERT INTO ${names.lanes} (lane) SELECT generate_series(0, ${LANE_COUNT - 1})::smallint ON CONFLICT (lane) DO NOTHING`,
  ];
  if (names.completionHistory) {
    statements.push(
      `CREATE TABLE IF NOT EXISTS ${names.completions} (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES ${names.events} (id) ON DELETE CASCADE,
  handler text NOT NULL,
  attempts integer NOT NULL,
  available_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  completed_at timestamptz NOT NULL
)`,
      `CREATE INDEX IF NOT EXISTS ${names.indexes.completionCompleted} ON ${names.completions} (completed_at, id)`,
    );
  }
  if (names.schema !== 'public') {
    statements.unshift(`CREATE SCHEMA IF NOT EXISTS ${names.quotedSchema}`);
  }
  return statements;
}

/**
 * Upgrades a 0.1.0 deliveries table, recognized by its `lease_token` column.
 * Rows keep their ids, attempts and payload references. Deliveries that were
 * `processing` become `pending` (their attempt stays counted, as a lost lease
 * would count it), every row gets its lane, every event learns which handlers
 * it already has deliveries for, and the per-delivery lease columns go. Deliveries completed under 0.1.0 stay as `completed` rows until
 * `retention.completed` purges them. Stop all 0.1.0 workers before the first
 * run: they cannot use the upgraded tables.
 */
function upgradeFromVersion010(names: ReturnType<typeof resolvePostgresNames>): string {
  return `DO $relaybus_upgrade$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = '${names.deliveries}'::regclass AND attname = 'lease_token' AND NOT attisdropped
  ) THEN
    ALTER TABLE ${names.events} ADD COLUMN IF NOT EXISTS handlers text[] NOT NULL DEFAULT '{}';
    UPDATE ${names.events} AS event SET handlers = known.handlers
    FROM (
      SELECT event_id, array_agg(handler) AS handlers FROM ${names.deliveries} GROUP BY event_id
    ) AS known
    WHERE event.id = known.event_id;
    ALTER TABLE ${names.deliveries} ADD COLUMN IF NOT EXISTS lane smallint;
    UPDATE ${names.deliveries} SET lane = ${laneExpression('key', 'event_id')} WHERE lane IS NULL;
    ALTER TABLE ${names.deliveries} ALTER COLUMN lane SET NOT NULL;
    UPDATE ${names.deliveries} SET status = 'pending' WHERE status = 'processing';
    ALTER TABLE ${names.deliveries}
      DROP COLUMN lease_owner, DROP COLUMN lease_token, DROP COLUMN lease_expires_at;
  END IF;
END
$relaybus_upgrade$`;
}

/**
 * The statements of {@link createPostgresSchemaStatements} as one SQL script,
 * for migration tools. Pair it with `new RelayBus({ setupStore: false })`.
 */
export function createPostgresSchemaSql(options: PostgresSchemaOptions = {}): string {
  return `${createPostgresSchemaStatements(options).join(';\n\n')};\n`;
}
