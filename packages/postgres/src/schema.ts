import { createHash } from 'node:crypto';
import {
  LANE_COUNT,
  resolvePostgresNames,
  type PostgresNames,
  type PostgresSchemaOptions,
} from './names.js';
import { laneExpression } from './sql-fragments.js';

/** Hex digits of the statement hash in the schema marker. */
const SCHEMA_MARKER_DIGITS = 32;

/** The payload and headers columns of the event table, and as 0.4.0 and earlier created them. */
const PAYLOAD_COLUMNS = `  payload json NOT NULL,
  headers json NOT NULL DEFAULT '{}'::json,`;
const JSONB_PAYLOAD_COLUMNS = `  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,`;

/**
 * Statements that create the schema, tables and indexes, in dependency order,
 * and upgrade a 0.1.0 schema and the `jsonb` payload columns of schemas before 0.4.1 in place.
 * Every statement is idempotent (`IF NOT EXISTS`, or guarded by a check for the old columns). Run them in
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
  const statements = [...eventAndDeliveryStatements(names), ...laneAndCompletionStatements(names)];
  if (names.completionHistory) {
    statements.push(...completionHistoryStatements(names));
  }
  if (names.schema !== 'public') {
    statements.unshift(`CREATE SCHEMA IF NOT EXISTS ${names.quotedSchema}`);
  }
  statements.push(`COMMENT ON TABLE ${names.lanes} IS '${postgresSchemaMarker(statements)}'`);
  return statements;
}

/** The event, key sequence and delivery tables, their upgrades and indexes. */
function eventAndDeliveryStatements(names: PostgresNames): string[] {
  return [...eventAndKeySequenceStatements(names), ...deliveryStatements(names)];
}

/** The event table with its index, and the key sequence table. */
function eventAndKeySequenceStatements(names: PostgresNames): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${names.events} (
  id text PRIMARY KEY,
  type text NOT NULL,
  source text NOT NULL,
  key text,
  sequence bigint,
${PAYLOAD_COLUMNS}
  correlation_id text,
  causation_id text,
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  handlers text[] NOT NULL DEFAULT '{}'
)`,
    upgradePayloadToJson(names),
    `CREATE INDEX IF NOT EXISTS ${names.indexes.eventCreated} ON ${names.events} (created_at, id)`,
    `CREATE TABLE IF NOT EXISTS ${names.keySequences} (
  key text PRIMARY KEY,
  sequence bigint NOT NULL
)`,
  ];
}

/** The delivery table, the 0.1.0 upgrade, the obsolete index drops and the current indexes. */
function deliveryStatements(names: PostgresNames): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${names.deliveries} (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES ${names.events} (id) ON DELETE CASCADE,
  handler text NOT NULL,
  lane smallint NOT NULL,
  claims integer NOT NULL DEFAULT 0,
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
    `ALTER TABLE ${names.deliveries} ADD COLUMN IF NOT EXISTS claims integer NOT NULL DEFAULT 0`,
    `ALTER TABLE ${names.deliveries} SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05)`,
    ...names.indexes.obsolete.map((index) => `DROP INDEX IF EXISTS ${names.quotedSchema}.${index}`),
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryLaneDue} ON ${names.deliveries} (lane, available_at, id) WHERE status = 'pending'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryKeyPending} ON ${names.deliveries} (lane, handler, key, sequence) WHERE key IS NOT NULL AND status = 'pending'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryCompleted} ON ${names.deliveries} (completed_at, id) WHERE status = 'completed'`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.deliveryDead} ON ${names.deliveries} (updated_at, id) WHERE status = 'dead'`,
  ];
}

/** The lane and event completion tables, and the lane rows. */
function laneAndCompletionStatements(names: PostgresNames): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${names.lanes} (
  lane smallint PRIMARY KEY CHECK (lane >= 0 AND lane < ${LANE_COUNT}),
  epoch bigint NOT NULL DEFAULT 0,
  lease_owner text,
  lease_expires_at timestamptz,
  in_flight text[] NOT NULL DEFAULT '{}',
  claim_floor bigint NOT NULL DEFAULT 0
) WITH (fillfactor = 50)`,
    `ALTER TABLE ${names.lanes} ADD COLUMN IF NOT EXISTS claim_floor bigint NOT NULL DEFAULT 0`,
    `CREATE TABLE IF NOT EXISTS ${names.eventCompletions} (
  event_id text PRIMARY KEY REFERENCES ${names.events} (id) ON DELETE CASCADE,
  completed_at timestamptz NOT NULL
)`,
    `CREATE INDEX IF NOT EXISTS ${names.indexes.eventCompletionsCompleted} ON ${names.eventCompletions} (completed_at, event_id)`,
    `INSERT INTO ${names.lanes} (lane) SELECT generate_series(0, ${LANE_COUNT - 1})::smallint ON CONFLICT (lane) DO NOTHING`,
  ];
}

/** The completion history table, kept with `completionHistory: true`. */
function completionHistoryStatements(names: PostgresNames): string[] {
  return [
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
  ];
}

/**
 * The comment the last schema statement leaves on the lane table: a hash of every statement
 * before it. {@link PostgresEventStore.setup} reads it to skip the statements when the schema is
 * already current, because their table locks queue behind running claims and completions and
 * deadlock with them.
 */
export function postgresSchemaMarker(statements: readonly string[]): string {
  return `relaybus schema ${createHash('sha256').update(statements.join(';')).digest('hex').slice(0, SCHEMA_MARKER_DIGITS)}`;
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
 * Turns the `jsonb` payload and headers columns of a schema created before 0.4.1 into `json`, which
 * keeps the text the producer serialized: `jsonb` re-orders object keys, so an event read back
 * serialized differently from what was published. One table rewrite under an exclusive lock; rows
 * stored before the upgrade keep the key order `jsonb` gave them. A no-op once both columns are
 * `json`, and when the table does not exist yet. {@link PostgresEventStore.setup} runs it in a
 * transaction of its own before the other statements, so it holds no other table lock while it
 * waits for the event table (a completion locks deliveries before the event rows it references).
 */
export function upgradePayloadToJson(names: PostgresNames): string {
  return `DO $relaybus_payload_json$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('${names.events}') AND attname IN ('payload', 'headers')
      AND atttypid = 'jsonb'::regtype AND NOT attisdropped
  ) THEN
    ALTER TABLE ${names.events}
      ALTER COLUMN headers DROP DEFAULT,
      ALTER COLUMN payload TYPE json USING payload::json,
      ALTER COLUMN headers TYPE json USING headers::json,
      ALTER COLUMN headers SET DEFAULT '{}'::json;
  END IF;
END
$relaybus_payload_json$`;
}

/**
 * The schema marker 0.4.0 left on the lane table: the hash of today's statements without the
 * payload conversion and with the `jsonb` columns. A schema that carries it needs only the
 * conversion, so {@link PostgresEventStore.setup} converts it and moves the marker on without
 * re-running the other schema statements, whose table locks could deadlock with running claims.
 */
export function version040SchemaMarker(options: PostgresSchemaOptions = {}): string {
  const conversion = upgradePayloadToJson(resolvePostgresNames(options));
  return postgresSchemaMarker(
    createPostgresSchemaStatements(options)
      .slice(0, -1)
      .filter((statement) => statement !== conversion)
      .map((statement) => statement.replace(PAYLOAD_COLUMNS, JSONB_PAYLOAD_COLUMNS)),
  );
}

/**
 * The statements of {@link createPostgresSchemaStatements} as one SQL script,
 * for migration tools. Pair it with `new RelayBus({ setupStore: false })`.
 */
export function createPostgresSchemaSql(options: PostgresSchemaOptions = {}): string {
  return `${createPostgresSchemaStatements(options).join(';\n\n')};\n`;
}
