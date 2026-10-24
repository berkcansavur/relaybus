import type { Pool } from 'pg';

/**
 * The PostgreSQL schema `@relaybus/postgres` 0.1.0 created, kept verbatim so
 * that the upgrade test starts from what real installations have.
 */
export async function createVersion010Schema(pool: Pool, schema: string): Promise<void> {
  const q = (name: string): string => `"${schema}"."relaybus_${name}"`;
  const unfinished = `status IN ('pending', 'processing')`;
  const due = `CASE WHEN status = 'pending' THEN available_at ELSE lease_expires_at END`;
  const statements = [
    `CREATE SCHEMA IF NOT EXISTS "${schema}"`,
    `CREATE TABLE ${q('events')} (
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
    `CREATE INDEX relaybus_events_created_idx ON ${q('events')} (created_at, id)`,
    `CREATE TABLE ${q('key_sequences')} (key text PRIMARY KEY, sequence bigint NOT NULL)`,
    `CREATE TABLE ${q('deliveries')} (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES ${q('events')} (id) ON DELETE CASCADE,
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
  CONSTRAINT relaybus_deliveries_event_handler_unique UNIQUE (event_id, handler)
)`,
    `CREATE INDEX relaybus_deliveries_due_idx ON ${q('deliveries')} ((${due}), id) WHERE ${unfinished}`,
    `CREATE INDEX relaybus_deliveries_due_unkeyed_idx ON ${q('deliveries')} ((${due}), id) WHERE key IS NULL AND ${unfinished}`,
    `CREATE INDEX relaybus_deliveries_key_unfinished_idx ON ${q('deliveries')} (handler, key, sequence) WHERE key IS NOT NULL AND ${unfinished}`,
    `CREATE INDEX relaybus_deliveries_key_processing_idx ON ${q('deliveries')} (handler, key) WHERE key IS NOT NULL AND status = 'processing'`,
    `CREATE INDEX relaybus_deliveries_status_created_idx ON ${q('deliveries')} (status, created_at, id)`,
    `CREATE INDEX relaybus_deliveries_handler_created_idx ON ${q('deliveries')} (handler, created_at, id)`,
    `CREATE INDEX relaybus_deliveries_completed_idx ON ${q('deliveries')} (completed_at, id) WHERE status = 'completed'`,
    `CREATE INDEX relaybus_deliveries_dead_idx ON ${q('deliveries')} (updated_at, id) WHERE status = 'dead'`,
  ];
  for (const statement of statements) {
    await pool.query(statement);
  }
}
