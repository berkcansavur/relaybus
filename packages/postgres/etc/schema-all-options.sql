CREATE SCHEMA IF NOT EXISTS "events";

CREATE TABLE IF NOT EXISTS "events"."bus_events" (
  id text PRIMARY KEY,
  type text NOT NULL,
  source text NOT NULL,
  key text,
  sequence bigint,
  payload json NOT NULL,
  headers json NOT NULL DEFAULT '{}'::json,
  correlation_id text,
  causation_id text,
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  handlers text[] NOT NULL DEFAULT '{}'
);

DO $relaybus_payload_json$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('"events"."bus_events"') AND attname IN ('payload', 'headers')
      AND atttypid = 'jsonb'::regtype AND NOT attisdropped
  ) THEN
    ALTER TABLE "events"."bus_events"
      ALTER COLUMN headers DROP DEFAULT,
      ALTER COLUMN payload TYPE json USING payload::json,
      ALTER COLUMN headers TYPE json USING headers::json,
      ALTER COLUMN headers SET DEFAULT '{}'::json;
  END IF;
END
$relaybus_payload_json$;

CREATE INDEX IF NOT EXISTS "bus_events_created_idx" ON "events"."bus_events" (created_at, id);

CREATE TABLE IF NOT EXISTS "events"."bus_key_sequences" (
  key text PRIMARY KEY,
  sequence bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS "events"."bus_deliveries" (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES "events"."bus_events" (id) ON DELETE CASCADE,
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
  CONSTRAINT "bus_deliveries_event_handler_unique" UNIQUE (event_id, handler)
) WITH (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05);

DO $relaybus_upgrade$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = '"events"."bus_deliveries"'::regclass AND attname = 'lease_token' AND NOT attisdropped
  ) THEN
    ALTER TABLE "events"."bus_events" ADD COLUMN IF NOT EXISTS handlers text[] NOT NULL DEFAULT '{}';
    UPDATE "events"."bus_events" AS event SET handlers = known.handlers
    FROM (
      SELECT event_id, array_agg(handler) AS handlers FROM "events"."bus_deliveries" GROUP BY event_id
    ) AS known
    WHERE event.id = known.event_id;
    ALTER TABLE "events"."bus_deliveries" ADD COLUMN IF NOT EXISTS lane smallint;
    UPDATE "events"."bus_deliveries" SET lane = (hashtextextended(COALESCE(key, event_id), 0) & 255)::smallint WHERE lane IS NULL;
    ALTER TABLE "events"."bus_deliveries" ALTER COLUMN lane SET NOT NULL;
    UPDATE "events"."bus_deliveries" SET status = 'pending' WHERE status = 'processing';
    ALTER TABLE "events"."bus_deliveries"
      DROP COLUMN lease_owner, DROP COLUMN lease_token, DROP COLUMN lease_expires_at;
  END IF;
END
$relaybus_upgrade$;

ALTER TABLE "events"."bus_deliveries" ADD COLUMN IF NOT EXISTS claims integer NOT NULL DEFAULT 0;

ALTER TABLE "events"."bus_deliveries" SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05);

DROP INDEX IF EXISTS "events"."bus_deliveries_due_idx";

DROP INDEX IF EXISTS "events"."bus_deliveries_due_unkeyed_idx";

DROP INDEX IF EXISTS "events"."bus_deliveries_handler_created_idx";

DROP INDEX IF EXISTS "events"."bus_deliveries_key_unfinished_idx";

DROP INDEX IF EXISTS "events"."bus_deliveries_key_processing_idx";

DROP INDEX IF EXISTS "events"."bus_deliveries_status_created_idx";

CREATE INDEX IF NOT EXISTS "bus_deliveries_lane_due_idx" ON "events"."bus_deliveries" (lane, available_at, id) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS "bus_deliveries_key_pending_idx" ON "events"."bus_deliveries" (lane, handler, key, sequence) WHERE key IS NOT NULL AND status = 'pending';

CREATE INDEX IF NOT EXISTS "bus_deliveries_completed_idx" ON "events"."bus_deliveries" (completed_at, id) WHERE status = 'completed';

CREATE INDEX IF NOT EXISTS "bus_deliveries_dead_idx" ON "events"."bus_deliveries" (updated_at, id) WHERE status = 'dead';

CREATE TABLE IF NOT EXISTS "events"."bus_lanes" (
  lane smallint PRIMARY KEY CHECK (lane >= 0 AND lane < 256),
  epoch bigint NOT NULL DEFAULT 0,
  lease_owner text,
  lease_expires_at timestamptz,
  in_flight text[] NOT NULL DEFAULT '{}',
  claim_floor bigint NOT NULL DEFAULT 0
) WITH (fillfactor = 50);

ALTER TABLE "events"."bus_lanes" ADD COLUMN IF NOT EXISTS claim_floor bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS "events"."bus_event_completions" (
  event_id text PRIMARY KEY REFERENCES "events"."bus_events" (id) ON DELETE CASCADE,
  completed_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS "bus_event_completions_completed_idx" ON "events"."bus_event_completions" (completed_at, event_id);

INSERT INTO "events"."bus_lanes" (lane) SELECT generate_series(0, 255)::smallint ON CONFLICT (lane) DO NOTHING;

CREATE TABLE IF NOT EXISTS "events"."bus_completed_deliveries" (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES "events"."bus_events" (id) ON DELETE CASCADE,
  handler text NOT NULL,
  attempts integer NOT NULL,
  available_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  completed_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS "bus_completed_deliveries_completed_idx" ON "events"."bus_completed_deliveries" (completed_at, id);

COMMENT ON TABLE "events"."bus_lanes" IS 'relaybus schema d031f626f927d4d52cf1013eaf413339';
