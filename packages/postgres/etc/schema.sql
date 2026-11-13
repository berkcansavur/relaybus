CREATE TABLE IF NOT EXISTS "public"."relaybus_events" (
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
    WHERE attrelid = to_regclass('"public"."relaybus_events"') AND attname IN ('payload', 'headers')
      AND atttypid = 'jsonb'::regtype AND NOT attisdropped
  ) THEN
    ALTER TABLE "public"."relaybus_events"
      ALTER COLUMN headers DROP DEFAULT,
      ALTER COLUMN payload TYPE json USING payload::json,
      ALTER COLUMN headers TYPE json USING headers::json,
      ALTER COLUMN headers SET DEFAULT '{}'::json;
  END IF;
END
$relaybus_payload_json$;

CREATE INDEX IF NOT EXISTS "relaybus_events_created_idx" ON "public"."relaybus_events" (created_at, id);

CREATE TABLE IF NOT EXISTS "public"."relaybus_key_sequences" (
  key text PRIMARY KEY,
  sequence bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS "public"."relaybus_deliveries" (
  id text COLLATE "C" PRIMARY KEY,
  event_id text NOT NULL REFERENCES "public"."relaybus_events" (id) ON DELETE CASCADE,
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
  CONSTRAINT "relaybus_deliveries_event_handler_unique" UNIQUE (event_id, handler)
) WITH (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05);

DO $relaybus_upgrade$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = '"public"."relaybus_deliveries"'::regclass AND attname = 'lease_token' AND NOT attisdropped
  ) THEN
    ALTER TABLE "public"."relaybus_events" ADD COLUMN IF NOT EXISTS handlers text[] NOT NULL DEFAULT '{}';
    UPDATE "public"."relaybus_events" AS event SET handlers = known.handlers
    FROM (
      SELECT event_id, array_agg(handler) AS handlers FROM "public"."relaybus_deliveries" GROUP BY event_id
    ) AS known
    WHERE event.id = known.event_id;
    ALTER TABLE "public"."relaybus_deliveries" ADD COLUMN IF NOT EXISTS lane smallint;
    UPDATE "public"."relaybus_deliveries" SET lane = (hashtextextended(COALESCE(key, event_id), 0) & 255)::smallint WHERE lane IS NULL;
    ALTER TABLE "public"."relaybus_deliveries" ALTER COLUMN lane SET NOT NULL;
    UPDATE "public"."relaybus_deliveries" SET status = 'pending' WHERE status = 'processing';
    ALTER TABLE "public"."relaybus_deliveries"
      DROP COLUMN lease_owner, DROP COLUMN lease_token, DROP COLUMN lease_expires_at;
  END IF;
END
$relaybus_upgrade$;

ALTER TABLE "public"."relaybus_deliveries" ADD COLUMN IF NOT EXISTS claims integer NOT NULL DEFAULT 0;

ALTER TABLE "public"."relaybus_deliveries" SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05);

DROP INDEX IF EXISTS "public"."relaybus_deliveries_due_idx";

DROP INDEX IF EXISTS "public"."relaybus_deliveries_due_unkeyed_idx";

DROP INDEX IF EXISTS "public"."relaybus_deliveries_handler_created_idx";

DROP INDEX IF EXISTS "public"."relaybus_deliveries_key_unfinished_idx";

DROP INDEX IF EXISTS "public"."relaybus_deliveries_key_processing_idx";

DROP INDEX IF EXISTS "public"."relaybus_deliveries_status_created_idx";

CREATE INDEX IF NOT EXISTS "relaybus_deliveries_lane_due_idx" ON "public"."relaybus_deliveries" (lane, available_at, id) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS "relaybus_deliveries_key_pending_idx" ON "public"."relaybus_deliveries" (lane, handler, key, sequence) WHERE key IS NOT NULL AND status = 'pending';

CREATE INDEX IF NOT EXISTS "relaybus_deliveries_completed_idx" ON "public"."relaybus_deliveries" (completed_at, id) WHERE status = 'completed';

CREATE INDEX IF NOT EXISTS "relaybus_deliveries_dead_idx" ON "public"."relaybus_deliveries" (updated_at, id) WHERE status = 'dead';

CREATE TABLE IF NOT EXISTS "public"."relaybus_lanes" (
  lane smallint PRIMARY KEY CHECK (lane >= 0 AND lane < 256),
  epoch bigint NOT NULL DEFAULT 0,
  lease_owner text,
  lease_expires_at timestamptz,
  in_flight text[] NOT NULL DEFAULT '{}',
  claim_floor bigint NOT NULL DEFAULT 0
) WITH (fillfactor = 50);

ALTER TABLE "public"."relaybus_lanes" ADD COLUMN IF NOT EXISTS claim_floor bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS "public"."relaybus_event_completions" (
  event_id text PRIMARY KEY REFERENCES "public"."relaybus_events" (id) ON DELETE CASCADE,
  completed_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS "relaybus_event_completions_completed_idx" ON "public"."relaybus_event_completions" (completed_at, event_id);

INSERT INTO "public"."relaybus_lanes" (lane) SELECT generate_series(0, 255)::smallint ON CONFLICT (lane) DO NOTHING;

COMMENT ON TABLE "public"."relaybus_lanes" IS 'relaybus schema ae44fe2cd55d06183921bc5e2bb0db92';
