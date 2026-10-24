import type { PostgresNames } from './names.js';
import { laneExpression } from './sql-fragments.js';

/** Statements that publish: events, deliveries and the wake-up notification. */
export class AppendStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
  }

  /**
   * Publishes in one statement: reserves the sequences of the keys (`[{ key, count }]` in `$4`, locked in
   * key order: each row lock is held until the publishing transaction ends, which makes sequence order equal commit order for a key), numbers the keyed events in entry order (`ordinal`), inserts
   * the events of `$1` that do not exist yet and the deliveries of `$3` that belong to them, and sends one
   * wake-up notification when a delivery it inserted is due at `$5`. Parameters:
   * `[eventsJson, createdAt, deliveriesJson, keyCountsJson, now]`. Returns one row per event of `$1`: its
   * id, its sequence, whether this statement inserted it, and (on every row) how many deliveries it
   * inserted. Deliveries of events that already existed are left to {@link insertDeliveries}.
   */
  get insertFreshEvents(): string {
    const events = this.#names.events;
    return `WITH reserved AS (
  INSERT INTO ${this.#names.keySequences} AS reserved (key, sequence)
  SELECT requested.key, requested.count
  FROM jsonb_to_recordset($4::jsonb) AS requested (key text, count bigint)
  ORDER BY requested.key
  ON CONFLICT (key) DO UPDATE SET sequence = reserved.sequence + EXCLUDED.sequence
  RETURNING reserved.key AS key, reserved.sequence AS sequence
), incoming AS (
  SELECT incoming.*,
    CASE WHEN incoming.key IS NULL THEN NULL ELSE
      reserved.sequence - count(*) OVER (PARTITION BY incoming.key)
        + row_number() OVER (PARTITION BY incoming.key ORDER BY incoming.ordinal)
    END AS sequence
  FROM jsonb_to_recordset($1::jsonb) AS incoming (
    ordinal int, id text, type text, source text, key text, payload jsonb, headers jsonb,
    correlation_id text, causation_id text, occurred_at timestamptz, handlers text[])
  LEFT JOIN reserved ON reserved.key = incoming.key
), inserted_events AS (
  INSERT INTO ${events}
    (id, type, source, key, sequence, payload, headers, correlation_id, causation_id, occurred_at, created_at, handlers)
  SELECT incoming.id, incoming.type, incoming.source, incoming.key, incoming.sequence,
    COALESCE(incoming.payload, 'null'::jsonb), COALESCE(incoming.headers, '{}'::jsonb),
    incoming.correlation_id, incoming.causation_id, incoming.occurred_at, $2::timestamptz,
    COALESCE(incoming.handlers, '{}')
  FROM incoming
  ORDER BY incoming.id
  ON CONFLICT (id) DO NOTHING
  RETURNING id, key, sequence
), inserted_deliveries AS (
  INSERT INTO ${this.#names.deliveries}
    (id, event_id, handler, lane, status, attempts, available_at, key, sequence, created_at, updated_at)
  SELECT delivery.id, delivery.event_id, delivery.handler, ${laneExpression('event.key', 'event.id')}, 'pending', 0,
    delivery.available_at, event.key, event.sequence, $2::timestamptz, $2::timestamptz
  FROM jsonb_to_recordset($3::jsonb) AS delivery (id text, event_id text, handler text, available_at timestamptz)
  JOIN inserted_events AS event ON event.id = delivery.event_id
  ORDER BY delivery.event_id, delivery.handler
  ON CONFLICT (event_id, handler) DO NOTHING
  RETURNING available_at
), ${this.#wakeUp('inserted_deliveries', '$5')}
SELECT incoming.id, incoming.sequence::text AS sequence,
  incoming.id IN (SELECT inserted_events.id FROM inserted_events) AS inserted,
  (SELECT count(*) FROM inserted_deliveries)::int AS deliveries
FROM incoming
WHERE (SELECT count(*) FROM woken) >= 0`;
  }

  /** One wake-up notification on {@link PostgresNames.notificationChannel}, outside any publishing transaction. */
  get notify(): string {
    return `SELECT pg_notify('${this.#names.notificationChannel}', '')`;
  }

  /**
   * A CTE `woken` that sends one notification on {@link PostgresNames.notificationChannel} when a row of
   * `inserted` is due at `now`. PostgreSQL delivers it when the transaction commits, merges the same
   * notification sent twice in one transaction, and drops it on rollback. The statement must reference
   * `woken` for it to run.
   */
  #wakeUp(inserted: string, now: string): string {
    return `woken AS (
  SELECT pg_notify('${this.#names.notificationChannel}', '') FROM (
    SELECT 1 FROM ${inserted} WHERE ${inserted}.available_at <= ${now}::timestamptz LIMIT 1
  ) AS due
)`;
  }

  /**
   * Parameters: `[eventsJson, createdAt]`. Returns the ids that were inserted.
   * `handlers` is the list of handlers the event gets deliveries for: it
   * outlives the deliveries, which are deleted when they complete.
   */
  get insertEvents(): string {
    return `INSERT INTO ${this.#names.events}
  (id, type, source, key, sequence, payload, headers, correlation_id, causation_id, occurred_at, created_at, handlers)
SELECT incoming.id, incoming.type, incoming.source, incoming.key, incoming.sequence,
  COALESCE(incoming.payload, 'null'::jsonb), COALESCE(incoming.headers, '{}'::jsonb),
  incoming.correlation_id, incoming.causation_id, incoming.occurred_at, $2::timestamptz,
  COALESCE(incoming.handlers, '{}')
FROM jsonb_to_recordset($1::jsonb) AS incoming (
  id text, type text, source text, key text, sequence bigint, payload jsonb, headers jsonb,
  correlation_id text, causation_id text, occurred_at timestamptz, handlers text[])
ORDER BY incoming.id
ON CONFLICT (id) DO NOTHING
RETURNING id`;
  }

  /**
   * Locks events that were not inserted by the current append, in id order, so
   * that appends adding handlers to the same event run one after the other.
   * Parameters: `[eventIdsJson]`. `FOR NO KEY UPDATE` conflicts with itself and
   * not with the foreign key checks of other inserts.
   */
  get lockExistingEvents(): string {
    return `SELECT id FROM ${this.#names.events}
WHERE id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
ORDER BY id
FOR NO KEY UPDATE`;
  }

  /**
   * Parameters: `[deliveriesJson, createdAt, insertedEventIdsJson, now]`. Key and
   * sequence are copied from the stored event, so a delivery added later to an
   * existing event keeps the event's position in its key and lands in the same
   * lane. Returns the ids that were inserted.
   *
   * The deliveries of events this call just inserted are all new. For an event
   * that already existed, a handler gets a delivery only if the event does not
   * list it yet: a completed delivery is deleted, but its handler stays listed,
   * so a redelivered or republished event never runs a handler twice. The
   * handlers added to an existing event are appended to its list.
   */
  get insertDeliveries(): string {
    const events = this.#names.events;
    const insertedEvents = `ARRAY(SELECT jsonb_array_elements_text($3::jsonb))`;
    return `WITH incoming AS (
  SELECT incoming.id, incoming.event_id, incoming.handler, incoming.available_at
  FROM jsonb_to_recordset($1::jsonb) AS incoming (id text, event_id text, handler text, available_at timestamptz)
), fresh AS (
  SELECT incoming.*, event.key, event.sequence, ${laneExpression('event.key', 'event.id')} AS lane
  FROM incoming
  JOIN ${events} AS event ON event.id = incoming.event_id
  WHERE event.id = ANY (${insertedEvents}) OR NOT incoming.handler = ANY (event.handlers)
), inserted AS (
  INSERT INTO ${this.#names.deliveries}
    (id, event_id, handler, lane, status, attempts, available_at, key, sequence, created_at, updated_at)
  SELECT fresh.id, fresh.event_id, fresh.handler, fresh.lane, 'pending', 0, fresh.available_at,
    fresh.key, fresh.sequence, $2::timestamptz, $2::timestamptz
  FROM fresh
  ORDER BY fresh.event_id, fresh.handler
  ON CONFLICT (event_id, handler) DO NOTHING
  RETURNING id, event_id, handler, available_at
), extended AS (
  UPDATE ${events} AS event SET handlers = event.handlers || added.handlers
  FROM (
    SELECT inserted.event_id, array_agg(inserted.handler) AS handlers FROM inserted
    WHERE inserted.event_id <> ALL (${insertedEvents}) GROUP BY inserted.event_id
  ) AS added
  WHERE event.id = added.event_id
  RETURNING event.id
), ${this.#wakeUp('inserted', '$4')}
SELECT id FROM inserted WHERE (SELECT count(*) FROM woken) >= 0`;
  }
}
