import type { DeliveryQuery } from '@relaybus/core';
import type { PostgresNames } from './names.js';
import { dueAtExpression, isoText } from './sql-fragments.js';
import type { PostgresParameter } from './executor.js';

export interface PostgresStatement {
  readonly sql: string;
  readonly parameters: readonly PostgresParameter[];
}

const DEFAULT_LIST_LIMIT = 100;

function eventColumns(alias: string): string {
  return `${alias}.id AS event_id,
    ${alias}.type AS event_type,
    ${alias}.source AS event_source,
    ${alias}.key AS event_key,
    ${alias}.payload::text AS event_payload,
    ${alias}.headers::text AS event_headers,
    ${alias}.correlation_id AS event_correlation_id,
    ${alias}.causation_id AS event_causation_id,
    ${isoText(`${alias}.occurred_at`)} AS event_occurred_at`;
}

/** `WITH changed AS (<dml> RETURNING 1) SELECT count(*)`: a top-level SELECT works the same on every driver. */
function countChanged(dml: string): string {
  return `WITH changed AS (${dml} RETURNING 1) SELECT count(*)::int AS count FROM changed`;
}

const CLEAR_LEASE = 'lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL';

/**
 * SQL text for every operation of the store. Statements take their data as
 * parameters; only validated identifiers are interpolated. Documents travel
 * as one JSON text parameter expanded server-side with `jsonb_to_recordset`,
 * which keeps the statements identical across drivers and avoids array
 * literal encoding differences.
 */
export class PostgresStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
  }

  /** Lock advisory key text for {@link setupLock}. */
  get setupLockKey(): string {
    return `relaybus:${this.#names.events}`;
  }

  get setupLock(): string {
    return 'SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))';
  }

  /**
   * Reserves sequence numbers per key. Parameters: `[{ key, count }]` as JSON.
   * Keys are locked in sorted order so concurrent publishers cannot deadlock;
   * each row lock is held until the publishing transaction ends, which makes
   * sequence order equal commit order for a key.
   */
  get reserveSequences(): string {
    return `INSERT INTO ${this.#names.keySequences} AS reserved (key, sequence)
SELECT requested.key, requested.count
FROM jsonb_to_recordset($1::jsonb) AS requested (key text, count bigint)
ORDER BY requested.key
ON CONFLICT (key) DO UPDATE SET sequence = reserved.sequence + EXCLUDED.sequence
RETURNING reserved.key AS key, reserved.sequence::text AS sequence`;
  }

  /** Parameters: `[eventsJson, createdAt]`. Returns the ids that were inserted. */
  get insertEvents(): string {
    return `INSERT INTO ${this.#names.events}
  (id, type, source, key, sequence, payload, headers, correlation_id, causation_id, occurred_at, created_at)
SELECT incoming.id, incoming.type, incoming.source, incoming.key, incoming.sequence,
  COALESCE(incoming.payload, 'null'::jsonb), COALESCE(incoming.headers, '{}'::jsonb),
  incoming.correlation_id, incoming.causation_id, incoming.occurred_at, $2::timestamptz
FROM jsonb_to_recordset($1::jsonb) AS incoming (
  id text, type text, source text, key text, sequence bigint, payload jsonb, headers jsonb,
  correlation_id text, causation_id text, occurred_at timestamptz)
ORDER BY incoming.id
ON CONFLICT (id) DO NOTHING
RETURNING id`;
  }

  /**
   * Parameters: `[deliveriesJson, createdAt]`. Key and sequence are copied from
   * the stored event, so a delivery added later to an existing event keeps the
   * event's position in its key. Returns the ids that were inserted.
   */
  get insertDeliveries(): string {
    return `INSERT INTO ${this.#names.deliveries}
  (id, event_id, handler, status, attempts, available_at, key, sequence, created_at, updated_at)
SELECT incoming.id, incoming.event_id, incoming.handler, 'pending', 0, incoming.available_at,
  event.key, event.sequence, $2::timestamptz, $2::timestamptz
FROM jsonb_to_recordset($1::jsonb) AS incoming (id text, event_id text, handler text, available_at timestamptz)
JOIN ${this.#names.events} AS event ON event.id = incoming.event_id
ORDER BY incoming.event_id, incoming.handler
ON CONFLICT (event_id, handler) DO NOTHING
RETURNING id`;
  }

  /**
   * Leases due deliveries in one statement. Parameters:
   * `[now, handlersJson, limit, leaseOwner, leaseExpiresAt]`.
   *
   * Candidates are chosen with `FOR UPDATE SKIP LOCKED`, so two claimers never
   * lock the same row. A keyed delivery is only a candidate while
   * (a) no earlier delivery of the same handler and key is pending or
   * processing, and (b) no other delivery of that handler and key is
   * processing. Rule (a) is the ordering guarantee. Rule (b) protects the
   * rare case of a delivery that re-enters the key behind a running one
   * (requeue, or a handler added to an old event).
   *
   * Why SKIP LOCKED alone is enough for ordering: only the head of a key can
   * ever be a candidate, and the head is a single row. Two claimers racing
   * for it either skip it (the other holds the row lock) or, if the other
   * already committed, re-check the row's new version (now processing and not
   * due), which excludes it. Followers stay blocked because their predecessor
   * remains pending or processing in every snapshot until it completes or is
   * buried, and that commit happens-before the next claim that sees it.
   */
  get claim(): string {
    const deliveries = this.#names.deliveries;
    return `WITH candidates AS MATERIALIZED (
  SELECT delivery.id, ${dueAtExpression('delivery')} AS due_at
  FROM ${deliveries} AS delivery
  WHERE delivery.status IN ('pending', 'processing')
    AND ${dueAtExpression('delivery')} <= $1::timestamptz
    AND delivery.handler = ANY (ARRAY(SELECT jsonb_array_elements_text($2::jsonb)))
    AND (
      delivery.key IS NULL
      OR NOT (
        EXISTS (
          SELECT 1 FROM ${deliveries} AS earlier
          WHERE earlier.handler = delivery.handler AND earlier.key = delivery.key
            AND earlier.status IN ('pending', 'processing')
            AND earlier.sequence < delivery.sequence
        )
        OR EXISTS (
          SELECT 1 FROM ${deliveries} AS running
          WHERE running.handler = delivery.handler AND running.key = delivery.key
            AND running.status = 'processing' AND running.id <> delivery.id
        )
      )
    )
  ORDER BY due_at, delivery.id
  LIMIT $3::int
  FOR UPDATE OF delivery SKIP LOCKED
), claimed AS (
  UPDATE ${deliveries} AS target
  SET status = 'processing',
      attempts = target.attempts + 1,
      lease_owner = $4::text,
      lease_token = gen_random_uuid()::text,
      lease_expires_at = $5::timestamptz,
      updated_at = $1::timestamptz
  FROM candidates
  WHERE target.id = candidates.id
  RETURNING target.id, target.event_id, target.handler, target.attempts, target.lease_token, candidates.due_at
)
SELECT claimed.id AS delivery_id, claimed.handler, claimed.attempts::int AS attempts, claimed.lease_token,
  ${eventColumns('event')}
FROM claimed
JOIN ${this.#names.events} AS event ON event.id = claimed.event_id
ORDER BY claimed.due_at, claimed.id`;
  }

  /** Parameters: `[deliveryId, leaseToken, leaseExpiresAt, now]`. */
  get extendLease(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET lease_expires_at = $3::timestamptz, updated_at = $4::timestamptz
WHERE id = $1::text AND status = 'processing' AND lease_token = $2::text`);
  }

  /** Parameters: `[deliveryId, leaseToken, now]`. */
  get complete(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET status = 'completed', completed_at = $3::timestamptz, updated_at = $3::timestamptz, ${CLEAR_LEASE}
WHERE id = $1::text AND status = 'processing' AND lease_token = $2::text`);
  }

  /** Parameters: `[deliveryId, leaseToken, availableAt, errorJson, now]`. */
  get reschedule(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET status = 'pending', available_at = $3::timestamptz, last_error = $4::jsonb, updated_at = $5::timestamptz, ${CLEAR_LEASE}
WHERE id = $1::text AND status = 'processing' AND lease_token = $2::text`);
  }

  /** Parameters: `[deliveryId, leaseToken, errorJson, now]`. */
  get bury(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET status = 'dead', last_error = $3::jsonb, updated_at = $4::timestamptz, ${CLEAR_LEASE}
WHERE id = $1::text AND status = 'processing' AND lease_token = $2::text`);
  }

  /**
   * Gives a leased delivery back without spending its attempt: pending, due
   * `now`, attempt count decremented. Parameters: `[deliveryId, leaseToken, now]`.
   */
  get release(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET status = 'pending', available_at = $3::timestamptz, attempts = attempts - 1, updated_at = $3::timestamptz, ${CLEAR_LEASE}
WHERE id = $1::text AND status = 'processing' AND lease_token = $2::text`);
  }

  /** Parameters: `[deliveryIdsJson, now]`. */
  get requeue(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET status = 'pending', attempts = 0, available_at = $2::timestamptz, updated_at = $2::timestamptz
WHERE id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb))) AND status = 'dead'`);
  }

  /** Builds the filtered, creation-ordered listing. */
  listDeliveries(query: DeliveryQuery): PostgresStatement {
    const parameters: PostgresParameter[] = [];
    const conditions: string[] = [];
    const bind = (value: PostgresParameter): string => {
      parameters.push(value);
      return `$${parameters.length}::${typeof value === 'number' ? 'int' : 'text'}`;
    };
    if (query.status !== undefined) {
      conditions.push(`delivery.status = ${bind(query.status)}`);
    }
    if (query.handler !== undefined) {
      conditions.push(`delivery.handler = ${bind(query.handler)}`);
    }
    if (query.eventType !== undefined) {
      conditions.push(`event.type = ${bind(query.eventType)}`);
    }
    if (query.eventId !== undefined) {
      conditions.push(`delivery.event_id = ${bind(query.eventId)}`);
    }
    const where = conditions.length === 0 ? '' : `WHERE ${conditions.join(' AND ')}`;
    const limit = bind(query.limit ?? DEFAULT_LIST_LIMIT);
    const sql = `SELECT delivery.id AS delivery_id, delivery.handler, delivery.status, delivery.attempts::int AS attempts,
  ${isoText('delivery.available_at')} AS available_at,
  delivery.lease_owner,
  ${isoText('delivery.lease_expires_at')} AS lease_expires_at,
  delivery.last_error::text AS last_error,
  ${isoText('delivery.created_at')} AS created_at,
  ${isoText('delivery.updated_at')} AS updated_at,
  ${isoText('delivery.completed_at')} AS completed_at,
  ${eventColumns('event')}
FROM ${this.#names.deliveries} AS delivery
JOIN ${this.#names.events} AS event ON event.id = delivery.event_id
${where}
ORDER BY delivery.created_at, delivery.id
LIMIT ${limit}`;
    return { sql, parameters };
  }

  get countByStatus(): string {
    return `SELECT status, count(*)::text AS count FROM ${this.#names.deliveries} GROUP BY status`;
  }

  /** Parameters: `[now]`. */
  get oldestDue(): string {
    return `SELECT ${isoText('due_at')} AS oldest_due_at FROM (
  SELECT ${dueAtExpression('delivery')} AS due_at
  FROM ${this.#names.deliveries} AS delivery
  WHERE delivery.status IN ('pending', 'processing') AND ${dueAtExpression('delivery')} <= $1::timestamptz
  ORDER BY due_at
  LIMIT 1
) AS oldest`;
  }

  /**
   * Deletes one bounded batch of purgeable deliveries and returns the event id
   * of each deleted delivery. Parameters: `[cutoff, batchSize]`.
   * `FOR UPDATE SKIP LOCKED` keeps purge from waiting on, or being waited on by, anything.
   */
  purgeCompletedBatch(): string {
    return this.#purgeBatch(
      `status = 'completed' AND completed_at < $1::timestamptz`,
      'completed_at',
    );
  }

  purgeDeadBatch(): string {
    return this.#purgeBatch(`status = 'dead' AND updated_at < $1::timestamptz`, 'updated_at');
  }

  /**
   * Locks the given events, skipping any that a concurrent transaction is
   * referencing from a new delivery (a foreign key check holds `FOR KEY
   * SHARE`, which conflicts with this lock). Parameters: `[eventIdsJson]`.
   */
  get lockEvents(): string {
    return `SELECT id FROM ${this.#names.events}
WHERE id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
FOR UPDATE SKIP LOCKED`;
  }

  /**
   * Deletes locked events that no delivery references. It must run as a
   * statement of its own after {@link lockEvents}: it then takes a fresh
   * snapshot that includes every delivery committed before the lock was
   * granted, so an event that just gained a delivery is never cascaded away.
   * Parameters: `[eventIdsJson]`.
   */
  get deleteOrphanEvents(): string {
    const events = this.#names.events;
    return countChanged(`DELETE FROM ${events} AS event
WHERE event.id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
  AND NOT EXISTS (SELECT 1 FROM ${this.#names.deliveries} AS delivery WHERE delivery.event_id = event.id)`);
  }

  #purgeBatch(condition: string, orderColumn: string): string {
    const deliveries = this.#names.deliveries;
    return `WITH batch AS MATERIALIZED (
  SELECT id FROM ${deliveries}
  WHERE ${condition}
  ORDER BY ${orderColumn}, id
  LIMIT $2::int
  FOR UPDATE SKIP LOCKED
), deleted AS (
  DELETE FROM ${deliveries} AS delivery USING batch WHERE delivery.id = batch.id RETURNING delivery.event_id
)
SELECT event_id FROM deleted`;
  }
}
