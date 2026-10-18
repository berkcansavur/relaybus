import type { DeliveryQuery } from '@relaybus/core';
import type { PostgresNames } from './names.js';
import { isoText, laneExpression } from './sql-fragments.js';
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

/**
 * Due deliveries the fast claim examines before it gives up and lets the slow
 * claim look at the heads of all keys.
 */
const CLAIM_SCAN_WINDOW = 256;

/** Due deliveries the fast claim reads from one lane. A lane that fills it may hide claimable deliveries behind blocked followers. */
const LANE_SCAN_WINDOW = 64;

const HANDLER_PREDICATE = `delivery.handler = ANY (ARRAY(SELECT jsonb_array_elements_text($2::jsonb)))`;

const LANE_LIST = `ARRAY(SELECT jsonb_array_elements_text($4::jsonb)::smallint)`;

/**
 * SQL text for every operation of the store. Statements take their data as
 * parameters; only validated identifiers are interpolated. Documents travel
 * as one JSON text parameter expanded server-side with `jsonb_to_recordset`,
 * which keeps the statements identical across drivers and avoids array
 * literal encoding differences.
 *
 * Leasing is per lane, not per delivery. A lane row carries the owner, an
 * `epoch` (the fencing token: it grows every time the lane changes hands) and
 * `in_flight`, the ids of the deliveries the owner has claimed. A claim
 * therefore writes one lane row for a whole batch and no delivery row; a
 * completion deletes delivery rows and writes no lane row. `in_flight` may
 * list ids that were completed (their rows are gone): a delivery is in flight
 * only while its row exists as `pending` and its id is listed.
 */
export class PostgresStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
  }

  /**
   * Lock advisory key text for {@link setupLock}. It is derived from the schema
   * name only, so stores with different table prefixes in one schema also
   * serialize their `CREATE SCHEMA IF NOT EXISTS`, which is not race-free.
   */
  get setupLockKey(): string {
    return `relaybus:${this.#names.schema}`;
  }

  get setupLock(): string {
    return 'SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))';
  }

  /**
   * Reserves sequence numbers per key. Parameters: `[{ key, count }]` as JSON.
   * Keys are locked in sorted order, so publishers cannot deadlock within one
   * call (across calls of one transaction they can, see the README); each row lock is held until the publishing transaction ends, which makes
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
   * Parameters: `[deliveriesJson, createdAt, insertedEventIdsJson]`. Key and
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
  RETURNING id, event_id, handler
), extended AS (
  UPDATE ${events} AS event SET handlers = event.handlers || added.handlers
  FROM (
    SELECT inserted.event_id, array_agg(inserted.handler) AS handlers FROM inserted
    WHERE inserted.event_id <> ALL (${insertedEvents}) GROUP BY inserted.event_id
  ) AS added
  WHERE event.id = added.event_id
  RETURNING event.id
)
SELECT id FROM inserted`;
  }

  /**
   * First step of a claim, one statement: claims from the lanes this owner
   * already holds and records the batch in those lanes, all atomically, so a
   * backlog that fills the batch costs one round trip. Parameters:
   * `[now, handlersJson, limit, leaseOwner, leaseExpiresAt]`.
   *
   * The held lanes are locked `FOR NO KEY UPDATE` in lane order (a lane that
   * another transaction of this owner is changing makes the statement wait, a
   * takeover by anybody else is excluded by the lease itself). The candidate
   * rules are those of {@link claim}. Held lanes that picked nothing, run
   * nothing and have no full scan window (so nothing claimable hides in them)
   * are freed. Always returns one row per picked delivery, or one row whose
   * `delivery_id` is null; `window_saturated` says that some lane filled its
   * scan window, and `held_lanes` (JSON text) lists the lanes still held.
   */
  get claimHeld(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `WITH held AS MATERIALIZED (
  SELECT lane.lane, lane.epoch, lane.in_flight
  FROM ${lanes} AS lane
  WHERE lane.lease_owner = $4::text AND lane.lease_expires_at > $1::timestamptz
  ORDER BY lane.lane
  FOR NO KEY UPDATE
), ${this.#scopeAndRunning('SELECT held.lane, held.epoch, held.in_flight FROM held')},
${this.#fastWindow()}
), picked_rows AS MATERIALIZED (
  ${this.#pickedRowsSelect()}
), by_lane AS (
  SELECT picked_rows.lane, array_agg(picked_rows.delivery_id) AS ids FROM picked_rows GROUP BY picked_rows.lane
), still_running AS (
  SELECT lane.lane, array_agg(delivery.id) AS ids
  FROM by_lane
  JOIN ${lanes} AS lane ON lane.lane = by_lane.lane
  CROSS JOIN LATERAL unnest(lane.in_flight) AS flying (id)
  JOIN ${deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'
  GROUP BY lane.lane
), extended AS (
  UPDATE ${lanes} AS lane
  SET in_flight = COALESCE(still_running.ids, '{}') || by_lane.ids,
      lease_expires_at = GREATEST(lane.lease_expires_at, $5::timestamptz)
  FROM by_lane
  LEFT JOIN still_running ON still_running.lane = by_lane.lane
  WHERE lane.lane = by_lane.lane
  RETURNING lane.lane
), saturated_lanes AS (
  SELECT due_window.lane FROM due_window GROUP BY due_window.lane HAVING count(*) >= ${LANE_SCAN_WINDOW}
), freed AS (
  UPDATE ${lanes} AS lane
  SET lease_owner = NULL, lease_expires_at = NULL, in_flight = '{}'
  FROM held
  WHERE lane.lane = held.lane
    AND lane.lane NOT IN (SELECT lane FROM by_lane)
    AND lane.lane NOT IN (SELECT lane FROM saturated_lanes)
    AND ${this.#noRunningDelivery('lane')}
  RETURNING lane.lane
)
SELECT EXISTS (SELECT 1 FROM saturated_lanes) AS window_saturated,
  (SELECT COALESCE(jsonb_agg(held.lane), '[]'::jsonb) FROM held WHERE held.lane NOT IN (SELECT lane FROM freed))::text AS held_lanes,
  picked.*
FROM (SELECT 1) AS anchor
LEFT JOIN picked_rows AS picked ON true
ORDER BY picked.due_at, picked.delivery_id`;
  }

  /**
   * Locks the lanes this owner already holds under a live lease and returns
   * them with their epochs. Parameters: `[leaseOwner, now]`. Rows are locked in
   * lane order, the order every statement that locks several lanes uses.
   */
  get lockHeldLanes(): string {
    return `SELECT lane::int AS lane, epoch::text AS epoch
FROM ${this.#names.lanes}
WHERE lease_owner = $1::text AND lease_expires_at > $2::timestamptz
ORDER BY lane
FOR NO KEY UPDATE`;
  }

  /**
   * Takes up to `$3` free lanes (no lease, or an expired one) that have a due
   * delivery of the requested handlers, the ones with the oldest due delivery
   * first. Parameters: `[now, handlersJson, laneCount, leaseOwner, leaseExpiresAt]`.
   *
   * A taken lane gets a new epoch, which fences everything the previous owner
   * still holds. Deliveries the previous owner had in flight are the ones whose
   * attempt was never recorded (a claim writes no delivery row), so each of
   * them is charged one attempt here: a worker that crashes with a poison
   * message still runs it into the retry limit. Lanes another claimer has locked
   * are skipped, never waited for.
   */
  get takeFreeLanes(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `WITH picked AS (
  SELECT free.lane
  FROM ${lanes} AS free
  CROSS JOIN LATERAL (
    SELECT delivery.available_at, delivery.id
    FROM ${deliveries} AS delivery
    WHERE delivery.lane = free.lane AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
    ORDER BY delivery.available_at, delivery.id
    LIMIT 1
  ) AS oldest
  WHERE free.lease_expires_at IS NULL OR free.lease_expires_at <= $1::timestamptz
  ORDER BY oldest.available_at, oldest.id
  LIMIT $3::int
  FOR UPDATE OF free SKIP LOCKED
), charged AS (
  UPDATE ${deliveries} AS delivery SET attempts = delivery.attempts + 1
  FROM (
    SELECT unnest(lane.in_flight) AS id FROM ${lanes} AS lane JOIN picked ON picked.lane = lane.lane
  ) AS lost
  WHERE delivery.id = lost.id AND delivery.status = 'pending'
  RETURNING delivery.id
), taken AS (
  UPDATE ${lanes} AS lane
  SET epoch = lane.epoch + 1, lease_owner = $4::text, lease_expires_at = $5::timestamptz, in_flight = '{}'
  FROM picked WHERE lane.lane = picked.lane
  RETURNING lane.lane, lane.epoch
)
SELECT lane::int AS lane, epoch::text AS epoch FROM taken ORDER BY lane`;
  }

  /**
   * Fast claim: picks from the first {@link CLAIM_SCAN_WINDOW} due deliveries of
   * the given lanes in due order, which is everything the claim needs while the
   * front of the queue is not clogged with blocked followers of busy keys.
   * Parameters: `[now, handlersJson, limit, lanesJson]`. The lanes must be held
   * and locked by the caller.
   *
   * Always returns one row per picked delivery, or a single row whose
   * `delivery_id` is null when nothing was picked. `window_saturated` says
   * whether the window was full: when it is and fewer than `limit` deliveries
   * were picked, deliveries beyond the window may still be claimable and the
   * caller runs {@link claimBehindBlockedKeys}. When the window is not
   * saturated every due delivery was examined, so the result is complete.
   *
   * A delivery is a candidate when it is pending, due, and not in flight. A
   * keyed delivery is a candidate only when it is the head of its (handler,
   * key), that is (a) no earlier delivery of that handler and key is pending
   * (an earlier one in flight is still a pending row, which is the ordering
   * guarantee), and (b) no other delivery of that handler and key is in
   * flight, which covers a delivery that re-enters the key behind a running one
   * (requeue, or a handler added to an old event).
   *
   * No row is locked or written here: the lanes' exclusive leases are the
   * mutual exclusion, so two claimers never see the same lane.
   */
  get claim(): string {
    return `WITH ${this.#scopeAndRunning(`SELECT lane.lane, lane.epoch, lane.in_flight FROM ${this.#names.lanes} AS lane WHERE lane.lane = ANY (${LANE_LIST})`)},
${this.#fastWindow()}
)
${this.#pickedRows(`((SELECT count(*) FROM due_window) >= ${CLAIM_SCAN_WINDOW} OR EXISTS (SELECT 1 FROM due_window GROUP BY lane HAVING count(*) >= ${LANE_SCAN_WINDOW}))`)}`;
  }

  /** The `due_window` and `candidates` CTEs of the fast claim, up to (not including) the closing parenthesis of `candidates`. */
  #fastWindow(): string {
    const deliveries = this.#names.deliveries;
    return `due_window AS MATERIALIZED (
  SELECT recent.id, recent.lane, recent.available_at
  FROM scope
  CROSS JOIN LATERAL (
    SELECT delivery.id, delivery.lane, delivery.available_at
    FROM ${deliveries} AS delivery
    WHERE delivery.lane = scope.lane AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
      AND delivery.id <> ALL (scope.in_flight)
    ORDER BY delivery.available_at, delivery.id
    LIMIT ${LANE_SCAN_WINDOW}
  ) AS recent
  ORDER BY recent.available_at, recent.id
  LIMIT ${CLAIM_SCAN_WINDOW}
), candidates AS MATERIALIZED (
  SELECT delivery.id, delivery.lane, delivery.available_at
  FROM due_window
  JOIN ${deliveries} AS delivery ON delivery.id = due_window.id
  WHERE delivery.key IS NULL OR NOT (${this.#blockedKey('delivery')})
  ORDER BY delivery.available_at, delivery.id
  LIMIT $3::int`;
  }

  /**
   * Slow claim, run when {@link claim} picked too few deliveries from a full
   * window: the front of the due queue is mostly followers of blocked keys.
   * Same parameters and rules as {@link claim}, but heads are found with
   * a loose index scan over the pending-key index of each lane (it hops from key
   * to key), so the cost follows the number of pending keys in the lanes
   * instead of one probe per due follower.
   */
  get claimBehindBlockedKeys(): string {
    const deliveries = this.#names.deliveries;
    return `WITH ${this.#scopeAndRunning(`SELECT lane.lane, lane.epoch, lane.in_flight FROM ${this.#names.lanes} AS lane WHERE lane.lane = ANY (${LANE_LIST})`)},
heads AS MATERIALIZED (
  SELECT head.id, head.lane, head.available_at, head.handler, head.key
  FROM scope
  CROSS JOIN unnest(ARRAY(SELECT jsonb_array_elements_text($2::jsonb))) AS wanted (handler)
  CROSS JOIN LATERAL (
    -- Loose index scan: hop from key to key on the pending-key index, then take the lowest
    -- sequence of each key. The cost follows the number of keys, not the number of followers.
    WITH RECURSIVE hops (key) AS (
      (SELECT candidate.key FROM ${deliveries} AS candidate
       WHERE candidate.lane = scope.lane AND candidate.handler = wanted.handler
         AND candidate.key IS NOT NULL AND candidate.status = 'pending'
       ORDER BY candidate.key LIMIT 1)
      UNION ALL
      SELECT (SELECT candidate.key FROM ${deliveries} AS candidate
              WHERE candidate.lane = scope.lane AND candidate.handler = wanted.handler
                AND candidate.key IS NOT NULL AND candidate.status = 'pending' AND candidate.key > hops.key
              ORDER BY candidate.key LIMIT 1)
      FROM hops WHERE hops.key IS NOT NULL
    )
    SELECT lowest.id, lowest.lane, lowest.available_at, lowest.handler, lowest.key
    FROM hops
    CROSS JOIN LATERAL (
      SELECT candidate.id, candidate.lane, candidate.available_at, candidate.handler, candidate.key
      FROM ${deliveries} AS candidate
      WHERE candidate.lane = scope.lane AND candidate.handler = wanted.handler
        AND candidate.key = hops.key AND candidate.status = 'pending'
      ORDER BY candidate.sequence LIMIT 1
    ) AS lowest
    WHERE hops.key IS NOT NULL
  ) AS head
), keyed AS MATERIALIZED (
  SELECT heads.id, heads.lane, heads.available_at
  FROM heads
  JOIN scope ON scope.lane = heads.lane
  WHERE heads.available_at <= $1::timestamptz AND heads.id <> ALL (scope.in_flight)
    AND NOT EXISTS (SELECT 1 FROM running WHERE running.handler = heads.handler AND running.key = heads.key)
  ORDER BY heads.available_at, heads.id
  LIMIT $3::int
), unkeyed AS MATERIALIZED (
  SELECT recent.id, recent.lane, recent.available_at
  FROM scope
  CROSS JOIN LATERAL (
    SELECT delivery.id, delivery.lane, delivery.available_at
    FROM ${deliveries} AS delivery
    WHERE delivery.lane = scope.lane AND delivery.key IS NULL AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
      AND delivery.id <> ALL (scope.in_flight)
    ORDER BY delivery.available_at, delivery.id
    LIMIT $3::int
  ) AS recent
  ORDER BY recent.available_at, recent.id
  LIMIT $3::int
), candidates AS MATERIALIZED (
  SELECT id, lane, available_at FROM (
    SELECT id, lane, available_at FROM keyed
    UNION ALL
    SELECT id, lane, available_at FROM unkeyed
  ) AS both_sets
  ORDER BY available_at, id
  LIMIT $3::int
)
${this.#pickedRows('false')}`;
  }

  /** The held lanes with their epochs and in-flight ids, and the keyed deliveries running in them. */
  #scopeAndRunning(scopeQuery: string): string {
    const deliveries = this.#names.deliveries;
    return `scope AS MATERIALIZED (
  ${scopeQuery}
), running AS MATERIALIZED (
  SELECT delivery.handler, delivery.key
  FROM scope
  CROSS JOIN LATERAL unnest(scope.in_flight) AS flying (id)
  JOIN ${deliveries} AS delivery ON delivery.id = flying.id
  WHERE delivery.status = 'pending' AND delivery.key IS NOT NULL
)`;
  }

  /** True when an earlier delivery of the candidate's handler and key is pending, or another one is running. */
  #blockedKey(alias: string): string {
    return `EXISTS (
        SELECT 1 FROM ${this.#names.deliveries} AS earlier
        WHERE earlier.lane = ${alias}.lane AND earlier.handler = ${alias}.handler AND earlier.key = ${alias}.key
          AND earlier.status = 'pending' AND earlier.key IS NOT NULL
          AND earlier.sequence < ${alias}.sequence
      ) OR ${this.#runningElsewhere(alias)}`;
  }

  /** True when a delivery of the candidate's handler and key is in flight. The candidate itself never is. */
  #runningElsewhere(alias: string): string {
    return `EXISTS (
        SELECT 1 FROM running
        WHERE running.handler = ${alias}.handler AND running.key = ${alias}.key
      )`;
  }

  /** Final SELECT shared by both claims: the picked deliveries with their lane epoch and event. */
  #pickedRows(saturated: string): string {
    return `SELECT ${saturated} AS window_saturated, picked.*
FROM (SELECT 1) AS anchor
LEFT JOIN (
  ${this.#pickedRowsSelect()}
) AS picked ON true
ORDER BY picked.due_at, picked.delivery_id`;
  }

  /** The picked deliveries joined with the lane epoch and their event, as the claim result columns. */
  #pickedRowsSelect(): string {
    return `SELECT candidates.available_at AS due_at, delivery.id AS delivery_id, delivery.lane::int AS lane,
    scope.epoch::text AS epoch, delivery.handler, (delivery.attempts + 1)::int AS attempt,
    ${eventColumns('event')}
  FROM candidates
  JOIN scope ON scope.lane = candidates.lane
  JOIN ${this.#names.deliveries} AS delivery ON delivery.id = candidates.id
  JOIN ${this.#names.events} AS event ON event.id = delivery.event_id`;
  }

  /**
   * Records the deliveries a claim picked in their lanes and frees the lanes it
   * took or held for nothing. Parameters:
   * `[pickedJson, idleLanesJson, leaseOwner, leaseExpiresAt]` with
   * `pickedJson = [{ lane, ids }]`.
   *
   * A picked lane lists the ids that are still in flight (listed ids whose row
   * still exists) plus the new ones, and its lease reaches `leaseExpiresAt`.
   * An idle lane is freed when no delivery of it is in flight, so other workers
   * need not wait for the lease to expire.
   */
  get recordClaim(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `WITH picked AS (
  SELECT recorded.lane, recorded.ids
  FROM jsonb_to_recordset($1::jsonb) AS recorded (lane smallint, ids text[])
), still_running AS (
  SELECT lane.lane, array_agg(delivery.id) AS ids
  FROM picked
  JOIN ${lanes} AS lane ON lane.lane = picked.lane
  CROSS JOIN LATERAL unnest(lane.in_flight) AS flying (id)
  JOIN ${deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'
  GROUP BY lane.lane
), extended AS (
  UPDATE ${lanes} AS lane
  SET in_flight = COALESCE(still_running.ids, '{}') || picked.ids,
      lease_expires_at = GREATEST(lane.lease_expires_at, $4::timestamptz)
  FROM picked
  LEFT JOIN still_running ON still_running.lane = picked.lane
  WHERE lane.lane = picked.lane
  RETURNING lane.lane
), freed AS (
  UPDATE ${lanes} AS lane
  SET lease_owner = NULL, lease_expires_at = NULL, in_flight = '{}'
  WHERE lane.lane = ANY (ARRAY(SELECT jsonb_array_elements_text($2::jsonb)::smallint))
    AND lane.lease_owner = $3::text
    AND ${this.#noRunningDelivery('lane')}
  RETURNING lane.lane
)
SELECT (SELECT count(*) FROM extended)::int AS extended, (SELECT count(*) FROM freed)::int AS freed`;
  }

  /** True when none of the lane's listed deliveries exists as a pending row. */
  #noRunningDelivery(laneAlias: string): string {
    return `NOT EXISTS (
      SELECT 1 FROM unnest(${laneAlias}.in_flight) AS flying (id)
      JOIN ${this.#names.deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'
    )`;
  }

  /**
   * Frees every lane `$1` holds that has no running delivery. Parameters:
   * `[leaseOwner]`.
   */
  get relinquish(): string {
    return `UPDATE ${this.#names.lanes} AS lane
SET lease_owner = NULL, lease_expires_at = NULL, in_flight = '{}'
WHERE lane.lease_owner = $1::text AND ${this.#noRunningDelivery('lane')}`;
  }

  /**
   * Extends the lease of the lane a running delivery belongs to; the lane lease
   * never shrinks. Parameters: `[deliveryId, lane, epoch, leaseExpiresAt]`.
   */
  get extendLease(): string {
    return countChanged(`UPDATE ${this.#names.lanes} AS lane
SET lease_expires_at = GREATEST(lane.lease_expires_at, $4::timestamptz)
WHERE lane.lane = $2::smallint AND lane.epoch = $3::bigint AND $1::text = ANY (lane.in_flight)
  AND EXISTS (SELECT 1 FROM ${this.#names.deliveries} AS delivery WHERE delivery.id = $1::text AND delivery.status = 'pending')`);
  }

  /**
   * Deletes the running deliveries whose lane lease is still the one they were
   * claimed under. Parameters: `[leasesJson]`, plus `now` when a completion history is kept, with
   * `leasesJson = [{ id, lane, epoch }]`. Returns the ids that were completed.
   *
   * The lane rows are locked `FOR KEY SHARE` first, in lane order, and the lock
   * is held until the surrounding transaction ends. A claimer that takes the
   * lane over locks it `FOR UPDATE`, which conflicts with that lock, so it
   * skips the lane until this completion commits or rolls back: a completion
   * and a takeover cannot both succeed for one delivery, which is what makes a
   * transactional handler's writes effectively-once. The weakest lock that
   * conflicts with a takeover is used on purpose: it does not conflict with the
   * owner's own claim and heartbeats (plain updates of the lane row), which
   * would otherwise wait for every completion in progress. With a completion history the deleted rows are copied to
   * it in the same statement.
   */
  get complete(): string {
    const deliveries = this.#names.deliveries;
    const history = this.#names.completionHistory;
    return `WITH leases AS (
  SELECT lease.id, lease.lane, lease.epoch
  FROM jsonb_to_recordset($1::jsonb) AS lease (id text, lane smallint, epoch bigint)
), fence AS MATERIALIZED (
  SELECT lane.lane, lane.epoch, lane.in_flight
  FROM ${this.#names.lanes} AS lane
  WHERE lane.lane IN (SELECT lane FROM leases)
  ORDER BY lane.lane
  FOR KEY SHARE
), completed AS (
  DELETE FROM ${deliveries} AS delivery
  USING leases JOIN fence ON fence.lane = leases.lane AND fence.epoch = leases.epoch
  WHERE delivery.id = leases.id AND delivery.lane = leases.lane AND delivery.status = 'pending'
    AND leases.id = ANY (fence.in_flight)
  RETURNING delivery.id, delivery.event_id, delivery.handler, delivery.attempts, delivery.available_at, delivery.created_at
)${
      history
        ? `, archived AS (
  INSERT INTO ${this.#names.completions} (id, event_id, handler, attempts, available_at, created_at, completed_at)
  SELECT id, event_id, handler, attempts + 1, available_at, created_at, $2::timestamptz FROM completed
  RETURNING id
)`
        : ''
    }
SELECT id FROM completed`;
  }

  /**
   * Returns a running delivery to pending, due at `availableAt`, and takes it
   * out of the lane's in-flight list. Parameters:
   * `[deliveryId, lane, epoch, availableAt, errorJson, now]`. The attempt that
   * just ended is counted.
   */
  get reschedule(): string {
    return this.#leaveFlight(
      `SET available_at = $4::timestamptz, last_error = $5::jsonb, attempts = delivery.attempts + 1, updated_at = $6::timestamptz`,
    );
  }

  /** Parameters: `[deliveryId, lane, epoch, errorJson, now]`. The attempt that just ended is counted. */
  get bury(): string {
    return this.#leaveFlight(
      `SET status = 'dead', last_error = $4::jsonb, attempts = delivery.attempts + 1, updated_at = $5::timestamptz`,
    );
  }

  /**
   * Gives a running delivery back without spending its attempt (an attempt is
   * only recorded when it ends in failure or a lost lease): pending, due `now`.
   * Parameters: `[deliveryId, lane, epoch, now]`.
   */
  get release(): string {
    return this.#leaveFlight(`SET available_at = $4::timestamptz, updated_at = $4::timestamptz`);
  }

  /**
   * Applies `assignments` to a running delivery and removes it from its lane's
   * in-flight list, if the lane still has the caller's epoch. The lane row is
   * locked first (`FOR NO KEY UPDATE`, which conflicts with a takeover's `FOR UPDATE`), so a takeover cannot interleave. Parameters
   * `$1`..`$3` are always `[deliveryId, lane, epoch]`.
   */
  #leaveFlight(assignments: string): string {
    const lanes = this.#names.lanes;
    return `WITH fence AS MATERIALIZED (
  SELECT lane.lane FROM ${lanes} AS lane
  WHERE lane.lane = $2::smallint AND lane.epoch = $3::bigint AND $1::text = ANY (lane.in_flight)
  FOR NO KEY UPDATE
), changed AS (
  UPDATE ${this.#names.deliveries} AS delivery ${assignments}
  FROM fence
  WHERE delivery.id = $1::text AND delivery.lane = fence.lane AND delivery.status = 'pending'
  RETURNING delivery.id
), trimmed AS (
  UPDATE ${lanes} AS lane SET in_flight = array_remove(lane.in_flight, $1::text)
  FROM changed WHERE lane.lane = $2::smallint
  RETURNING lane.lane
)
SELECT count(*)::int AS count FROM changed`;
  }

  /** Parameters: `[deliveryIdsJson, now]`. */
  get requeue(): string {
    return countChanged(`UPDATE ${this.#names.deliveries}
SET status = 'pending', attempts = 0, available_at = $2::timestamptz, updated_at = $2::timestamptz
WHERE id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb))) AND status = 'dead'`);
  }

  /**
   * Builds the filtered, creation-ordered listing. A delivery is `processing`
   * while its row exists as `pending` and its lane lists its id; its attempt
   * count, lease owner and lease expiry then come from the claim and the lane.
   * Records of the completion history (when kept) are listed as `completed`.
   */
  listDeliveries(query: DeliveryQuery): PostgresStatement {
    const parameters: PostgresParameter[] = [];
    const bind = (value: PostgresParameter): string => {
      parameters.push(value);
      return `$${parameters.length}::${typeof value === 'number' ? 'int' : 'text'}`;
    };
    const filters = {
      handler: query.handler === undefined ? undefined : bind(query.handler),
      eventType: query.eventType === undefined ? undefined : bind(query.eventType),
      eventId: query.eventId === undefined ? undefined : bind(query.eventId),
      status: query.status === undefined ? undefined : bind(query.status),
    };
    const limit = bind(query.limit ?? DEFAULT_LIST_LIMIT);
    const common = (alias: string): string[] => [
      ...(filters.handler === undefined ? [] : [`${alias}.handler = ${filters.handler}`]),
      ...(filters.eventType === undefined ? [] : [`event.type = ${filters.eventType}`]),
      ...(filters.eventId === undefined ? [] : [`${alias}.event_id = ${filters.eventId}`]),
    ];
    const inFlight = `(delivery.status = 'pending' AND delivery.id = ANY (lane.in_flight))`;
    const whereCurrent = [
      ...common('delivery'),
      ...(filters.status === undefined
        ? []
        : [
            `(CASE WHEN ${inFlight} THEN 'processing' ELSE delivery.status END) = ${filters.status}`,
          ]),
    ];
    const current = `SELECT delivery.id AS delivery_id, delivery.handler,
  CASE WHEN ${inFlight} THEN 'processing' ELSE delivery.status END AS status,
  (delivery.attempts + CASE WHEN ${inFlight} THEN 1 ELSE 0 END)::int AS attempts,
  ${isoText('delivery.available_at')} AS available_at,
  CASE WHEN ${inFlight} THEN lane.lease_owner END AS lease_owner,
  CASE WHEN ${inFlight} THEN ${isoText('lane.lease_expires_at')} END AS lease_expires_at,
  delivery.last_error::text AS last_error,
  ${isoText('delivery.created_at')} AS created_at,
  ${isoText('delivery.updated_at')} AS updated_at,
  ${isoText('delivery.completed_at')} AS completed_at,
  ${eventColumns('event')}
FROM ${this.#names.deliveries} AS delivery
JOIN ${this.#names.lanes} AS lane ON lane.lane = delivery.lane
JOIN ${this.#names.events} AS event ON event.id = delivery.event_id
${whereCurrent.length === 0 ? '' : `WHERE ${whereCurrent.join(' AND ')}`}`;
    const includeHistory =
      this.#names.completionHistory && (query.status === undefined || query.status === 'completed');
    if (!includeHistory) {
      return {
        sql: `${current}\nORDER BY delivery.created_at, delivery.id\nLIMIT ${limit}`,
        parameters,
      };
    }
    const history = `SELECT done.id AS delivery_id, done.handler, 'completed' AS status, done.attempts,
  ${isoText('done.available_at')} AS available_at,
  NULL AS lease_owner,
  NULL AS lease_expires_at,
  NULL AS last_error,
  ${isoText('done.created_at')} AS created_at,
  ${isoText('done.completed_at')} AS updated_at,
  ${isoText('done.completed_at')} AS completed_at,
  ${eventColumns('event')}
FROM ${this.#names.completions} AS done
JOIN ${this.#names.events} AS event ON event.id = done.event_id
${common('done').length === 0 ? '' : `WHERE ${common('done').join(' AND ')}`}`;
    return {
      sql: `SELECT * FROM (\n${current}\nUNION ALL\n${history}\n) AS listed\nORDER BY created_at, delivery_id\nLIMIT ${limit}`,
      parameters,
    };
  }

  /**
   * Counts of rows by stored status (`pending` includes running deliveries;
   * `completed` rows exist only after an upgrade from 0.1.0). Each count is
   * answered from a partial index.
   */
  get countByStatus(): string {
    const deliveries = this.#names.deliveries;
    return ['pending', 'dead', 'completed']
      .map(
        (status) =>
          `SELECT '${status}' AS status, count(*)::text AS count FROM ${deliveries} WHERE status = '${status}'`,
      )
      .join('\nUNION ALL\n');
  }

  /** Deliveries that are running: listed by a lane and still pending as rows. */
  get countRunning(): string {
    return `SELECT count(*)::text AS count
FROM ${this.#names.lanes} AS lane
CROSS JOIN LATERAL unnest(lane.in_flight) AS flying (id)
JOIN ${this.#names.deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'`;
  }

  get countCompletionHistory(): string {
    return `SELECT count(*)::text AS count FROM ${this.#names.completions}`;
  }

  /**
   * Oldest instant at which a delivery became claimable: `available_at` of a
   * pending delivery that nobody runs, or the lease expiry of a lane that runs
   * deliveries. Parameters: `[now]`.
   */
  get oldestDue(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `SELECT ${isoText('min(due.due_at)')} AS oldest_due_at FROM (
  SELECT waiting.available_at AS due_at
  FROM ${lanes} AS lane
  CROSS JOIN LATERAL (
    SELECT delivery.available_at FROM ${deliveries} AS delivery
    WHERE delivery.lane = lane.lane AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND delivery.id <> ALL (lane.in_flight)
    ORDER BY delivery.available_at LIMIT 1
  ) AS waiting
  UNION ALL
  SELECT lane.lease_expires_at AS due_at
  FROM ${lanes} AS lane
  WHERE lane.lease_expires_at <= $1::timestamptz AND NOT ${this.#noRunningDelivery('lane')}
) AS due`;
  }

  /**
   * Deletes one bounded batch of purgeable deliveries (or of the completion
   * history) and returns the event id of each deleted row. Parameters:
   * `[cutoff, batchSize]`. `FOR UPDATE SKIP LOCKED` keeps purge from waiting
   * on, or being waited on by, anything.
   */
  purgeCompletedBatch(): string {
    return this.#purgeBatch(
      this.#names.deliveries,
      `status = 'completed' AND completed_at < $1::timestamptz`,
      'completed_at',
    );
  }

  purgeCompletionHistoryBatch(): string {
    return this.#purgeBatch(
      this.#names.completions,
      `completed_at < $1::timestamptz`,
      'completed_at',
    );
  }

  purgeDeadBatch(): string {
    return this.#purgeBatch(
      this.#names.deliveries,
      `status = 'dead' AND updated_at < $1::timestamptz`,
      'updated_at',
    );
  }

  /**
   * Selects and locks one bounded batch of events created before the cutoff
   * that no delivery references. Parameters: `[cutoff, batchSize]`. The ids go
   * through {@link deleteOrphanEvents}, which re-checks under a fresh snapshot.
   */
  get lockOrphanEventsBatch(): string {
    return `SELECT event.id FROM ${this.#names.events} AS event
WHERE event.created_at < $1::timestamptz
  AND ${this.#unreferenced('event')}
ORDER BY event.created_at, event.id
LIMIT $2::int
FOR UPDATE OF event SKIP LOCKED`;
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
  AND ${this.#unreferenced('event')}`);
  }

  /** True when no delivery and no completion history record references the event. */
  #unreferenced(eventAlias: string): string {
    const history = this.#names.completionHistory
      ? ` AND NOT EXISTS (SELECT 1 FROM ${this.#names.completions} AS done WHERE done.event_id = ${eventAlias}.id)`
      : '';
    return `NOT EXISTS (SELECT 1 FROM ${this.#names.deliveries} AS delivery WHERE delivery.event_id = ${eventAlias}.id)${history}`;
  }

  #purgeBatch(table: string, condition: string, orderColumn: string): string {
    return `WITH batch AS MATERIALIZED (
  SELECT id FROM ${table}
  WHERE ${condition}
  ORDER BY ${orderColumn}, id
  LIMIT $2::int
  FOR UPDATE SKIP LOCKED
), deleted AS (
  DELETE FROM ${table} AS delivery USING batch WHERE delivery.id = batch.id RETURNING delivery.event_id
)
SELECT event_id FROM deleted`;
  }
}
