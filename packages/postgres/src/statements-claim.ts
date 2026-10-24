import type { PostgresNames } from './names.js';
import {
  CLAIM_SCAN_WINDOW,
  HANDLER_PREDICATE,
  LANE_SCAN_WINDOW,
  eventColumns,
  keyHeads,
  noRunningDelivery,
} from './statement-fragments.js';

const LANE_LIST = `ARRAY(SELECT jsonb_array_elements_text($4::jsonb)::smallint)`;

/** Statements of the claim continued round by round, its record, reconciliation and lane handback. */
export class ClaimStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
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
  CROSS JOIN LATERAL (${keyHeads(this.#names, 'scope')}) AS head
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
    scope.epoch::text AS epoch, delivery.claims AS claims, delivery.handler, delivery.key AS delivery_key, delivery.sequence::text AS sequence, (delivery.attempts + 1)::int AS attempt,
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
    AND ${noRunningDelivery(this.#names, 'lane')}
  RETURNING lane.lane
)
SELECT (SELECT count(*) FROM extended)::int AS extended, (SELECT count(*) FROM freed)::int AS freed`;
  }

  /**
   * Keeps in the in-flight lists of the lanes `$1` holds only the claims in `$2`
   * (`[{ id, lane, epoch, claims }]`, the ones this store handed out and nobody
   * settled yet) and takes every other delivery out. That is a batch a claim
   * recorded but whose answer never arrived: nothing runs it and the lane lease,
   * renewed by the owner's other deliveries, would keep it away from everybody for
   * ever. A claim matches only with the lane epoch and the delivery generation it was
   * handed out with. Parameters: `[leaseOwner, handedOutJson, generation]`; a lane whose `claim_floor` is above
   * the generation was reconciled by a newer call, so an older (late) reconciliation leaves it alone.
   * The lanes are locked in lane order first (see the lock order on {@link PostgresStatements}).
   */
  get reconcileClaims(): string {
    const deliveries = this.#names.deliveries;
    const handedOut = `jsonb_to_recordset($2::jsonb) AS handed (id text, lane smallint, epoch bigint, claims int)`;
    const kept = `SELECT 1 FROM ${handedOut} JOIN ${deliveries} AS delivery ON delivery.id = handed.id
      WHERE handed.id = flying.id AND handed.lane = lane.lane AND handed.epoch = lane.epoch AND delivery.claims = handed.claims`;
    return `WITH owned AS MATERIALIZED (
  SELECT lane.lane FROM ${this.#names.lanes} AS lane
  WHERE lane.lease_owner = $1::text AND lane.claim_floor <= $3::bigint
  ORDER BY lane.lane
  FOR NO KEY UPDATE
)
UPDATE ${this.#names.lanes} AS lane
SET in_flight = ARRAY(SELECT flying.id FROM unnest(lane.in_flight) AS flying (id) WHERE EXISTS (${kept})),
    claim_floor = $3::bigint
FROM owned
WHERE lane.lane = owned.lane AND lane.lease_owner = $1::text AND lane.claim_floor <= $3::bigint`;
  }

  /**
   * Frees every lane `$1` holds that has no running delivery, locking them in lane order first.
   * Parameters: `[leaseOwner]`.
   */
  get relinquish(): string {
    return `WITH owned AS MATERIALIZED (
  SELECT lane.lane FROM ${this.#names.lanes} AS lane
  WHERE lane.lease_owner = $1::text
  ORDER BY lane.lane
  FOR NO KEY UPDATE
)
UPDATE ${this.#names.lanes} AS lane
SET lease_owner = NULL, lease_expires_at = NULL, in_flight = '{}'
FROM owned
WHERE lane.lane = owned.lane AND lane.lease_owner = $1::text AND ${noRunningDelivery(this.#names, 'lane')}`;
  }
}
