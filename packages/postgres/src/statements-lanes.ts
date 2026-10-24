import type { PostgresNames } from './names.js';
import {
  CLAIM_SCAN_WINDOW,
  HANDLER_PREDICATE,
  INITIAL_FREE_LANES,
  LANE_DEPTH_CAP,
  LANE_SCAN_WINDOW,
  eventColumns,
  keyHeads,
  noRunningDelivery,
} from './statement-fragments.js';

/**
 * Free lanes a claim looks at beyond the number of deliveries it still wants, so lanes whose due
 * deliveries all wait behind a key's later head do not hide the others. When even these are not enough
 * the claim continues in the transaction that takes lanes round by round.
 */
const FREE_LANE_SURPLUS = 16;

/** Which lanes `#candidates` picks from, under which CTE names, and how many. */
type CandidateSpec = {
  readonly prefix: string;
  readonly scope: string;
  readonly running: string | null;
  readonly limit: string;
};

/** True for a delivery whose (handler, key) runs nowhere in the CTE `running`; always true without one. */
function notRunning(running: string | null, alias: string): string {
  return running === null
    ? 'true'
    : `NOT EXISTS (SELECT 1 FROM ${running} AS running WHERE running.handler = ${alias}.handler AND running.key = ${alias}.key)`;
}

/** A free lane: its lease expired, or this owner left it behind under an older claim floor. */
const freeLane = (alias: string): string =>
  `(${alias}.lease_expires_at IS NULL OR ${alias}.lease_expires_at <= $1::timestamptz
      OR (${alias}.lease_owner = $4::text AND ${alias}.claim_floor < $6::bigint))`;

/** The free-lane CTEs of a claim that only looks at the lanes it holds: all empty. */
const NO_FREE_LANES = `), free_lanes AS (
  SELECT NULL::smallint AS lane, NULL::bigint AS epoch, NULL::text[] AS in_flight, false AS running WHERE false
), free_scope AS (
  SELECT NULL::smallint AS lane, NULL::bigint AS epoch WHERE false
), free_candidates AS (
  SELECT NULL::text AS id, NULL::smallint AS lane, NULL::timestamptz AS available_at WHERE false
)`;

/** Statements that claim with the lanes they hold or take: the one-statement claim and the lane takeover. */
export class LaneStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
  }

  /**
   * A claim in one statement, the only one in the common case. Parameters:
   * `[now, handlersJson, limit, leaseOwner, leaseExpiresAt, generation]`.
   *
   * 1. **Held lanes.** The lanes this owner holds under a live lease whose `claim_floor` is exactly the
   *    generation are locked `FOR NO KEY UPDATE` in lane order. A lane above it was raised by a
   *    reconciliation, so a statement of a claim whose caller gave up (a client timeout does not cancel
   *    the statement on the server) and that reaches the server late records nothing; a lane below it was
   *    taken by such a late statement after the reconciliation, and its listed deliveries were never
   *    handed out. A lane that another transaction of this owner is changing makes the statement wait;
   *    the statement then reports `stale` and does nothing (see the `stale` CTE).
   * 2. **Free lanes**, only when the held lanes yield fewer than `limit`: the free lanes with a due
   *    delivery (one index probe per free lane) are ranked by their oldest due delivery; the first
   *    {@link INITIAL_FREE_LANES} that can give something now, and further ones until what they can give
   *    (estimated from their first {@link LANE_DEPTH_CAP} due deliveries) covers what is still wanted, are locked
   *    `FOR UPDATE SKIP LOCKED`, so a lane another claimer has is skipped, never waited
   *    for. A lane that still runs deliveries of an expired lease is left to the transaction of
   *    {@link takeFreeLanes}, which charges their lost attempts (`takeover_waiting`); so is any lane whose
   *    locked row differs from what the statement's snapshot saw. A lane that runs nothing and did not
   *    change cannot have had a delivery written since the snapshot, so its deliveries can be read here.
   * 3. **Picks** follow the rules of `ClaimStatements.claim`: the fast window of each lane, and for lanes whose
   *    window filled up while the batch is not full, the heads of all keys (a loose index scan per lane).
   * 4. **Writes**: one update per lane. A held lane records its picks; a taken lane gets a new epoch, the
   *    owner, the lease and its picks; a taken lane that yielded nothing is not written. Held lanes that
   *    picked nothing, run nothing and have no full scan window are freed.
   *
   * Always returns one row per picked delivery, or one row whose `delivery_id` is null. `running_ids`
   * (JSON text) lists the deliveries the held lanes still run, `examined_lanes` the held lanes.
   */
  get claimLanes(): string {
    return this.#claimLanes(true);
  }

  /**
   * {@link claimLanes} limited to the lanes the owner holds: the same parameters, rows and writes, without
   * looking at free lanes (`takeover_waiting` and `unexamined_free_lanes` are false). A worker draining a
   * backlog finds its batch in the lanes it holds; this statement is about half as costly to plan, and
   * the caller runs {@link claimLanes} after it only when it came up short.
   */
  get claimHeldLanes(): string {
    return this.#claimLanes(false);
  }

  #claimLanes(includeFreeLanes: boolean): string {
    const free = includeFreeLanes
      ? this.#freeLaneChoice() + this.#freeLaneDepth() + this.#freeLaneTake()
      : NO_FREE_LANES;
    return (
      this.#heldLanes() + free + this.#claimPicks() + this.#claimRecord() + this.#claimRelease()
    );
  }

  /** Locks the held lanes, detects a stale snapshot and picks from the held lanes; ends with `wanted`. */
  #heldLanes(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `WITH held AS MATERIALIZED (
  SELECT lane.lane, lane.epoch, lane.in_flight
  FROM ${lanes} AS lane
  WHERE lane.lease_owner = $4::text AND lane.lease_expires_at > $1::timestamptz
    AND lane.claim_floor = $6::bigint
  ORDER BY lane.lane
  FOR NO KEY UPDATE
), stale AS (
  -- A claim that waited for a lane lock sees the new lane row but reads deliveries with the snapshot it took
  -- before the wait: whatever changed the lane meanwhile (a reschedule, a release) may also have changed
  -- deliveries it cannot see. The statement then does nothing and reports it, and the caller runs it again.
  SELECT EXISTS (
    SELECT 1 FROM held
    JOIN ${lanes} AS before ON before.lane = held.lane
    WHERE before.epoch <> held.epoch OR before.in_flight IS DISTINCT FROM held.in_flight
  ) AS value
), held_scope AS MATERIALIZED (
  SELECT held.lane, held.epoch, held.in_flight FROM held WHERE NOT (SELECT value FROM stale)
), held_running AS MATERIALIZED (
  SELECT delivery.handler, delivery.key
  FROM held_scope
  CROSS JOIN LATERAL unnest(held_scope.in_flight) AS flying (id)
  JOIN ${deliveries} AS delivery ON delivery.id = flying.id
  WHERE delivery.status = 'pending' AND delivery.key IS NOT NULL
), ${this.#candidates('held', 'held_scope', 'held_running', '$3::int')}, wanted AS MATERIALIZED (
  SELECT CASE WHEN (SELECT value FROM stale) THEN 0
    ELSE $3::int - (SELECT count(*) FROM held_candidates)::int END AS count
`;
  }

  /** The oldest due delivery of each free lane, at most the number still wanted plus a surplus. */
  #freeLaneChoice(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `), free_lanes AS MATERIALIZED (
  -- The oldest due delivery of each free lane: one index probe per free lane, bounded by the due time,
  -- so deliveries scheduled for later are never read.
  SELECT lane.lane, lane.epoch, lane.in_flight, lane.lease_owner, lane.lease_expires_at,
    oldest.available_at, oldest.id, NOT ${noRunningDelivery(this.#names, 'lane')} AS running
  FROM ${lanes} AS lane
  CROSS JOIN LATERAL (
    SELECT delivery.available_at, delivery.id
    FROM ${deliveries} AS delivery
    WHERE delivery.lane = lane.lane AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
    ORDER BY delivery.available_at, delivery.id
    LIMIT 1
  ) AS oldest
  WHERE (SELECT count FROM wanted) > 0 AND ${freeLane('lane')}
  ORDER BY oldest.available_at, oldest.id
  LIMIT (SELECT count FROM wanted) + ${FREE_LANE_SURPLUS}
`;
  }

  /** What each free candidate lane can give now, and their order. */
  #freeLaneDepth(): string {
    const deliveries = this.#names.deliveries;
    return `), free_depth AS MATERIALIZED (
  -- What each candidate lane can give now, from its first due deliveries: unkeyed ones, and of a key its
  -- lowest one when nothing earlier of the key is pending. Lanes that can give nothing are passed over;
  -- of the others, the first ${INITIAL_FREE_LANES} are taken, and further ones until they cover what is
  -- still wanted, so the earliest due work of the free lanes comes first.
  SELECT runnable.lane,
    row_number() OVER (ORDER BY runnable.available_at, runnable.id) AS rank,
    sum(runnable.count) OVER (ORDER BY runnable.available_at, runnable.id ROWS UNBOUNDED PRECEDING) - runnable.count AS due_before
  FROM (
    SELECT lane_candidate.lane, lane_candidate.available_at, lane_candidate.id, depth.count
    FROM free_lanes AS lane_candidate
    CROSS JOIN LATERAL (
      -- A full window that shows nothing runnable may hide runnable work behind it: such a lane counts
      -- once, and the claim looks behind its blocked deliveries.
      SELECT GREATEST(count(*) FILTER (
          WHERE due.key IS NULL OR (due.position = 1 AND NOT EXISTS (
            SELECT 1 FROM ${deliveries} AS earlier
            WHERE earlier.lane = due.lane AND earlier.handler = due.handler AND earlier.key = due.key
              AND earlier.status = 'pending' AND earlier.key IS NOT NULL AND earlier.sequence < due.sequence
          ))
        ), CASE WHEN count(*) >= ${LANE_DEPTH_CAP} THEN 1 ELSE 0 END) AS count
      FROM (
        SELECT window_rows.*,
          row_number() OVER (PARTITION BY window_rows.handler, window_rows.key ORDER BY window_rows.sequence) AS position
        FROM (
          SELECT delivery.lane, delivery.handler, delivery.key, delivery.sequence FROM ${deliveries} AS delivery
          WHERE delivery.lane = lane_candidate.lane AND delivery.status = 'pending'
            AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
          ORDER BY delivery.available_at, delivery.id
          LIMIT ${LANE_DEPTH_CAP}
        ) AS window_rows
      ) AS due
    ) AS depth
    WHERE NOT lane_candidate.running AND depth.count > 0
  ) AS runnable
`;
  }

  /** Locks the chosen free lanes that did not change hands and picks from them. */
  #freeLaneTake(): string {
    const lanes = this.#names.lanes;
    return `), free_locked AS MATERIALIZED (
  SELECT lane.lane, lane.epoch, lane.in_flight, lane.lease_owner, lane.lease_expires_at
  FROM ${lanes} AS lane
  WHERE lane.lane IN (
      SELECT chosen.lane FROM free_depth AS chosen
      WHERE chosen.rank <= ${INITIAL_FREE_LANES} OR chosen.due_before < (SELECT count FROM wanted)
    )
    AND ${freeLane('lane')}
  ORDER BY lane.lane
  FOR UPDATE SKIP LOCKED
), free_scope AS MATERIALIZED (
  -- A lane whose locked row is not the one the snapshot saw changed hands meanwhile: left for a later claim.
  SELECT locked.lane, locked.epoch + 1 AS epoch, '{}'::text[] AS in_flight
  FROM free_locked AS locked
  JOIN free_lanes AS seen ON seen.lane = locked.lane
  WHERE locked.epoch = seen.epoch AND locked.in_flight = seen.in_flight
    AND locked.lease_owner IS NOT DISTINCT FROM seen.lease_owner
    AND locked.lease_expires_at IS NOT DISTINCT FROM seen.lease_expires_at
), ${this.#candidates('free', 'free_scope', null, '(SELECT count FROM wanted)')}`;
  }

  /** The picked deliveries of held and taken lanes with their events, and the picks per lane. */
  #claimPicks(): string {
    const deliveries = this.#names.deliveries;
    return `, claim_scope AS MATERIALIZED (
  SELECT held_scope.lane, held_scope.epoch FROM held_scope
  UNION ALL
  SELECT free_scope.lane, free_scope.epoch FROM free_scope
), picked_rows AS MATERIALIZED (
  SELECT candidates.available_at AS due_at, delivery.id AS delivery_id, delivery.lane::int AS lane,
    scope.epoch::text AS epoch, delivery.claims AS claims, delivery.handler, delivery.key AS delivery_key, delivery.sequence::text AS sequence, (delivery.attempts + 1)::int AS attempt,
    ${eventColumns('event')}
  FROM (
    SELECT held_candidates.id, held_candidates.lane, held_candidates.available_at FROM held_candidates
    UNION ALL
    SELECT free_candidates.id, free_candidates.lane, free_candidates.available_at FROM free_candidates
  ) AS candidates
  JOIN claim_scope AS scope ON scope.lane = candidates.lane
  JOIN ${deliveries} AS delivery ON delivery.id = candidates.id
  JOIN ${this.#names.events} AS event ON event.id = delivery.event_id
), by_lane AS (
  SELECT picked_rows.lane, array_agg(picked_rows.delivery_id) AS ids FROM picked_rows GROUP BY picked_rows.lane
`;
  }

  /** Records the picks in the held lanes and takes the free lanes that yielded picks. */
  #claimRecord(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `), still_running AS (
  -- The retained ids come from the locked \`held\` rows, not from the lane table: a claim that waited for the lock
  -- must see the in-flight list the claim before it recorded, which the statement snapshot may not.
  SELECT held.lane, array_agg(delivery.id) AS ids
  FROM by_lane
  JOIN held ON held.lane = by_lane.lane
  CROSS JOIN LATERAL unnest(held.in_flight) AS flying (id)
  JOIN ${deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'
  GROUP BY held.lane
), extended AS (
  UPDATE ${lanes} AS lane
  SET in_flight = COALESCE(still_running.ids, '{}') || by_lane.ids,
      lease_expires_at = GREATEST(lane.lease_expires_at, $5::timestamptz)
  FROM by_lane
  JOIN held_scope ON held_scope.lane = by_lane.lane
  LEFT JOIN still_running ON still_running.lane = by_lane.lane
  WHERE lane.lane = by_lane.lane
  RETURNING lane.lane
), taken AS (
  UPDATE ${lanes} AS lane
  SET epoch = free_scope.epoch, lease_owner = $4::text, lease_expires_at = $5::timestamptz,
      in_flight = by_lane.ids, claim_floor = $6::bigint
  FROM free_scope
  JOIN by_lane ON by_lane.lane = free_scope.lane
  WHERE lane.lane = free_scope.lane
  RETURNING lane.lane
`;
  }

  /** Frees the held lanes that yielded nothing and run nothing, and selects the result rows. */
  #claimRelease(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    return `), saturated_lanes AS (
  SELECT held_window.lane FROM held_window GROUP BY held_window.lane HAVING count(*) >= ${LANE_SCAN_WINDOW}
), freed AS (
  UPDATE ${lanes} AS lane
  SET lease_owner = NULL, lease_expires_at = NULL, in_flight = '{}'
  FROM held
  WHERE lane.lane = held.lane
    AND lane.lane NOT IN (SELECT lane FROM by_lane)
    AND lane.lane NOT IN (SELECT lane FROM saturated_lanes)
    AND (SELECT count(*) FROM held_window) < ${CLAIM_SCAN_WINDOW}
    AND NOT (SELECT value FROM stale)
    AND ${noRunningDelivery(this.#names, 'lane')}
  RETURNING lane.lane
)
SELECT (SELECT value FROM stale) AS stale,
  EXISTS (SELECT 1 FROM free_lanes WHERE free_lanes.running) AS takeover_waiting,
  (SELECT count(*) FROM free_lanes) = (SELECT count FROM wanted) + ${FREE_LANE_SURPLUS} AS unexamined_free_lanes,
  (
    SELECT COALESCE(jsonb_agg(delivery.id), '[]'::jsonb)
    FROM held
    CROSS JOIN LATERAL unnest(held.in_flight) AS flying (id)
    JOIN ${deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'
  )::text AS running_ids,
  (SELECT COALESCE(jsonb_agg(held.lane), '[]'::jsonb) FROM held)::text AS examined_lanes,
  picked.*
FROM (SELECT 1) AS anchor
LEFT JOIN picked_rows AS picked ON true
ORDER BY picked.due_at, picked.delivery_id`;
  }

  /**
   * CTEs that pick up to `limit` claimable deliveries from the lanes of the CTE `scope` (columns `lane`,
   * `epoch`, `in_flight`), named after `prefix`; the result is `<prefix>_candidates (id, lane, available_at)`.
   * The rules are those of `ClaimStatements.claim`. The fast window reads the first {@link LANE_SCAN_WINDOW} due
   * deliveries of each lane (at most {@link CLAIM_SCAN_WINDOW} together); of a key, only the lowest sequence
   * in the window can be its head, so only that one is checked for an earlier pending delivery. Only when
   * that yields fewer than `limit` are the lanes whose window filled up looked at again with a loose index
   * scan over the heads of all their keys. `running` names the CTE of the keyed deliveries running in the
   * lanes, or is null when nothing runs there.
   */
  #candidates(prefix: string, scope: string, running: string | null, limit: string): string {
    const spec: CandidateSpec = { prefix, scope, running, limit };
    return this.#fastCandidates(spec) + this.#slowCandidates(spec);
  }

  /** The fast window of each lane and the claimable deliveries in it (`<prefix>_window`, `<prefix>_fast`). */
  #fastCandidates({ prefix, scope, running, limit }: CandidateSpec): string {
    const deliveries = this.#names.deliveries;
    return `${prefix}_window AS MATERIALIZED (
  SELECT recent.id, recent.lane, recent.available_at, recent.handler, recent.key, recent.sequence
  FROM ${scope}
  CROSS JOIN LATERAL (
    SELECT delivery.id, delivery.lane, delivery.available_at, delivery.handler, delivery.key, delivery.sequence
    FROM ${deliveries} AS delivery
    WHERE delivery.lane = ${scope}.lane AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
      AND delivery.id <> ALL (${scope}.in_flight)
    ORDER BY delivery.available_at, delivery.id
    LIMIT ${LANE_SCAN_WINDOW}
  ) AS recent
  ORDER BY recent.available_at, recent.id
  LIMIT ${CLAIM_SCAN_WINDOW}
), ${prefix}_fast AS MATERIALIZED (
  SELECT picked.id, picked.lane, picked.available_at FROM (
    SELECT unkeyed.id, unkeyed.lane, unkeyed.available_at FROM ${prefix}_window AS unkeyed WHERE unkeyed.key IS NULL
    UNION ALL
    SELECT head.id, head.lane, head.available_at
    FROM (
      SELECT DISTINCT ON (keyed.handler, keyed.key) keyed.id, keyed.lane, keyed.available_at, keyed.handler, keyed.key, keyed.sequence
      FROM ${prefix}_window AS keyed
      WHERE keyed.key IS NOT NULL
      ORDER BY keyed.handler, keyed.key, keyed.sequence
    ) AS head
    WHERE NOT EXISTS (
        SELECT 1 FROM ${deliveries} AS earlier
        WHERE earlier.lane = head.lane AND earlier.handler = head.handler AND earlier.key = head.key
          AND earlier.status = 'pending' AND earlier.key IS NOT NULL AND earlier.sequence < head.sequence
      ) AND ${notRunning(running, 'head')}
  ) AS picked
  ORDER BY picked.available_at, picked.id
  LIMIT ${limit}
`;
  }

  /**
   * For lanes whose window filled up while the fast pick came up short: the heads of all their keys and
   * their unkeyed deliveries, then the union with the fast pick (`<prefix>_candidates`).
   */
  #slowCandidates({ prefix, scope, running, limit }: CandidateSpec): string {
    const deliveries = this.#names.deliveries;
    return `), ${prefix}_saturated AS MATERIALIZED (
  SELECT ${scope}.lane, ${scope}.in_flight FROM ${scope}
  WHERE (SELECT count(*) FROM ${prefix}_fast) < ${limit}
    AND ((SELECT count(*) FROM ${prefix}_window) >= ${CLAIM_SCAN_WINDOW}
      OR ${scope}.lane IN (
        SELECT full_lane.lane FROM ${prefix}_window AS full_lane
        GROUP BY full_lane.lane HAVING count(*) >= ${LANE_SCAN_WINDOW}
      ))
), ${prefix}_heads AS MATERIALIZED (
  SELECT head.id, head.lane, head.available_at, head.handler, head.key
  FROM ${prefix}_saturated AS scope
  CROSS JOIN LATERAL (${keyHeads(this.#names, 'scope')}) AS head
), ${prefix}_keyed AS MATERIALIZED (
  SELECT heads.id, heads.lane, heads.available_at
  FROM ${prefix}_heads AS heads
  JOIN ${prefix}_saturated AS scope ON scope.lane = heads.lane
  WHERE heads.available_at <= $1::timestamptz AND heads.id <> ALL (scope.in_flight) AND ${notRunning(running, 'heads')}
  ORDER BY heads.available_at, heads.id
  LIMIT ${limit}
), ${prefix}_unkeyed AS MATERIALIZED (
  SELECT recent.id, recent.lane, recent.available_at
  FROM ${prefix}_saturated AS scope
  CROSS JOIN LATERAL (
    SELECT delivery.id, delivery.lane, delivery.available_at
    FROM ${deliveries} AS delivery
    WHERE delivery.lane = scope.lane AND delivery.key IS NULL AND delivery.status = 'pending'
      AND delivery.available_at <= $1::timestamptz AND ${HANDLER_PREDICATE}
      AND delivery.id <> ALL (scope.in_flight)
    ORDER BY delivery.available_at, delivery.id
    LIMIT ${limit}
  ) AS recent
  ORDER BY recent.available_at, recent.id
  LIMIT ${limit}
), ${prefix}_candidates AS MATERIALIZED (
  SELECT chosen.id, chosen.lane, chosen.available_at FROM (
    SELECT fast.id, fast.lane, fast.available_at FROM ${prefix}_fast AS fast
    UNION
    SELECT keyed.id, keyed.lane, keyed.available_at FROM ${prefix}_keyed AS keyed
    UNION
    SELECT unkeyed.id, unkeyed.lane, unkeyed.available_at FROM ${prefix}_unkeyed AS unkeyed
  ) AS chosen
  ORDER BY chosen.available_at, chosen.id
  LIMIT ${limit}
)`;
  }

  /**
   * Takes up to `$3` free lanes (no lease, or an expired one) that have a due
   * delivery of the requested handlers, the ones with the oldest due delivery
   * first. Parameters: `[now, handlersJson, laneCount, leaseOwner, leaseExpiresAt, generation]`.
   * The lane gets the owner's `generation` as its `claim_floor`. A lane the owner itself holds under a live
   * lease with a lower floor was taken by a claim statement that reached the server after its caller gave up
   * and the owner reconciled (or by an earlier incarnation of the same owner): it is taken over like the lane of a
   * crashed worker, so its listed deliveries are charged an attempt.
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
  -- in_flight is read here, from the locked version of the lane: a settlement that committed after the
  -- statement took its snapshot has taken its delivery out of the list already and must not be charged again.
  SELECT free.lane, free.in_flight
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
    OR (free.lease_owner = $4::text AND free.claim_floor < $6::bigint)
  ORDER BY oldest.available_at, oldest.id
  LIMIT $3::int
  FOR UPDATE OF free SKIP LOCKED
), charged AS (
  UPDATE ${deliveries} AS delivery SET attempts = delivery.attempts + 1, claims = delivery.claims + 1
  FROM (
    SELECT unnest(picked.in_flight) AS id FROM picked
  ) AS lost
  WHERE delivery.id = lost.id AND delivery.status = 'pending'
  RETURNING delivery.id
), taken AS (
  UPDATE ${lanes} AS lane
  SET epoch = lane.epoch + 1, lease_owner = $4::text, lease_expires_at = $5::timestamptz, in_flight = '{}',
      claim_floor = $6::bigint
  FROM picked WHERE lane.lane = picked.lane
  RETURNING lane.lane, lane.epoch
)
SELECT lane::int AS lane, epoch::text AS epoch FROM taken ORDER BY lane`;
  }
}
