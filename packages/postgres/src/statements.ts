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

/**
 * Due deliveries of a free lane a claim looks at when it decides how many lanes to take: their unkeyed
 * deliveries and distinct keys estimate what the lane can give, so a deep backlog is taken a few lanes
 * at a time and a lane of few hot keys counts for no more than its keys.
 */
const LANE_DEPTH_CAP = 16;

/** Free lanes a claim that needs more takes at least (those with the oldest due deliveries), as in its first round. */
const INITIAL_FREE_LANES = 4;

/**
 * Free lanes a claim looks at beyond the number of deliveries it still wants, so lanes whose due
 * deliveries all wait behind a key's later head do not hide the others. When even these are not enough
 * the claim continues in the transaction that takes lanes round by round.
 */
const FREE_LANE_SURPLUS = 16;

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
 *
 * **Lock order.** Every statement that can wait for a lock takes its locks in this order, and rows of
 * one table in key order, so no two of them can wait for each other in a cycle:
 *
 * 1. the setup advisory lock and the schema DDL ({@link setupLock}); only while the schema is created or
 *    upgraded, never on a current schema (see `PostgresEventStore.setup`);
 * 2. `key_sequences` rows, in key order ({@link insertFreshEvents});
 * 3. `events` rows, in id order ({@link lockExistingEvents}, the inserts of a publish, the foreign key
 *    checks of new deliveries and completions);
 * 4. `lanes` rows, in lane order: `FOR NO KEY UPDATE` for the owner's held lanes, `FOR KEY SHARE` for
 *    completions, `FOR UPDATE SKIP LOCKED` (never waits) for lanes taken from others;
 * 5. `deliveries` rows, only of lanes locked in step 4 (claims, completions, reschedules, takeovers),
 *    or inserted new (publish), or `dead` ones in id order ({@link requeue});
 * 6. `event_completions` rows, in event id order ({@link complete}).
 *
 * Publishing never touches `lanes`, and dispatching never touches `key_sequences`. Purging only takes
 * `SKIP LOCKED` row locks and then deletes rows nobody references, so it never waits in a cycle either.
 * A caller's own transaction (a transactional handler that completes and publishes) adds the locks of
 * its own statements in the order it runs them; as documented on `append`, keys appended in opposite
 * orders by two such transactions can still deadlock, and the database then rolls one back.
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

  /**
   * Whether the lane table carries `marker` as its comment, that is whether the schema statements
   * that produce that marker already ran. Takes no table lock. Parameters: `[marker]`.
   */
  get schemaCurrent(): string {
    return `SELECT coalesce(obj_description(to_regclass('${this.#names.lanes}'), 'pg_class') = $1::text, false) AS current`;
  }

  get setupLock(): string {
    return 'SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))';
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
   * 3. **Picks** follow the rules of {@link claim}: the fast window of each lane, and for lanes whose
   *    window filled up while the batch is not full, the heads of all keys (a loose index scan per lane).
   * 4. **Writes**: one update per lane. A held lane records its picks; a taken lane gets a new epoch, the
   *    owner, the lease and its picks; a taken lane that yielded nothing is not written. Held lanes that
   *    picked nothing, run nothing and have no full scan window are freed.
   *
   * Always returns one row per picked delivery, or one row whose `delivery_id` is null. `running_ids`
   * (JSON text) lists the deliveries the held lanes still run, `examined_lanes` the held lanes.
   */
  get claimLanes(): string {
    const deliveries = this.#names.deliveries;
    const lanes = this.#names.lanes;
    const freeLane = (alias: string): string =>
      `(${alias}.lease_expires_at IS NULL OR ${alias}.lease_expires_at <= $1::timestamptz
      OR (${alias}.lease_owner = $4::text AND ${alias}.claim_floor < $6::bigint))`;
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
), free_lanes AS MATERIALIZED (
  -- The oldest due delivery of each free lane: one index probe per free lane, bounded by the due time,
  -- so deliveries scheduled for later are never read.
  SELECT lane.lane, lane.epoch, lane.in_flight, lane.lease_owner, lane.lease_expires_at,
    oldest.available_at, oldest.id, NOT ${this.#noRunningDelivery('lane')} AS running
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
), free_depth AS MATERIALIZED (
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
), free_locked AS MATERIALIZED (
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
), ${this.#candidates('free', 'free_scope', null, '(SELECT count FROM wanted)')}, claim_scope AS MATERIALIZED (
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
), still_running AS (
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
), saturated_lanes AS (
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
    AND ${this.#noRunningDelivery('lane')}
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
   * The head (lowest pending sequence) of every key of every wanted handler in the lane `scope.lane`,
   * as a subquery returning `(id, lane, available_at, handler, key)`. A loose index scan on the
   * pending-key index hops from key to key of a handler, and after its last key on to the first key of
   * the next wanted handler, so the cost follows the number of keys, not the number of followers. The
   * handlers are walked inside the recursion instead of joined to it: a join multiplied the planner's
   * cost estimate by the handler count, which pushed the claim statements over `jit_above_cost`, and
   * compiling them took longer than running them.
   */
  #keyHeads(scope: string): string {
    const deliveries = this.#names.deliveries;
    // `key IS NOT NULL` is spelled out only where nothing else implies it: next to `key > ...` or
    // `key = ...` the planner proves the partial index from those, and with the column all null in the
    // statistics the extra clause made it expect one row behind many index entries and cost every hop as a
    // full range scan.
    const pending = `candidate.lane = ${scope}.lane AND candidate.status = 'pending'`;
    const nextHandler = (after: string): string =>
      `(SELECT min(wanted.handler) FROM jsonb_array_elements_text($2::jsonb) AS wanted (handler)${after === '' ? '' : ` WHERE wanted.handler > ${after}`})`;
    return `
    WITH RECURSIVE hops (handler, key) AS (
      SELECT ${nextHandler('')}, NULL::text
      UNION ALL
      SELECT CASE WHEN next.key IS NULL THEN ${nextHandler('hops.handler')} ELSE hops.handler END, next.key
      FROM hops
      CROSS JOIN LATERAL (
        SELECT CASE WHEN hops.key IS NULL
          THEN (SELECT candidate.key FROM ${deliveries} AS candidate
                WHERE ${pending} AND candidate.handler = hops.handler AND candidate.key IS NOT NULL
                ORDER BY candidate.key LIMIT 1)
          ELSE (SELECT candidate.key FROM ${deliveries} AS candidate
                WHERE ${pending} AND candidate.handler = hops.handler AND candidate.key > hops.key
                ORDER BY candidate.key LIMIT 1)
        END AS key
        OFFSET 0
      ) AS next
      WHERE hops.handler IS NOT NULL
    )
    SELECT lowest.id, lowest.lane, lowest.available_at, lowest.handler, lowest.key
    FROM hops
    CROSS JOIN LATERAL (
      SELECT candidate.id, candidate.lane, candidate.available_at, candidate.handler, candidate.key
      FROM ${deliveries} AS candidate
      WHERE ${pending} AND candidate.handler = hops.handler AND candidate.key = hops.key
      ORDER BY candidate.sequence LIMIT 1
    ) AS lowest
    WHERE hops.key IS NOT NULL
  `;
  }

  /**
   * CTEs that pick up to `limit` claimable deliveries from the lanes of the CTE `scope` (columns `lane`,
   * `epoch`, `in_flight`), named after `prefix`; the result is `<prefix>_candidates (id, lane, available_at)`.
   * The rules are those of {@link claim}. The fast window reads the first {@link LANE_SCAN_WINDOW} due
   * deliveries of each lane (at most {@link CLAIM_SCAN_WINDOW} together); of a key, only the lowest sequence
   * in the window can be its head, so only that one is checked for an earlier pending delivery. Only when
   * that yields fewer than `limit` are the lanes whose window filled up looked at again with a loose index
   * scan over the heads of all their keys. `running` names the CTE of the keyed deliveries running in the
   * lanes, or is null when nothing runs there.
   */
  #candidates(prefix: string, scope: string, running: string | null, limit: string): string {
    const deliveries = this.#names.deliveries;
    const notRunning = (alias: string): string =>
      running === null
        ? 'true'
        : `NOT EXISTS (SELECT 1 FROM ${running} AS running WHERE running.handler = ${alias}.handler AND running.key = ${alias}.key)`;
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
      ) AND ${notRunning('head')}
  ) AS picked
  ORDER BY picked.available_at, picked.id
  LIMIT ${limit}
), ${prefix}_saturated AS MATERIALIZED (
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
  CROSS JOIN LATERAL (${this.#keyHeads('scope')}) AS head
), ${prefix}_keyed AS MATERIALIZED (
  SELECT heads.id, heads.lane, heads.available_at
  FROM ${prefix}_heads AS heads
  JOIN ${prefix}_saturated AS scope ON scope.lane = heads.lane
  WHERE heads.available_at <= $1::timestamptz AND heads.id <> ALL (scope.in_flight) AND ${notRunning('heads')}
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
  CROSS JOIN LATERAL (${this.#keyHeads('scope')}) AS head
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
WHERE lane.lane = owned.lane AND lane.lease_owner = $1::text AND ${this.#noRunningDelivery('lane')}`;
  }

  /**
   * Extends the lease of the lane a running delivery belongs to; the lane lease
   * never shrinks. Parameters: `[deliveryId, lane, epoch, claims, leaseExpiresAt]`.
   */
  get extendLease(): string {
    return countChanged(`UPDATE ${this.#names.lanes} AS lane
SET lease_expires_at = GREATEST(lane.lease_expires_at, $5::timestamptz)
WHERE lane.lane = $2::smallint AND lane.epoch = $3::bigint AND $1::text = ANY (lane.in_flight)
  AND EXISTS (
    SELECT 1 FROM ${this.#names.deliveries} AS delivery
    WHERE delivery.id = $1::text AND delivery.status = 'pending' AND delivery.claims = $4::int
  )`);
  }

  /**
   * Deletes the running deliveries whose lane lease is still the one they were
   * claimed under. Parameters: `[leasesJson, now]` with
   * `leasesJson = [{ ord, id, lane, epoch, claims }]`. Returns the `ord` of every lease that was completed, so
   * results are matched to leases and not to delivery ids. A delivery is only deleted by the claim that
   * is still its current one (`claims`), which makes a stale lease harmless even when the same owner re-claimed
   * the delivery in the same lane epoch. The event gets a completion time for retention.
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
  SELECT lease.ord, lease.id, lease.lane, lease.epoch, lease.claims
  FROM jsonb_to_recordset($1::jsonb) AS lease (ord int, id text, lane smallint, epoch bigint, claims int)
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
    AND delivery.claims = leases.claims AND leases.id = ANY (fence.in_flight)
  RETURNING leases.ord, delivery.id, delivery.event_id, delivery.handler, delivery.attempts, delivery.available_at, delivery.created_at
), stamped AS (
  INSERT INTO ${this.#names.eventCompletions} (event_id, completed_at)
  SELECT DISTINCT completed.event_id, $2::timestamptz FROM completed
  -- Event id order: completions of other handlers of the same events lock these rows too.
  ORDER BY completed.event_id
  ON CONFLICT (event_id) DO UPDATE SET completed_at = GREATEST(${this.#names.eventCompletions}.completed_at, EXCLUDED.completed_at)
  RETURNING event_id
)${
      history
        ? `, archived AS (
  INSERT INTO ${this.#names.completions} (id, event_id, handler, attempts, available_at, created_at, completed_at)
  SELECT id, event_id, handler, attempts + 1, available_at, created_at, $2::timestamptz FROM completed
  RETURNING id
)`
        : ''
    }
SELECT ord FROM completed`;
  }

  /**
   * Returns a running delivery to pending, due at `availableAt`, and takes it
   * out of the lane's in-flight list. Parameters:
   * `[deliveryId, lane, epoch, claims, availableAt, errorJson, now]`. The attempt that
   * just ended is counted.
   */
  get reschedule(): string {
    return this.#leaveFlight(
      `SET available_at = $5::timestamptz, last_error = $6::jsonb, attempts = delivery.attempts + 1, claims = delivery.claims + 1, updated_at = $7::timestamptz`,
    );
  }

  /** Parameters: `[deliveryId, lane, epoch, claims, errorJson, now]`. The attempt that just ended is counted. */
  get bury(): string {
    return this.#leaveFlight(
      `SET status = 'dead', last_error = $5::jsonb, attempts = delivery.attempts + 1, claims = delivery.claims + 1, updated_at = $6::timestamptz`,
    );
  }

  /**
   * Gives a running delivery back without spending its attempt (an attempt is
   * only recorded when it ends in failure or a lost lease): pending, due `now`.
   * Parameters: `[deliveryId, lane, epoch, claims, now]`.
   */
  get release(): string {
    return this.#leaveFlight(
      `SET available_at = $5::timestamptz, claims = delivery.claims + 1, updated_at = $5::timestamptz`,
    );
  }

  /**
   * Applies `assignments` to a running delivery and removes it from its lane's
   * in-flight list, if the lane still has the caller's epoch. The lane row is
   * locked first (`FOR NO KEY UPDATE`, which conflicts with a takeover's `FOR UPDATE`), so a takeover cannot interleave. Parameters
   * `$1`..`$4` are always `[deliveryId, lane, epoch, claims]`; ending a claim bumps `claims`.
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
    AND delivery.claims = $4::int
  RETURNING delivery.id
), trimmed AS (
  UPDATE ${lanes} AS lane SET in_flight = array_remove(lane.in_flight, $1::text)
  FROM changed WHERE lane.lane = $2::smallint
  RETURNING lane.lane
)
SELECT count(*)::int AS count FROM changed`;
  }

  /** Parameters: `[deliveryIdsJson, now]`. The dead rows are locked in id order first. */
  get requeue(): string {
    const deliveries = this.#names.deliveries;
    return countChanged(`WITH dead AS MATERIALIZED (
  SELECT delivery.id FROM ${deliveries} AS delivery
  WHERE delivery.id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb))) AND delivery.status = 'dead'
  ORDER BY delivery.id
  FOR NO KEY UPDATE
)
UPDATE ${deliveries} AS delivery
SET status = 'pending', attempts = 0, available_at = $2::timestamptz, updated_at = $2::timestamptz
FROM dead
WHERE delivery.id = dead.id AND delivery.status = 'dead'`);
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
   * that no delivery references and that never had a delivery complete (those
   * are measured from their completion, see {@link lockCompletedOrphanEventsBatch}).
   * Parameters: `[cutoff, batchSize]`. The ids go through
   * {@link deleteOrphanEventsWithoutCompletion} or {@link deleteExpiredCompletedOrphanEvents}, which re-check under a fresh snapshot.
   */
  get lockOrphanEventsBatch(): string {
    return `SELECT event.id FROM ${this.#names.events} AS event
WHERE event.created_at < $1::timestamptz
  AND ${this.#unreferenced('event')}
  AND NOT EXISTS (SELECT 1 FROM ${this.#names.eventCompletions} AS completion WHERE completion.event_id = event.id)
ORDER BY event.created_at, event.id
LIMIT $2::int
FOR UPDATE OF event SKIP LOCKED`;
  }

  /**
   * Like {@link lockOrphanEventsBatch} for events that had a delivery complete:
   * they go once their last completion is older than the cutoff, however old the
   * event itself is. Parameters: `[cutoff, batchSize]`.
   */
  get lockCompletedOrphanEventsBatch(): string {
    return `SELECT event.id FROM ${this.#names.events} AS event
JOIN ${this.#names.eventCompletions} AS completion ON completion.event_id = event.id
WHERE completion.completed_at < $1::timestamptz
  AND ${this.#unreferenced('event')}
ORDER BY completion.completed_at, event.id
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
   * Deletes events that no delivery references and whose last completion is older than
   * `$2`, checked again under the fresh snapshot of this statement: a completion recorded
   * after the sweep selected the event moves its time and keeps the event. Parameters:
   * `[eventIdsJson, cutoff]`.
   */
  get deleteExpiredCompletedOrphanEvents(): string {
    return countChanged(`DELETE FROM ${this.#names.events} AS event
WHERE event.id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
  AND ${this.#unreferenced('event')}
  AND EXISTS (
    SELECT 1 FROM ${this.#names.eventCompletions} AS completion
    WHERE completion.event_id = event.id AND completion.completed_at < $2::timestamptz
  )`);
  }

  /**
   * Deletes locked events that no delivery references and that never had a delivery complete.
   * It must run as a statement of its own after {@link lockEvents}: it then takes a fresh
   * snapshot that includes every delivery committed before the lock was granted, so an event
   * that just gained a delivery is never cascaded away. Keeps events that had a delivery complete:
   * deleting dead letters must not shorten the retention of completed work.
   */
  get deleteOrphanEventsWithoutCompletion(): string {
    return countChanged(`DELETE FROM ${this.#names.events} AS event
WHERE event.id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
  AND ${this.#unreferenced('event')}
  AND NOT EXISTS (SELECT 1 FROM ${this.#names.eventCompletions} AS completion WHERE completion.event_id = event.id)`);
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
