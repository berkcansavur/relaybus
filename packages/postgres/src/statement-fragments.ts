import type { PostgresParameter } from './executor.js';
import type { PostgresNames } from './names.js';
import { isoText } from './sql-fragments.js';

export interface PostgresStatement {
  readonly sql: string;
  readonly parameters: readonly PostgresParameter[];
}

/**
 * Due deliveries the fast claim examines before it gives up and lets the slow
 * claim look at the heads of all keys.
 */
export const CLAIM_SCAN_WINDOW = 256;

/** Due deliveries the fast claim reads from one lane. A lane that fills it may hide claimable deliveries behind blocked followers. */
export const LANE_SCAN_WINDOW = 64;

/**
 * Due deliveries of a free lane a claim looks at when it decides how many lanes to take: their unkeyed
 * deliveries and distinct keys estimate what the lane can give, so a deep backlog is taken a few lanes
 * at a time and a lane of few hot keys counts for no more than its keys.
 */
export const LANE_DEPTH_CAP = 16;

/** Free lanes a claim that needs more takes at least (those with the oldest due deliveries), as in its first round. */
export const INITIAL_FREE_LANES = 4;

export const HANDLER_PREDICATE = `delivery.handler = ANY (ARRAY(SELECT jsonb_array_elements_text($2::jsonb)))`;

export function eventColumns(alias: string): string {
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
export function countChanged(dml: string): string {
  return `WITH changed AS (${dml} RETURNING 1) SELECT count(*)::int AS count FROM changed`;
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
export function keyHeads(names: PostgresNames, scope: string): string {
  const deliveries = names.deliveries;
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

/** True when none of the lane's listed deliveries exists as a pending row. */
export function noRunningDelivery(names: PostgresNames, laneAlias: string): string {
  return `NOT EXISTS (
      SELECT 1 FROM unnest(${laneAlias}.in_flight) AS flying (id)
      JOIN ${names.deliveries} AS delivery ON delivery.id = flying.id AND delivery.status = 'pending'
    )`;
}
