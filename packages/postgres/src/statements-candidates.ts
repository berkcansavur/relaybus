import type { PostgresNames } from './names.js';
import {
  CLAIM_SCAN_WINDOW,
  HANDLER_PREDICATE,
  LANE_SCAN_WINDOW,
  keyHeads,
} from './statement-fragments.js';

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

/** Statements that pick the claimable deliveries of a set of lanes, as CTEs a claim embeds. */
export class CandidateStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
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
  candidates(
    prefix: string,
    {
      scope,
      running,
      limit,
    }: { readonly scope: string; readonly running: string | null; readonly limit: string },
  ): string {
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
  #slowCandidates(spec: CandidateSpec): string {
    return (
      this.#saturatedLanes(spec) +
      this.#keyHeads(spec) +
      this.#keyedHeads(spec) +
      this.#unkeyedRecent(spec) +
      this.#unionCandidates(spec)
    );
  }

  /** `<prefix>_saturated`: the lanes whose window filled up while the fast pick came up short. */
  #saturatedLanes({ prefix, scope, limit }: CandidateSpec): string {
    return `), ${prefix}_saturated AS MATERIALIZED (
  SELECT ${scope}.lane, ${scope}.in_flight FROM ${scope}
  WHERE (SELECT count(*) FROM ${prefix}_fast) < ${limit}
    AND ((SELECT count(*) FROM ${prefix}_window) >= ${CLAIM_SCAN_WINDOW}
      OR ${scope}.lane IN (
        SELECT full_lane.lane FROM ${prefix}_window AS full_lane
        GROUP BY full_lane.lane HAVING count(*) >= ${LANE_SCAN_WINDOW}
      ))
`;
  }

  /** `<prefix>_heads`: the head of every key of the saturated lanes. */
  #keyHeads({ prefix }: CandidateSpec): string {
    return `), ${prefix}_heads AS MATERIALIZED (
  SELECT head.id, head.lane, head.available_at, head.handler, head.key
  FROM ${prefix}_saturated AS scope
  CROSS JOIN LATERAL (${keyHeads(this.#names, 'scope')}) AS head
`;
  }

  /** `<prefix>_keyed`: the due heads that are not in flight and whose key runs nowhere. */
  #keyedHeads({ prefix, running, limit }: CandidateSpec): string {
    return `), ${prefix}_keyed AS MATERIALIZED (
  SELECT heads.id, heads.lane, heads.available_at
  FROM ${prefix}_heads AS heads
  JOIN ${prefix}_saturated AS scope ON scope.lane = heads.lane
  WHERE heads.available_at <= $1::timestamptz AND heads.id <> ALL (scope.in_flight) AND ${notRunning(running, 'heads')}
  ORDER BY heads.available_at, heads.id
  LIMIT ${limit}
`;
  }

  /** `<prefix>_unkeyed`: the due unkeyed deliveries of the saturated lanes that are not in flight. */
  #unkeyedRecent({ prefix, limit }: CandidateSpec): string {
    const deliveries = this.#names.deliveries;
    return `), ${prefix}_unkeyed AS MATERIALIZED (
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
`;
  }

  /** `<prefix>_candidates`: the union of the fast pick and the slow picks, oldest first, at most `limit`. */
  #unionCandidates({ prefix, limit }: CandidateSpec): string {
    return `), ${prefix}_candidates AS MATERIALIZED (
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
}
