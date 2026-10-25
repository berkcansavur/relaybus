import type { PostgresNames } from './names.js';
import { countChanged } from './statement-fragments.js';

/** Fenced statements that end or extend a claim. */
export class CompletionStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
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
}
