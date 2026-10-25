import type { DeliveryQuery } from '@relaybus/core';
import type { PostgresParameter } from './executor.js';
import type { PostgresNames } from './names.js';
import { isoText } from './sql-fragments.js';
import {
  countChanged,
  eventColumns,
  noRunningDelivery,
  type PostgresStatement,
} from './statement-fragments.js';

const DEFAULT_LIST_LIMIT = 100;

/** The bound placeholders of the filters of a listing; undefined where the query has none. */
type ListFilters = {
  readonly handler: string | undefined;
  readonly eventType: string | undefined;
  readonly eventId: string | undefined;
  readonly status: string | undefined;
};

/** Binds the filters, then the limit, as parameters in that order. */
function bindListFilters(query: DeliveryQuery): {
  parameters: PostgresParameter[];
  filters: ListFilters;
  limit: string;
} {
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
  return { parameters, filters, limit };
}

/** The conditions of the handler, event type and event id filters on the rows of `alias`. */
function listConditions(filters: ListFilters, alias: string): string[] {
  return [
    ...(filters.handler === undefined ? [] : [`${alias}.handler = ${filters.handler}`]),
    ...(filters.eventType === undefined ? [] : [`event.type = ${filters.eventType}`]),
    ...(filters.eventId === undefined ? [] : [`${alias}.event_id = ${filters.eventId}`]),
  ];
}

/** Statements for listing, requeueing and statistics. */
export class QueryStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
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
    const { parameters, filters, limit } = bindListFilters(query);
    const current = this.#currentDeliveries(filters);
    const includeHistory =
      this.#names.completionHistory && (query.status === undefined || query.status === 'completed');
    if (!includeHistory) {
      return {
        sql: `${current}\nORDER BY delivery.created_at, delivery.id\nLIMIT ${limit}`,
        parameters,
      };
    }
    const history = this.#completionHistory(filters);
    return {
      sql: `SELECT * FROM (\n${current}\nUNION ALL\n${history}\n) AS listed\nORDER BY created_at, delivery_id\nLIMIT ${limit}`,
      parameters,
    };
  }

  /** The deliveries that still exist, with a running one reported as `processing`. */
  #currentDeliveries(filters: ListFilters): string {
    const inFlight = `(delivery.status = 'pending' AND delivery.id = ANY (lane.in_flight))`;
    const whereCurrent = [
      ...listConditions(filters, 'delivery'),
      ...(filters.status === undefined
        ? []
        : [
            `(CASE WHEN ${inFlight} THEN 'processing' ELSE delivery.status END) = ${filters.status}`,
          ]),
    ];
    return `SELECT delivery.id AS delivery_id, delivery.handler,
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
  }

  /** The completion history records, listed as `completed`. */
  #completionHistory(filters: ListFilters): string {
    const common = (alias: string): string[] => listConditions(filters, alias);
    return `SELECT done.id AS delivery_id, done.handler, 'completed' AS status, done.attempts,
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
  WHERE lane.lease_expires_at <= $1::timestamptz AND NOT ${noRunningDelivery(this.#names, 'lane')}
) AS due`;
  }
}
