import { LANE_COUNT, type PostgresNames } from './names.js';

/**
 * Renders a timestamptz as ISO-8601 text in UTC. Reading timestamps as text
 * makes results independent of the driver's type parsers (Drizzle, for
 * instance, replaces them) and of the session time zone.
 */
export function isoText(expression: string): string {
  return `to_char(${expression} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
}

/**
 * The lane of a delivery: a hash of the ordering key, or of the event id when
 * the event has no key. All deliveries of one key therefore share a lane and
 * are owned by one worker at a time, which is what keeps their order.
 * `hashtextextended` is stable across PostgreSQL versions and platforms.
 */
export function laneExpression(keyExpression: string, eventIdExpression: string): string {
  return `(hashtextextended(COALESCE(${keyExpression}, ${eventIdExpression}), 0) & ${LANE_COUNT - 1})::smallint`;
}

/** The first lane of the block of each store's lane scope, once {@link PostgresLaneScope} has it. */
const laneBlockStarts = new WeakMap<PostgresNames, number>();

/** Records `first` as the first lane of the block of the lane scope of `names`. */
export function setLaneBlockStart(names: PostgresNames, first: number): void {
  laneBlockStarts.set(names, first);
}

/**
 * The first lane of the store's scope (see {@link PostgresNames.laneScope}) as a literal: 0 for the
 * default scope, else the first lane of the scope's block. A literal needs no lookup in the statement,
 * so a statement whose snapshot predates the block, or that runs in a caller's transaction, sees it too.
 */
export function laneBlockStart(names: PostgresNames): string {
  if (names.laneScope === undefined) {
    return '0';
  }
  const first = laneBlockStarts.get(names);
  if (first === undefined) {
    throw new Error(`lane scope '${names.laneScope}' has no block yet`);
  }
  return String(first);
}

/**
 * Whether the store claims from lane `alias.lane`: the {@link LANE_COUNT} lanes from {@link laneBlockStart}
 * and, for a scope other than the default, the default lanes too. A worker takes a lane only while it
 * has due deliveries of the worker's handlers, so a store of another scope takes a default lane only to
 * finish the deliveries its service wrote there before it moved to its scope.
 */
export function laneInScope(names: PostgresNames, alias: string): string {
  const start = laneBlockStart(names);
  return names.laneScope === undefined
    ? `${alias}.lane <= ${LANE_COUNT - 1}`
    : `(${alias}.lane <= ${LANE_COUNT - 1} OR ${alias}.lane BETWEEN ${start} AND ${start} + ${LANE_COUNT - 1})`;
}

/**
 * The lane of a delivery the store writes: {@link laneExpression} counted from the first lane of the
 * store's scope. Every delivery of one key the store writes shares one lane.
 */
export function scopedLaneExpression(
  names: PostgresNames,
  { key, eventId }: { readonly key: string; readonly eventId: string },
): string {
  if (names.laneScope === undefined) {
    return laneExpression(key, eventId);
  }
  return `(${laneBlockStart(names)} + (hashtextextended(COALESCE(${key}, ${eventId}), 0) & ${LANE_COUNT - 1}))::smallint`;
}

/**
 * For a store of a scope other than the default, a condition true when a keyed delivery `alias` must
 * wait for its key's deliveries in the other lane of its key: the default lane, where its service wrote
 * them before it moved to its scope, or the scope's lane, for a delivery of the default lane. It waits
 * while one of them with a lower sequence is pending, or one of them is in flight in that lane (a
 * requeued dead letter never runs next to a later delivery of its key). `sequence` is the delivery's
 * sequence. Null for the default scope, whose deliveries of a key share one lane.
 */
export function blockedInOtherLane(
  names: PostgresNames,
  { alias, sequence }: { readonly alias: string; readonly sequence: string },
): string | null {
  if (names.laneScope === undefined) {
    return null;
  }
  const start = laneBlockStart(names);
  const otherLane = `CASE WHEN ${alias}.lane <= ${LANE_COUNT - 1} THEN ${alias}.lane + ${start} ELSE ${alias}.lane - ${start} END`;
  return `EXISTS (
        SELECT 1 FROM ${names.deliveries} AS other_lane
        WHERE other_lane.lane = ${otherLane} AND other_lane.handler = ${alias}.handler AND other_lane.key = ${alias}.key
          AND other_lane.status = 'pending'
          AND (other_lane.sequence < ${sequence} OR ARRAY[other_lane.id] <@ (
            SELECT other_lane_row.in_flight FROM ${names.lanes} AS other_lane_row WHERE other_lane_row.lane = ${otherLane}
          ))
      )`;
}
