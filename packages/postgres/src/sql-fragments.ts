import { LANE_COUNT } from './names.js';

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
