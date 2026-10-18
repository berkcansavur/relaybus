/**
 * The instant a delivery becomes claimable: `available_at` while pending,
 * `lease_expires_at` while processing. The claim query, the statistics query
 * and the partial index all use this exact expression, which is what lets
 * PostgreSQL answer "earliest due delivery" from an index.
 */
export function dueAtExpression(alias?: string): string {
  const prefix = alias === undefined ? '' : `${alias}.`;
  return `CASE WHEN ${prefix}status = 'pending' THEN ${prefix}available_at ELSE ${prefix}lease_expires_at END`;
}

/**
 * Renders a timestamptz as ISO-8601 text in UTC. Reading timestamps as text
 * makes results independent of the driver's type parsers (Drizzle, for
 * instance, replaces them) and of the session time zone.
 */
export function isoText(expression: string): string {
  return `to_char(${expression} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
}
