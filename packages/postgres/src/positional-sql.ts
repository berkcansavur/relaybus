/** A piece of SQL text, or a reference to the (zero-based) parameter that stands at this position. */
export type PositionalSqlPart = string | { readonly parameterIndex: number };

/**
 * Splits SQL written with PostgreSQL `$1, $2, ...` placeholders into text and
 * parameter references, in order of appearance. A placeholder used twice
 * appears twice. Adapters for libraries with their own placeholder syntax
 * (Knex `?`, Drizzle parameter chunks) rebuild the statement from the parts.
 */
export function splitPositionalSql(sql: string): PositionalSqlPart[] {
  const parts: PositionalSqlPart[] = [];
  let textStart = 0;
  for (const match of sql.matchAll(/\$(\d+)/g)) {
    parts.push(sql.slice(textStart, match.index), { parameterIndex: Number(match[1]) - 1 });
    textStart = match.index + match[0].length;
  }
  parts.push(sql.slice(textStart));
  return parts;
}
