import { adapterExecutor } from '../adapter-executor.js';
import { sql as drizzleSql, type SQL } from 'drizzle-orm';
import type { PostgresExecutor } from '../executor.js';
import { splitPositionalSql } from '../positional-sql.js';

/** What a Drizzle database and a Drizzle transaction have in common. */
export interface DrizzleQueryable {
  execute(query: SQL): PromiseLike<{ readonly rows: Record<string, unknown>[] }>;
}

/** The part of a Drizzle `node-postgres` database the executor uses. */
export interface DrizzleLike extends DrizzleQueryable {
  transaction<T>(work: (transaction: DrizzleQueryable) => Promise<T>): Promise<T>;
}

/**
 * Executor for Drizzle with the `node-postgres` driver. The transaction handle
 * is a Drizzle transaction: pass `tx` from `db.transaction(async (tx) => ...)`.
 */
export function createDrizzleExecutor(database: DrizzleLike): PostgresExecutor<DrizzleQueryable> {
  return adapterExecutor<DrizzleQueryable>({
    root: database,
    transaction: (work) => database.transaction(work),
    async run(queryable, { sql, parameters }) {
      const chunks = splitPositionalSql(sql).map((part) =>
        typeof part === 'string'
          ? drizzleSql.raw(part)
          : drizzleSql.param(parameters[part.parameterIndex]),
      );
      const result = await queryable.execute(drizzleSql.join(chunks));
      return result.rows;
    },
  });
}
