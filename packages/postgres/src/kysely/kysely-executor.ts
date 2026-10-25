import { CompiledQuery } from 'kysely';
import type { PostgresExecutor, PostgresParameter, PostgresRow } from '../executor.js';

/** What a Kysely instance and a Kysely transaction have in common. */
export interface KyselyQueryable {
  executeQuery<R>(query: CompiledQuery<R>): Promise<{ readonly rows: R[] }>;
}

/** The part of a `Kysely` instance the executor uses. */
export interface KyselyLike extends KyselyQueryable {
  transaction(): {
    execute<T>(work: (transaction: KyselyQueryable) => Promise<T>): Promise<T>;
  };
}

/**
 * Executor for Kysely. The transaction handle is a Kysely `Transaction`: pass
 * `trx` from `db.transaction().execute(async (trx) => ...)` as the
 * `transaction` option of `publish`.
 */
export function createKyselyExecutor(database: KyselyLike): PostgresExecutor<KyselyQueryable> {
  return {
    transaction: (work) => database.transaction().execute(work),
    // eslint-disable-next-line relaybus/max-two-params -- PostgresExecutor port signature
    async query(sql, parameters: readonly PostgresParameter[], transaction) {
      const result = await (transaction ?? database).executeQuery<PostgresRow>(
        CompiledQuery.raw(sql, [...parameters]),
      );
      return result.rows;
    },
  };
}
