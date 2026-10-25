import { adapterExecutor } from '../adapter-executor.js';
import { CompiledQuery } from 'kysely';
import type { PostgresExecutor, PostgresRow } from '../executor.js';

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
  return adapterExecutor<KyselyQueryable>({
    root: database,
    transaction: (work) => database.transaction().execute(work),
    async run(queryable, { sql, parameters }) {
      const result = await queryable.executeQuery<PostgresRow>(
        CompiledQuery.raw(sql, [...parameters]),
      );
      return result.rows;
    },
  });
}
