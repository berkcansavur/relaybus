import { adapterExecutor } from '../adapter-executor.js';
import type { PostgresExecutor, PostgresRow } from '../executor.js';

/** What a TypeORM `DataSource` and an `EntityManager` have in common. */
export interface TypeOrmQueryable {
  query(query: string, parameters?: unknown[]): Promise<unknown>;
}

/** The part of a `DataSource` the executor uses. */
export interface TypeOrmDataSourceLike extends TypeOrmQueryable {
  transaction<T>(work: (manager: TypeOrmQueryable) => Promise<T>): Promise<T>;
}

/**
 * Executor for TypeORM. The transaction handle is the `EntityManager` of a
 * transaction: pass `manager` from `dataSource.transaction(async (manager) => ...)`
 * (or the manager your transaction decorator provides). Every statement the
 * store sends is a plain SELECT or INSERT, for which `query` returns the rows.
 */
export function createTypeOrmExecutor(
  dataSource: TypeOrmDataSourceLike,
): PostgresExecutor<TypeOrmQueryable> {
  return adapterExecutor<TypeOrmQueryable>({
    root: dataSource,
    transaction: (work) => dataSource.transaction(work),
    async run(queryable, { sql, parameters }) {
      return (await queryable.query(sql, [...parameters])) as PostgresRow[];
    },
  });
}
