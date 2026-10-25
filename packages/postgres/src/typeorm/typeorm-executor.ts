import type { PostgresExecutor, PostgresParameter, PostgresRow } from '../executor.js';

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
  return {
    transaction: (work) => dataSource.transaction(work),
    // eslint-disable-next-line relaybus/max-two-params -- PostgresExecutor port signature
    async query(sql, parameters: readonly PostgresParameter[], transaction) {
      return (await (transaction ?? dataSource).query(sql, [...parameters])) as PostgresRow[];
    },
  };
}
