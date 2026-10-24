/** Values the store binds: always text, numbers or null (documents travel as JSON text). */
export type PostgresParameter = string | number | null;

export type PostgresRow = Readonly<Record<string, unknown>>;

/** Hints about one statement. An executor may ignore them. */
export interface PostgresQueryOptions {
  /**
   * The text never changes and the store sends it again and again (publish, claim, complete): an
   * executor that can keep it as a prepared statement per connection should, so the server parses and
   * plans it once instead of on every call. `PgExecutor` does.
   */
  readonly prepare?: boolean;
}

/**
 * The only thing {@link PostgresEventStore} needs from a database library:
 * run a parameterized statement, optionally on a transaction handle, and open
 * a transaction. Implement it to support another client; adapters for `pg`,
 * Kysely, Knex, TypeORM, Drizzle and Prisma ship with the package.
 *
 * Contract:
 * - SQL uses PostgreSQL placeholders (`$1`, `$2`, ...). Adapters translate if
 *   their library needs another syntax.
 * - Every statement the store sends returns its rows (or none), so `query`
 *   always resolves to an array of rows keyed by column name.
 * - Without a `transaction`, a statement runs on its own connection in
 *   autocommit mode, in READ COMMITTED isolation.
 * - `transaction` commits when `work` resolves and rolls back when it rejects.
 * - `options` are hints ({@link PostgresQueryOptions}); ignoring them is correct, only slower.
 */
export interface PostgresExecutor<TTransaction> {
  transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T>;
  query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: TTransaction,
    options?: PostgresQueryOptions,
  ): Promise<readonly PostgresRow[]>;
}
