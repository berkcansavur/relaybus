import type { TransactionIsolationLevel } from '@relaybus/core';

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
  /**
   * True when the process keeps running if the server ends the session of an open transaction
   * (an `error` listener is on the connection while the transaction runs). The store then holds
   * `bus.transaction` to its budget with `idle_in_transaction_session_timeout` and, on PostgreSQL
   * 17+, `transaction_timeout` as well as `statement_timeout`. Without it only `statement_timeout`
   * is set: a `pg` client checked out by another library without an `error` listener would crash
   * the process with an unhandled `error` event when the server ends its session. `PgExecutor`
   * sets it.
   */
  readonly sessionTimeouts?: boolean;
  transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T>;
  /**
   * Optional. Opens a transaction like {@link PostgresExecutor.transaction} that the server holds to
   * `timeoutMs` (`statement_timeout`, plus the session timeouts when {@link sessionTimeouts} is set),
   * set in the round trip of its `BEGIN`. Without it the store sets the budget with a statement of
   * its own, one round trip more per `bus.transaction`. `PgExecutor` implements it.
   */
  transactionWithBudget?<T>(
    work: (transaction: TTransaction) => Promise<T>,
    timeoutMs: number,
  ): Promise<T>;
  /**
   * Optional. Opens a transaction at `isolationLevel` that the server holds to `timeoutMs`, like
   * {@link PostgresExecutor.transactionWithBudget}, with the level set in the same round trip as its
   * `BEGIN` (`BEGIN ISOLATION LEVEL ...`). Without it the store sends `SET TRANSACTION ISOLATION
   * LEVEL ...` as the first statement of {@link PostgresExecutor.transaction}, then the budget:
   * two round trips more, on the transactions that ask for a level only. `PgExecutor` implements it.
   */
  transactionWithIsolation?<T>(
    work: (transaction: TTransaction) => Promise<T>,
    options: PostgresIsolatedTransactionOptions,
  ): Promise<T>;
  /**
   * Optional. True when `error` is a serialization failure (SQLSTATE `40001`) of this library, for
   * errors the store does not recognize itself. The store already finds `40001` in `code`, in
   * `cause` and `driverError` chains, and in Prisma's `P2034` / `meta.code`; implement it only for
   * a library that wraps the error in another shape.
   */
  isSerializationFailure?(error: unknown): boolean;
  query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: TTransaction,
    options?: PostgresQueryOptions,
  ): Promise<readonly PostgresRow[]>;
}

/** What {@link PostgresExecutor.transactionWithIsolation} opens a transaction with. */
export interface PostgresIsolatedTransactionOptions {
  readonly isolationLevel: TransactionIsolationLevel;
  /** The time budget, as for {@link PostgresExecutor.transactionWithBudget}. */
  readonly timeoutMs: number;
}
