import type { TransactionIsolationLevel } from '@relaybus/core';
import type { PostgresExecutor, PostgresParameter, PostgresRow } from './executor.js';
import { budgetSettingsSql, budgetValue } from './transaction-timeouts.js';

/** One statement the store sends, with its positional parameters. */
export type AdapterStatement = {
  readonly sql: string;
  readonly parameters: readonly PostgresParameter[];
};

/** What an adapter supplies: where statements run outside a transaction, and how it runs one. */
export type AdapterClient<TQueryable> = {
  /** The instance itself: statements outside a transaction run on it. */
  readonly root: TQueryable;
  readonly transaction: PostgresExecutor<TQueryable>['transaction'];
  /**
   * Optional. Opens a transaction at an isolation level through the library's own API, for a library
   * that runs code of its own (hooks, subscribers) right after it begins a transaction: that code
   * would otherwise run a query before the store's `SET TRANSACTION ISOLATION LEVEL`.
   */
  readonly transactionAt?: <T>(
    isolationLevel: TransactionIsolationLevel,
    work: (queryable: TQueryable) => Promise<T>,
  ) => Promise<T>;
  /** Runs one statement on the caller's transaction, else on the root. */
  readonly run: (queryable: TQueryable, statement: AdapterStatement) => Promise<PostgresRow[]>;
};

/**
 * The executor every library adapter shares: a statement runs on the transaction it is given,
 * else on the root instance. An adapter supplies only how its library runs one statement.
 */
export function adapterExecutor<TQueryable>(
  client: AdapterClient<TQueryable>,
): PostgresExecutor<TQueryable> {
  return {
    transaction: client.transaction,
    ...(client.transactionAt === undefined
      ? {}
      : {
          // The level is set by the library at BEGIN, so the budget statement can follow as the first query.
          transactionWithIsolation: (work, { isolationLevel, timeoutMs }) =>
            client.transactionAt!(isolationLevel, async (queryable) => {
              const sql = budgetSettingsSql({
                value: '$1',
                sessionTimeouts: false,
                scope: 'transaction',
              });
              await client.run(queryable, { sql, parameters: [budgetValue(timeoutMs)] });
              return work(queryable);
            }),
        }),
    // eslint-disable-next-line relaybus/max-two-params -- PostgresExecutor port signature
    query: (sql, parameters, transaction) =>
      client.run(transaction ?? client.root, { sql, parameters }),
  };
}
