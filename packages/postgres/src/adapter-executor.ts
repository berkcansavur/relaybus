import type { PostgresExecutor, PostgresParameter, PostgresRow } from './executor.js';

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
    // eslint-disable-next-line relaybus/max-two-params -- PostgresExecutor port signature
    query: (sql, parameters, transaction) =>
      client.run(transaction ?? client.root, { sql, parameters }),
  };
}
