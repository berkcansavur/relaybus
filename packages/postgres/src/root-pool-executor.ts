import type { PoolClient } from 'pg';
import type { PostgresExecutor } from './executor.js';

/**
 * An executor whose transactions (and the statements in them) belong to `executor`, the application's
 * library, while statements outside a transaction run on `root`, a `PgExecutor` on a `pg` pool of the
 * same database that keeps them as prepared statements.
 */
export function withRootPool<TTransaction>(
  executor: PostgresExecutor<TTransaction>,
  root: PostgresExecutor<PoolClient>,
): PostgresExecutor<TTransaction> {
  return {
    ...(executor.sessionTimeouts === undefined
      ? {}
      : { sessionTimeouts: executor.sessionTimeouts }),
    transaction: (work) => executor.transaction(work),
    ...(executor.transactionWithBudget === undefined
      ? {}
      : {
          transactionWithBudget: (work, timeoutMs) =>
            executor.transactionWithBudget!(work, timeoutMs),
        }),
    ...(executor.transactionWithIsolation === undefined
      ? {}
      : {
          transactionWithIsolation: (work, options) =>
            executor.transactionWithIsolation!(work, options),
        }),
    ...(executor.isSerializationFailure === undefined
      ? {}
      : { isSerializationFailure: (error) => executor.isSerializationFailure!(error) }),
    // eslint-disable-next-line relaybus/max-two-params -- PostgresExecutor port signature
    query: (sql, parameters, transaction, options) =>
      transaction === undefined
        ? root.query(sql, parameters, undefined, options)
        : executor.query(sql, parameters, transaction, options),
  };
}
