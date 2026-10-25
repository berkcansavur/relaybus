import { ConfigurationError, type RelayBusLogger } from '@relaybus/core';
import { PositiveInteger } from '@relaybus/core/internal';
import type { Pool, PoolClient, PoolConfig } from 'pg';
import type { PostgresExecutor, PostgresQueryOptions } from './executor.js';
import type { PostgresSchemaOptions } from './names.js';

/** Options shared by every way of connecting. */
export interface PostgresEventStoreSettings extends PostgresSchemaOptions {
  /**
   * Maximum deliveries deleted per transaction by `purge`. Smaller batches
   * hold row locks for less time; `purge` keeps going until nothing is left.
   * Default 1000.
   */
  readonly purgeBatchSize?: number;
  /**
   * Receives errors of idle connections in a pool the store creates itself
   * (database failover, `pg_terminate_backend`, network resets). Default
   * `console`. A pool you pass in is yours: attach `pool.on('error', ...)` to
   * it, otherwise `pg` raises an unhandled `error` event and the process exits.
   */
  readonly logger?: RelayBusLogger;
  /**
   * Send the statements the store repeats (publish, claim, complete, renew) as named prepared
   * statements, so each connection parses and plans their text once instead of on every call. Default
   * `true`. Set `false` behind a pooler in transaction mode that does not keep prepared statements per
   * client (PgBouncer before 1.21, or 1.21+ with `max_prepared_statements = 0`). Left on there, the
   * first statement that lands on another server session fails once, then the store stops preparing
   * and logs a warning. Only executors that support it prepare (`PgExecutor`, the default); the
   * other adapters send every statement unprepared.
   */
  readonly preparedStatements?: boolean;
  /**
   * How dispatchers in other processes learn about new deliveries.
   * - `'notify'` (default): a publishing transaction sends `NOTIFY` (at most one per 10 ms per store,
   *   the rest merged into one trailing notification after the commit) and each dispatcher listens
   *   with `LISTEN` on a connection of its own, so a delivery is picked up within milliseconds.
   * - `'poll'`: no `NOTIFY`, no listening connection. Other processes find new deliveries when they
   *   poll (`dispatcher.pollInterval`, default 1 s); a dispatcher in the publishing process is still
   *   woken at once. Use it behind a pooler in transaction mode, where `LISTEN` does not work.
   */
  readonly wakeUp?: PostgresWakeUp;
}

/** See {@link PostgresEventStoreSettings.wakeUp}. */
export type PostgresWakeUp = 'notify' | 'poll';

const WAKE_UPS: readonly PostgresWakeUp[] = ['notify', 'poll'];

/** Use an existing `pg` pool. The store never closes it. */
export interface PostgresPoolOptions extends PostgresEventStoreSettings {
  readonly pool: Pool;
  readonly connectionString?: never;
  readonly poolConfig?: never;
  readonly executor?: never;
}

/** Let the store create (and on `close()` end) its own `pg` pool. */
export interface PostgresConnectionOptions extends PostgresEventStoreSettings {
  readonly connectionString: string;
  /** Extra `pg` pool settings, for example `max`. */
  readonly poolConfig?: Omit<PoolConfig, 'connectionString'>;
  readonly pool?: never;
  readonly executor?: never;
}

/** Use another database library through an adapter. The store never closes it. */
export interface PostgresExecutorOptions<TTransaction> extends PostgresEventStoreSettings {
  readonly executor: PostgresExecutor<TTransaction>;
  readonly pool?: never;
  readonly connectionString?: never;
  readonly poolConfig?: never;
}

export type PostgresEventStoreOptions<TTransaction = PoolClient> =
  PostgresPoolOptions | PostgresConnectionOptions | PostgresExecutorOptions<TTransaction>;

const DEFAULT_PURGE_BATCH_SIZE = 1_000;

/** Validates the options that are not names, in declaration order. */
export function resolveStoreSettings<TTransaction>(
  options: PostgresEventStoreOptions<TTransaction>,
): {
  purgeBatchSize: number;
  wakeUp: PostgresWakeUp;
  queryOptions: PostgresQueryOptions | undefined;
} {
  const sources = [options.pool, options.connectionString, options.executor].filter(
    (source) => source !== undefined,
  );
  if (sources.length !== 1) {
    throw new ConfigurationError(
      'PostgresEventStore needs exactly one of pool, connectionString or executor',
    );
  }
  const purgeBatchSize = PositiveInteger.of(
    options.purgeBatchSize ?? DEFAULT_PURGE_BATCH_SIZE,
    'purgeBatchSize',
  );
  const wakeUp = options.wakeUp ?? 'notify';
  if (!WAKE_UPS.includes(wakeUp)) {
    throw new ConfigurationError(`wakeUp must be 'notify' or 'poll', received ${String(wakeUp)}`);
  }
  const preparedStatements = options.preparedStatements ?? true;
  if (typeof preparedStatements !== 'boolean') {
    throw new ConfigurationError(
      `preparedStatements must be a boolean, received ${String(preparedStatements)}`,
    );
  }
  return {
    purgeBatchSize,
    wakeUp,
    queryOptions: preparedStatements ? { prepare: true } : undefined,
  };
}
