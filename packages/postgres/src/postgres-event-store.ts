import {
  ConfigurationError,
  type AppendEntry,
  type AppendResult,
  type ClaimRequest,
  type ClaimedDelivery,
  type DeliveryQuery,
  type DeliveryRecord,
  type ErrorInfo,
  type EventStore,
  type EventStoreStatistics,
  type LeaseReference,
  type PurgeRequest,
  type PurgeResult,
  type RelayBusLogger,
} from '@relaybus/core';
import { assertPositiveInteger } from '@relaybus/core/internal';
import { Client, Pool, type PoolClient, type PoolConfig } from 'pg';
import type { PostgresExecutor, PostgresQueryOptions } from './executor.js';
import { resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { NotificationListener } from './notification-listener.js';
import { PgExecutor } from './pg-executor.js';
import { PostgresAppend } from './postgres-append.js';
import { PostgresClaims } from './postgres-claim.js';
import { PostgresCompletion } from './postgres-completion.js';
import { PostgresMaintenance } from './postgres-maintenance.js';
import type { PostgresStoreState } from './postgres-store-state.js';
import { createPostgresSchemaStatements, postgresSchemaMarker } from './schema.js';
import { PostgresStatements } from './statements.js';

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

/**
 * PostgreSQL {@link EventStore} (PostgreSQL 13 or newer).
 *
 * The transaction handle is whatever the chosen driver uses: a `pg`
 * `PoolClient` by default, or a Kysely / Knex / TypeORM / Drizzle / Prisma
 * transaction through the adapters in `@relaybus/postgres/<library>`. Events
 * published with that handle commit and roll back together with the caller's
 * own writes.
 *
 * Guarantees:
 * - **Atomic publish.** Events and deliveries are written in the caller's transaction.
 * - **Per-key order.** Each key has a counter row in the key-sequence table.
 *   Publishing locks that row and holds the lock until commit, so two
 *   transactions publishing the same key are serialized and sequence order
 *   equals commit order. Claiming then hands out only the lowest unfinished
 *   sequence of a (handler, key), and all deliveries of a key live in one lane,
 *   so one worker at a time sees them.
 * - **Lane leases.** Workers lease lanes (256 hash buckets of the ordering key
 *   or event id), not single deliveries. A claim writes one lane row for a
 *   whole batch and never writes a delivery row; a completion deletes the
 *   delivery rows. Delivery rows are therefore inserted once and deleted once,
 *   which leaves no dead index entries from updates on the normal path.
 * - **Fenced state changes.** Complete, reschedule, bury and extend only apply
 *   with the epoch of the lane lease that claimed the delivery. A lane taken
 *   over by another worker gets a new epoch, which fences everything the
 *   previous holder still runs. Completion takes a key-share lock on the lane
 *   row, so it cannot interleave with a takeover.
 * - **Attempts survive crashes.** A claim records which deliveries it put in
 *   flight in the lane row. If the holder dies, the next holder charges one
 *   attempt to each of them, so a poison message still reaches the retry limit.
 * - **App clock.** Every timestamp comes from the `now` the caller passes in,
 *   never from the database clock.
 *
 * Completed deliveries are deleted. Pass `completionHistory: true` to keep an
 * append-only record for `listDeliveries({ status: 'completed' })`.
 */
export class PostgresEventStore<TTransaction = PoolClient> implements EventStore<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;
  readonly #appender: PostgresAppend<TTransaction>;
  readonly #claims: PostgresClaims<TTransaction>;
  readonly #completion: PostgresCompletion<TTransaction>;
  readonly #maintenance: PostgresMaintenance<TTransaction>;

  constructor(options: PostgresEventStoreOptions<TTransaction>) {
    const { purgeBatchSize, wakeUp, queryOptions } = resolveStoreSettings(options);
    const names = resolvePostgresNames(options);
    const statements = new PostgresStatements(names);
    const schemaStatements = createPostgresSchemaStatements(options);
    const logger = options.logger ?? console;
    const { executor, pool, ownedPool } = connect(options, logger);
    const state: PostgresStoreState<TTransaction> = {
      executor,
      statements,
      schemaStatements,
      setupLockKey: statements.setup.setupLockKey,
      schemaMarker: postgresSchemaMarker(schemaStatements.slice(0, -1)),
      purgeBatchSize,
      completionHistory: names.completionHistory,
      logger,
      queryOptions,
      notifies: wakeUp === 'notify',
      query: (sql, parameters, transaction) =>
        executor.query(sql, parameters, transaction, queryOptions),
      handedOut: new Map(),
      unreconciled: new Set(),
      generations: new Map(),
      ownerQueues: new Map(),
      drainingOwners: new Set(),
      ownedPool,
      pool,
      notificationChannel: names.notificationChannel,
      lastNotificationAt: Number.NEGATIVE_INFINITY,
      trailingNotification: undefined,
      closed: false,
      ownTransactions: new Map(),
    };
    this.#state = state;
    this.#appender = new PostgresAppend(state);
    this.#claims = new PostgresClaims(state);
    this.#completion = new PostgresCompletion(state);
    this.#maintenance = new PostgresMaintenance(state);
  }

  /**
   * Creates the schema, tables and indexes if they do not exist. Safe to call
   * from many processes at once: a transaction-scoped advisory lock serializes
   * the DDL, so racing `CREATE TABLE IF NOT EXISTS` statements cannot collide.
   * When the lane table already carries the marker of the current statements
   * ({@link postgresSchemaMarker}) nothing runs and no table is locked, so instances
   * that start next to running dispatchers cannot deadlock with their claims.
   * Skip it (`setupStore: false`) when a migration tool applies
   * `createPostgresSchemaSql()` instead.
   */
  async setup(): Promise<void> {
    // DDL on a current schema would still lock the tables, and those locks deadlock with running claims.
    if (await this.#schemaIsCurrent()) {
      return;
    }
    await this.#state.executor.transaction(async (transaction) => {
      await this.#state.executor.query(
        this.#state.statements.setup.setupLock,
        [this.#state.setupLockKey],
        transaction,
      );
      if (await this.#schemaIsCurrent(transaction)) {
        return;
      }
      for (const statement of this.#state.schemaStatements) {
        await this.#state.executor.query(statement, [], transaction);
      }
    });
  }

  async #schemaIsCurrent(transaction?: TTransaction): Promise<boolean> {
    const rows = await this.#state.executor.query(
      this.#state.statements.setup.schemaCurrent,
      [this.#state.schemaMarker],
      transaction,
    );
    return rows[0]!['current'] === true;
  }

  async transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T> {
    const own = { wake: false };
    const result = await this.#state.executor.transaction(async (transaction) => {
      this.#state.ownTransactions.set(transaction, own);
      try {
        return await work(transaction);
      } finally {
        this.#state.ownTransactions.delete(transaction);
      }
    });
    if (own.wake) {
      this.#appender.wakeAfterCommit();
    }
    return result;
  }

  /**
   * Writes the entries in `transaction`, normally with one statement
   * (`AppendStatements.insertFreshEvents`): keyed events reserve their sequence numbers (locking
   * the key rows in sorted order, so publishers cannot deadlock inside one call), then the new events and
   * their deliveries are inserted, ordered by primary key, and one notification wakes the dispatchers
   * when a delivery is due now. Events that already existed get their missing handlers' deliveries in
   * two more statements. Two transactions that append keys in opposite order across several calls can
   * still deadlock (40P01); the database rolls one back and the caller retries.
   * A duplicate event id still consumes a sequence number; gaps are harmless
   * because only the order of sequences matters.
   */
  append(
    transaction: TTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    return this.#appender.append(transaction, entries, now);
  }

  /**
   * Claims up to `limit` due deliveries, normally with one statement
   * (`LaneStatements.claimLanes`): from the lanes this owner already holds, then from free lanes
   * it takes in the same statement, recording the batch in the lane rows. A claim that would have to take
   * over a lane whose previous holder still runs deliveries under an expired lease continues in a
   * transaction that charges their lost attempts first, taking free lanes a few at a time (4, 8, 16, ...);
   * so does a claim whose statement kept seeing a lane it holds change under it, and one that came up
   * short with more free lanes left than it looked at. The step that fails after
   * the first one has recorded deliveries returns those instead of failing: a recorded delivery nobody
   * received would be held for a whole lease. See `ClaimStatements.claim` for the rules a delivery
   * must satisfy.
   */
  claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    return this.#claims.claim(request);
  }

  extendLease(lease: LeaseReference, leaseExpiresAt: Date): Promise<boolean> {
    return this.#completion.extendLease(lease, leaseExpiresAt);
  }

  complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean> {
    return this.#completion.complete(lease, now, transaction);
  }

  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completion.completeMany(leases, now);
  }

  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#completion.reschedule(lease, availableAt, error, now);
  }

  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#completion.bury(lease, error, now);
  }

  /** Returns a leased delivery to pending, due now, without counting the attempt. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#completion.release(lease, now);
  }

  /**
   * Calls `listener` soon after a transaction that published deliveries due at once commits, in any
   * process: `append` sends one `pg_notify` per call (merged per transaction, none for delayed
   * deliveries), and this listens with `LISTEN` on a connection of its own, opened with the settings of
   * the store's pool (one connection more than the pool's `max`). A lost connection is replaced after a
   * second. Resolves with `undefined` for a store built on an `executor`, which has no connection of its
   * own to listen on, and with `wakeUp: 'poll'`: polling then finds new deliveries (and the bus still
   * wakes its own dispatcher).
   */
  async watchNewDeliveries(listener: () => void): Promise<(() => Promise<void>) | undefined> {
    if (this.#state.pool === undefined || !this.#state.notifies) {
      return undefined;
    }
    const options = this.#state.pool.options;
    const watcher = new NotificationListener(
      async () => {
        // A client of its own, with the pool's connection settings: the pool keeps all its connections.
        const client = new Client(options);
        await client.connect();
        return client;
      },
      this.#state.notificationChannel,
      listener,
      this.#state.logger,
    );
    await watcher.start();
    return () => watcher.close();
  }

  /** Frees the lanes `leaseOwner` holds that run no delivery, so other workers need not wait for the lease to expire. */
  relinquish(leaseOwner: string): Promise<void> {
    return this.#maintenance.relinquish(leaseOwner);
  }

  listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    return this.#maintenance.listDeliveries(query);
  }

  requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    return this.#maintenance.requeue(deliveryIds, now);
  }

  /**
   * Deletes purgeable deliveries in batches of `purgeBatchSize`, one short
   * transaction per batch, then the events those batches left without any
   * delivery. Finally, events created before the earliest given cutoff that
   * have no delivery at all are removed: they are what a publish leaves behind
   * when its last delivery was purged in the same instant, or when no handler
   * was subscribed. Events that a concurrent publish is attaching a delivery
   * to are skipped and kept.
   */
  purge(request: PurgeRequest): Promise<PurgeResult> {
    return this.#maintenance.purge(request);
  }

  statistics(now: Date): Promise<EventStoreStatistics> {
    return this.#maintenance.statistics(now);
  }

  /** Ends the pool this store created from `connectionString`. Pools and executors you passed in stay open. */
  async close(): Promise<void> {
    this.#state.closed = true;
    this.#state.trailingNotification?.();
    this.#state.trailingNotification = undefined;
    const pool = this.#state.ownedPool;
    this.#state.ownedPool = undefined;
    await pool?.end();
  }
}

/** Validates the options that are not names, in declaration order. */
function resolveStoreSettings<TTransaction>(options: PostgresEventStoreOptions<TTransaction>): {
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
  const purgeBatchSize = assertPositiveInteger(
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

/**
 * The executor of the store: the one passed in, or a `pg` executor on the given pool or on a pool
 * of its own (`ownedPool`, which the store ends on close).
 */
function connect<TTransaction>(
  options: PostgresEventStoreOptions<TTransaction>,
  logger: RelayBusLogger,
): {
  executor: PostgresExecutor<TTransaction>;
  pool: Pool | undefined;
  ownedPool: Pool | undefined;
} {
  if (options.executor !== undefined) {
    return { executor: options.executor, pool: undefined, ownedPool: undefined };
  }
  const pool =
    options.pool ?? new Pool({ ...options.poolConfig, connectionString: options.connectionString });
  let ownedPool: Pool | undefined;
  if (options.pool === undefined) {
    const poolLogger = options.logger ?? console;
    pool.on('error', (error) => {
      poolLogger.error(
        `[PostgresEventStore](connect) Idle connection failed, the pool replaces it | error=${error.message}`,
      );
    });
    ownedPool = pool;
  }
  // Without an executor the transaction type is PoolClient, as the default type argument states.
  const executor = new PgExecutor(pool, {
    logger,
  }) as unknown as PostgresExecutor<TTransaction>;
  return { executor, pool, ownedPool };
}
