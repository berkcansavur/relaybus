import {
  type AppendEntry,
  type AppendResult,
  type ClaimRequest,
  type ClaimedDelivery,
  type CloseOptions,
  type DeliveryQuery,
  type DeliveryRecord,
  type ErrorInfo,
  type EventStore,
  type EventStoreStatistics,
  type EventStoreTransactionOptions,
  type LeaseReference,
  type PurgeRequest,
  type PurgeResult,
  type RelayBusLogger,
} from '@relaybus/core';
import { HandedOutLedger } from '@relaybus/core/internal';
import { Client, Pool, type PoolClient } from 'pg';
import type { PostgresExecutor } from './executor.js';
import { resolvePostgresNames } from './names.js';
import { NotificationListener } from './notification-listener.js';
import { PgExecutor } from './pg-executor.js';
import { withRootPool } from './root-pool-executor.js';
import { PostgresAppend } from './postgres-append.js';
import { PostgresClaims } from './postgres-claim.js';
import { PostgresCompletion } from './postgres-completion.js';
import { OwnedConnections } from './owned-connections.js';
import { PostgresMaintenance } from './postgres-maintenance.js';
import { PostgresSetup } from './postgres-setup.js';
import type { PostgresStoreState } from './postgres-store-state.js';
import { resolveStoreSettings, type PostgresEventStoreOptions } from './postgres-store-options.js';
import { createPostgresSchemaStatements, postgresSchemaMarker } from './schema.js';
import { PostgresStatements } from './statements.js';
import { inBudgetedTransaction } from './transaction-timeouts.js';

export type {
  PostgresConnectionOptions,
  PostgresEventStoreOptions,
  PostgresEventStoreSettings,
  PostgresExecutorOptions,
  PostgresPoolOptions,
  PostgresWakeUp,
} from './postgres-store-options.js';

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
  readonly #setup: PostgresSetup<TTransaction>;
  readonly #owned = new OwnedConnections();

  constructor(options: PostgresEventStoreOptions<TTransaction>) {
    const { purgeBatchSize, wakeUp, queryOptions } = resolveStoreSettings(options);
    const names = resolvePostgresNames(options);
    const statements = new PostgresStatements(names);
    const schemaStatements = createPostgresSchemaStatements(options);
    const logger = options.logger ?? console;
    const { executor, pool, ownedPool } = connect(options, { logger, owned: this.#owned });
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
      query: ({ sql, parameters, transaction }) =>
        executor.query(sql, parameters, transaction, queryOptions),
      handedOut: new HandedOutLedger(),
      unreconciled: new Set(),
      generations: new Map(),
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
    this.#setup = new PostgresSetup(state, options);
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
    await this.#setup.run();
  }

  /**
   * Runs `work` in a transaction of the executor. With `options.timeoutMs` (from `bus.transaction`)
   * the server holds the transaction to the budget: `statement_timeout`, and for executors with
   * `sessionTimeouts` (`pg`) also `idle_in_transaction_session_timeout` and, on PostgreSQL 17+,
   * `transaction_timeout`, all scoped to the transaction. An executor with `transactionWithBudget`
   * (`PgExecutor`) sets them with `BEGIN`, so the budget costs no round trip; on a pool the store
   * created they are kept per connection and sent only when the connection does not have them yet.
   */
  async transaction<T>(
    work: (transaction: TTransaction) => Promise<T>,
    options?: EventStoreTransactionOptions,
  ): Promise<T> {
    const own = { wake: false };
    const tracked = async (transaction: TTransaction): Promise<T> => {
      this.#state.ownTransactions.set(transaction, own);
      try {
        return await work(transaction);
      } finally {
        this.#state.ownTransactions.delete(transaction);
      }
    };
    const result = await (options === undefined
      ? this.#state.executor.transaction(tracked)
      : inBudgetedTransaction(this.#state, { work: tracked, timeoutMs: options.timeoutMs }));
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
  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  append(
    transaction: TTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    return this.#appender.append(transaction, { entries, now });
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
    // A database answers with a promise; Promise.resolve returns it as it is.
    return Promise.resolve(this.#completion.extendLease({ lease, leaseExpiresAt }));
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean> {
    return this.#completion.complete(lease, { now, transaction });
  }

  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completion.completeMany(leases, now);
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return Promise.resolve(
      this.#completion.changeClaim({
        lease,
        change: { kind: 'reschedule', availableAt, error, now },
      }),
    );
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return Promise.resolve(
      this.#completion.changeClaim({ lease, change: { kind: 'bury', error, now } }),
    );
  }

  /** Returns a leased delivery to pending, due now, without counting the attempt. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return Promise.resolve(
      this.#completion.changeClaim({ lease, change: { kind: 'release', now } }),
    );
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
        this.#owned.track(client);
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

  /**
   * Ends the pool this store created from `connectionString`. Pools and executors you passed in stay
   * open. Once `options.signal` aborts (the shutdown deadline passed), the connections the store opened
   * itself are destroyed: queries still running on them fail at once instead of holding the shutdown.
   */
  async close(options: CloseOptions = {}): Promise<void> {
    this.#state.closed = true;
    this.#state.trailingNotification?.();
    this.#state.trailingNotification = undefined;
    const pool = this.#state.ownedPool;
    this.#state.ownedPool = undefined;
    const { signal } = options;
    const force = (): void => this.#owned.destroyAll();
    if (signal?.aborted === true) {
      force();
    }
    signal?.addEventListener('abort', force, { once: true });
    try {
      await pool?.end();
    } finally {
      signal?.removeEventListener('abort', force);
    }
  }
}

/**
 * The executor of the store: the one passed in, or a `pg` executor on the given pool or on a pool
 * of its own (`ownedPool`, which the store ends on close).
 */
function connect<TTransaction>(
  options: PostgresEventStoreOptions<TTransaction>,
  { logger, owned }: { readonly logger: RelayBusLogger; readonly owned: OwnedConnections },
): {
  executor: PostgresExecutor<TTransaction>;
  pool: Pool | undefined;
  ownedPool: Pool | undefined;
} {
  if (options.executor !== undefined) {
    const { executor, pool } = options;
    return {
      executor:
        pool === undefined ? executor : withRootPool(executor, new PgExecutor(pool, { logger })),
      pool,
      ownedPool: undefined,
    };
  }
  const pool =
    options.pool ??
    new Pool({
      ...options.poolConfig,
      connectionString: options.connectionString,
      Client: owned.clientClass(),
    });
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
  // A pool of its own keeps the budget per connection; a pooler is what preparedStatements: false is for.
  const executor = new PgExecutor(pool, {
    logger,
    sessionBudget: ownedPool !== undefined && options.preparedStatements !== false,
  }) as unknown as PostgresExecutor<TTransaction>;
  return { executor, pool, ownedPool };
}
