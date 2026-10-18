import {
  ConfigurationError,
  type AppendEntry,
  type AppendResult,
  type ClaimRequest,
  type ClaimedDelivery,
  type DeliveryQuery,
  type DeliveryRecord,
  type DeliveryStatus,
  type ErrorInfo,
  type EventEnvelope,
  type EventStore,
  type EventStoreStatistics,
  type LeaseReference,
  type PurgeRequest,
  type PurgeResult,
  type RelayBusLogger,
} from '@relaybus/core';
import { Pool, type PoolClient, type PoolConfig } from 'pg';
import type { PostgresExecutor, PostgresParameter, PostgresRow } from './executor.js';
import { resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { PgExecutor } from './pg-executor.js';
import { mapClaimedDelivery, mapDeliveryRecord, mapOldestDueAt, readCount } from './row-mapping.js';
import { createPostgresSchemaStatements } from './schema.js';
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
}

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

interface EventRow {
  readonly id: string;
  readonly type: string;
  readonly source: string;
  readonly key: string | null;
  readonly sequence: string | null;
  readonly payload: unknown;
  readonly headers: EventEnvelope['headers'];
  readonly correlation_id: string | null;
  readonly causation_id: string | null;
  readonly occurred_at: string;
}

interface DeliveryRow {
  readonly id: string;
  readonly event_id: string;
  readonly handler: string;
  readonly available_at: string;
}

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
 *   sequence of a (handler, key).
 * - **Safe parallel claiming.** One `FOR UPDATE SKIP LOCKED` statement leases
 *   deliveries; any number of processes can claim at once.
 * - **Fenced state changes.** Complete, reschedule, bury and extend only apply
 *   to the lease token that claimed the delivery.
 * - **App clock.** Every timestamp comes from the `now` the caller passes in,
 *   never from the database clock.
 */
export class PostgresEventStore<TTransaction = PoolClient> implements EventStore<TTransaction> {
  readonly #executor: PostgresExecutor<TTransaction>;
  readonly #statements: PostgresStatements;
  readonly #schemaStatements: readonly string[];
  readonly #setupLockKey: string;
  readonly #purgeBatchSize: number;
  #ownedPool: Pool | undefined;

  constructor(options: PostgresEventStoreOptions<TTransaction>) {
    const sources = [options.pool, options.connectionString, options.executor].filter(
      (source) => source !== undefined,
    );
    if (sources.length !== 1) {
      throw new ConfigurationError(
        'PostgresEventStore needs exactly one of pool, connectionString or executor',
      );
    }
    const purgeBatchSize = options.purgeBatchSize ?? DEFAULT_PURGE_BATCH_SIZE;
    if (!Number.isInteger(purgeBatchSize) || purgeBatchSize < 1) {
      throw new ConfigurationError(
        `purgeBatchSize must be a positive integer, received ${String(purgeBatchSize)}`,
      );
    }
    const names = resolvePostgresNames(options);
    this.#statements = new PostgresStatements(names);
    this.#schemaStatements = createPostgresSchemaStatements(options);
    this.#setupLockKey = this.#statements.setupLockKey;
    this.#purgeBatchSize = purgeBatchSize;
    if (options.executor !== undefined) {
      this.#executor = options.executor;
    } else {
      const pool =
        options.pool ??
        new Pool({ ...options.poolConfig, connectionString: options.connectionString });
      if (options.pool === undefined) {
        const logger = options.logger ?? console;
        pool.on('error', (error) => {
          logger.error(
            `[PostgresEventStore](constructor) Idle connection failed, the pool replaces it | error=${error.message}`,
          );
        });
        this.#ownedPool = pool;
      }
      // Without an executor the transaction type is PoolClient, as the default type argument states.
      this.#executor = new PgExecutor(pool) as unknown as PostgresExecutor<TTransaction>;
    }
  }

  /**
   * Creates the schema, tables and indexes if they do not exist. Safe to call
   * from many processes at once: a transaction-scoped advisory lock serializes
   * the DDL, so racing `CREATE TABLE IF NOT EXISTS` statements cannot collide.
   * Skip it (`setupStore: false`) when a migration tool applies
   * `createPostgresSchemaSql()` instead.
   */
  async setup(): Promise<void> {
    await this.#executor.transaction(async (transaction) => {
      await this.#executor.query(this.#statements.setupLock, [this.#setupLockKey], transaction);
      for (const statement of this.#schemaStatements) {
        await this.#executor.query(statement, [], transaction);
      }
    });
  }

  transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T> {
    return this.#executor.transaction(work);
  }

  /**
   * Writes the entries in `transaction`. Keyed events reserve their sequence
   * numbers first (locking the key rows in sorted order, so publishers cannot
   * deadlock inside one call), then events, then deliveries, each in a single
   * multi-row statement ordered by primary key. Two transactions that append
   * keys in opposite order across several calls can still deadlock (40P01);
   * the database rolls one back and the caller retries.
   * A duplicate event id still consumes a sequence number; gaps are harmless
   * because only the order of sequences matters.
   */
  async append(
    transaction: TTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    const plan = planAppend(entries);
    const createdAt = now.toISOString();
    const sequences = await this.#reserveSequences(plan.events, transaction);
    const insertedEventIds = new Set(
      plan.events.length === 0
        ? []
        : (
            await this.#executor.query(
              this.#statements.insertEvents,
              [JSON.stringify(toEventRows(plan.events, sequences)), createdAt],
              transaction,
            )
          ).map((row) => row['id'] as string),
    );
    const insertedDeliveries =
      plan.deliveries.length === 0
        ? []
        : await this.#executor.query(
            this.#statements.insertDeliveries,
            [JSON.stringify(plan.deliveries), createdAt],
            transaction,
          );
    const appendedEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    for (const { eventId, repeated } of plan.outcomes) {
      (!repeated && insertedEventIds.has(eventId) ? appendedEventIds : duplicateEventIds).push(
        eventId,
      );
    }
    return {
      appendedEventIds,
      duplicateEventIds,
      appendedDeliveryCount: insertedDeliveries.length,
    };
  }

  /**
   * Leases up to `limit` due deliveries with `FOR UPDATE SKIP LOCKED`. The fast
   * statement examines the first due deliveries; only when they are mostly
   * blocked followers of busy keys does a second statement look at the heads of
   * all keys. See {@link PostgresStatements.claim} for why that keeps a
   * (handler, key) strictly one-at-a-time and in order.
   */
  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || !(request.limit > 0)) {
      return [];
    }
    const parameters = (limit: number): PostgresParameter[] => [
      request.now.toISOString(),
      JSON.stringify(request.handlers),
      limit,
      request.leaseOwner,
      request.leaseExpiresAt.toISOString(),
    ];
    const fastRows = await this.#executor.query(this.#statements.claim, parameters(request.limit));
    const leased = fastRows.filter((row) => row['delivery_id'] !== null);
    if (leased.length < request.limit && fastRows[0]!['window_saturated'] === true) {
      leased.push(
        ...(
          await this.#executor.query(
            this.#statements.claimBehindBlockedKeys,
            parameters(request.limit - leased.length),
          )
        ).filter((row) => row['delivery_id'] !== null),
      );
    }
    return leased.map((row) => mapClaimedDelivery(row, request.leaseExpiresAt));
  }

  extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean> {
    return this.#changed(this.#statements.extendLease, [
      lease.deliveryId,
      lease.leaseToken,
      leaseExpiresAt.toISOString(),
      now.toISOString(),
    ]);
  }

  complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean> {
    return this.#changed(
      this.#statements.complete,
      [lease.deliveryId, lease.leaseToken, now.toISOString()],
      transaction,
    );
  }

  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#changed(this.#statements.reschedule, [
      lease.deliveryId,
      lease.leaseToken,
      availableAt.toISOString(),
      JSON.stringify(error),
      now.toISOString(),
    ]);
  }

  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#changed(this.#statements.bury, [
      lease.deliveryId,
      lease.leaseToken,
      JSON.stringify(error),
      now.toISOString(),
    ]);
  }

  /** Returns a leased delivery to pending, due now, without counting the attempt. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#changed(this.#statements.release, [
      lease.deliveryId,
      lease.leaseToken,
      now.toISOString(),
    ]);
  }

  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    const statement = this.#statements.listDeliveries(query);
    const rows = await this.#executor.query(statement.sql, statement.parameters);
    return rows.map(mapDeliveryRecord);
  }

  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    const rows = await this.#executor.query(this.#statements.requeue, [
      JSON.stringify(deliveryIds),
      now.toISOString(),
    ]);
    return readCount(rows[0]!, 'count');
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
  async purge(request: PurgeRequest): Promise<PurgeResult> {
    const total = { deletedDeliveries: 0, deletedEvents: 0 };
    const batches: Array<[string, Date | undefined]> = [
      [this.#statements.purgeCompletedBatch(), request.completedBefore],
      [this.#statements.purgeDeadBatch(), request.deadBefore],
    ];
    for (const [sql, cutoff] of batches) {
      if (cutoff === undefined) {
        continue;
      }
      let deletedInBatch = this.#purgeBatchSize;
      while (deletedInBatch === this.#purgeBatchSize) {
        const batch = await this.#purgeBatch(sql, cutoff);
        deletedInBatch = batch.deletedDeliveries;
        total.deletedDeliveries += batch.deletedDeliveries;
        total.deletedEvents += batch.deletedEvents;
      }
    }
    const orphanCutoff = [request.completedBefore, request.deadBefore]
      .filter((cutoff) => cutoff !== undefined)
      .sort((first, second) => first.getTime() - second.getTime())[0];
    if (orphanCutoff !== undefined) {
      total.deletedEvents += await this.#purgeOrphanEvents(orphanCutoff);
    }
    return total;
  }

  async statistics(now: Date): Promise<EventStoreStatistics> {
    const counts: Record<DeliveryStatus, number> = {
      pending: 0,
      processing: 0,
      completed: 0,
      dead: 0,
    };
    for (const row of await this.#executor.query(this.#statements.countByStatus, [])) {
      // A CHECK constraint restricts the column to the four statuses.
      counts[row['status'] as DeliveryStatus] = readCount(row, 'count');
    }
    const oldestDue = await this.#executor.query(this.#statements.oldestDue, [now.toISOString()]);
    return { ...counts, oldestDueAt: mapOldestDueAt(oldestDue) };
  }

  /** Ends the pool this store created from `connectionString`. Pools and executors you passed in stay open. */
  async close(): Promise<void> {
    const pool = this.#ownedPool;
    this.#ownedPool = undefined;
    await pool?.end();
  }

  async #changed(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: TTransaction,
  ): Promise<boolean> {
    const rows = await this.#executor.query(sql, parameters, transaction);
    return readCount(rows[0]!, 'count') > 0;
  }

  async #reserveSequences(
    events: readonly EventEnvelope[],
    transaction: TTransaction,
  ): Promise<Map<string, bigint>> {
    const counts = new Map<string, number>();
    for (const event of events) {
      if (event.key !== null) {
        counts.set(event.key, (counts.get(event.key) ?? 0) + 1);
      }
    }
    const firstSequences = new Map<string, bigint>();
    if (counts.size === 0) {
      return firstSequences;
    }
    const requested = [...counts].map(([key, count]) => ({ key, count }));
    const rows = await this.#executor.query(
      this.#statements.reserveSequences,
      [JSON.stringify(requested)],
      transaction,
    );
    for (const row of rows) {
      const key = row['key'] as string;
      firstSequences.set(key, BigInt(row['sequence'] as string) - BigInt(counts.get(key)!) + 1n);
    }
    return firstSequences;
  }

  async #purgeOrphanEvents(cutoff: Date): Promise<number> {
    let deletedEvents = 0;
    let removedInBatch = this.#purgeBatchSize;
    while (removedInBatch === this.#purgeBatchSize) {
      removedInBatch = await this.#executor.transaction(async (transaction) => {
        const locked = await this.#executor.query(
          this.#statements.lockOrphanEventsBatch,
          [cutoff.toISOString(), this.#purgeBatchSize],
          transaction,
        );
        const removed = await this.#executor.query(
          this.#statements.deleteOrphanEvents,
          [JSON.stringify(locked.map((row: PostgresRow) => row['id'] as string))],
          transaction,
        );
        return readCount(removed[0]!, 'count');
      });
      deletedEvents += removedInBatch;
    }
    return deletedEvents;
  }

  async #purgeBatch(
    sql: string,
    cutoff: Date,
  ): Promise<{ deletedDeliveries: number; deletedEvents: number }> {
    return this.#executor.transaction(async (transaction) => {
      const deleted = await this.#executor.query(
        sql,
        [cutoff.toISOString(), this.#purgeBatchSize],
        transaction,
      );
      if (deleted.length === 0) {
        return { deletedDeliveries: 0, deletedEvents: 0 };
      }
      const eventIds = JSON.stringify([
        ...new Set(deleted.map((row) => row['event_id'] as string)),
      ]);
      const locked = await this.#executor.query(
        this.#statements.lockEvents,
        [eventIds],
        transaction,
      );
      const removed = await this.#executor.query(
        this.#statements.deleteOrphanEvents,
        [JSON.stringify(locked.map((row: PostgresRow) => row['id'] as string))],
        transaction,
      );
      return { deletedDeliveries: deleted.length, deletedEvents: readCount(removed[0]!, 'count') };
    });
  }
}

interface AppendPlan {
  /** First occurrence of every event id in the call, in entry order. */
  readonly events: readonly EventEnvelope[];
  readonly deliveries: readonly DeliveryRow[];
  /** One per entry: its event id, and whether an earlier entry of the call carried the same id. */
  readonly outcomes: ReadonlyArray<{ readonly eventId: string; readonly repeated: boolean }>;
}

function planAppend(entries: readonly AppendEntry[]): AppendPlan {
  const events: EventEnvelope[] = [];
  const deliveries: DeliveryRow[] = [];
  const outcomes: Array<{ eventId: string; repeated: boolean }> = [];
  const seenEventIds = new Set<string>();
  const seenDeliveries = new Set<string>();
  for (const { event, deliveries: newDeliveries } of entries) {
    const repeated = seenEventIds.has(event.id);
    seenEventIds.add(event.id);
    outcomes.push({ eventId: event.id, repeated });
    if (!repeated) {
      events.push(event);
    }
    for (const delivery of newDeliveries) {
      const identity = `${event.id}\u0000${delivery.handler}`;
      if (!seenDeliveries.has(identity)) {
        seenDeliveries.add(identity);
        deliveries.push({
          id: delivery.id,
          event_id: event.id,
          handler: delivery.handler,
          available_at: delivery.availableAt.toISOString(),
        });
      }
    }
  }
  return { events, deliveries, outcomes };
}

function toEventRows(
  events: readonly EventEnvelope[],
  firstSequences: ReadonlyMap<string, bigint>,
): EventRow[] {
  const nextSequences = new Map(firstSequences);
  return events.map((event) => {
    let sequence: string | null = null;
    if (event.key !== null) {
      const next = nextSequences.get(event.key)!;
      nextSequences.set(event.key, next + 1n);
      sequence = next.toString();
    }
    return {
      id: event.id,
      type: event.type,
      source: event.source,
      key: event.key,
      sequence,
      payload: event.payload,
      headers: event.headers,
      correlation_id: event.correlationId,
      causation_id: event.causationId,
      occurred_at: event.occurredAt.toISOString(),
    };
  });
}
