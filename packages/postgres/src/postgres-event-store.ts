import {
  ConfigurationError,
  type AppendEntry,
  type AppendResult,
  type ClaimRequest,
  type ClaimedDelivery,
  type DeliveryQuery,
  type DeliveryRecord,
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
import {
  mapClaimedDelivery,
  mapDeliveryRecord,
  mapOldestDueAt,
  parseLeaseToken,
  readCount,
  type LaneLease,
} from './row-mapping.js';
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
/** Lanes a claim takes in its first round; every further round takes twice as many. */
const INITIAL_LANES_PER_ROUND = 4;

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
  readonly handlers: readonly string[];
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
  readonly #executor: PostgresExecutor<TTransaction>;
  readonly #statements: PostgresStatements;
  readonly #schemaStatements: readonly string[];
  readonly #setupLockKey: string;
  readonly #purgeBatchSize: number;
  readonly #completionHistory: boolean;
  readonly #logger: RelayBusLogger;
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
    this.#completionHistory = names.completionHistory;
    this.#logger = options.logger ?? console;
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
              [JSON.stringify(toEventRows(plan, sequences)), createdAt],
              transaction,
            )
          ).map((row) => row['id'] as string),
    );
    const insertedDeliveries =
      plan.deliveries.length === 0
        ? []
        : await this.#executor.query(
            this.#statements.insertDeliveries,
            [JSON.stringify(plan.deliveries), createdAt, JSON.stringify([...insertedEventIds])],
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
   * Claims up to `limit` due deliveries in two steps. The first is one
   * statement on the lanes this owner already holds ({@link PostgresStatements.claimHeld}):
   * a backlog that fills the batch costs one round trip. When it does not, a
   * transaction continues: the slow claim on held lanes whose scan window was
   * full of blocked followers, then free lanes with due work, taken a few at
   * a time (4, 8, 16, ...) so a worker holds only as many lanes as it needs,
   * and the batch is recorded in the lane rows. Lanes taken for nothing
   * (everything in them is blocked or not yet due) are handed back. The step
   * that fails after the first one has recorded deliveries returns those
   * instead of failing: a recorded delivery nobody received would be held
   * for a whole lease. See {@link PostgresStatements.claim} for the rules a
   * delivery must satisfy.
   */
  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || !(request.limit > 0)) {
      return [];
    }
    const heldRows = await this.#executor.query(this.#statements.claimHeld, [
      request.now.toISOString(),
      JSON.stringify(request.handlers),
      request.limit,
      request.leaseOwner,
      request.leaseExpiresAt.toISOString(),
    ]);
    const claimed = heldRows.filter((row) => row['delivery_id'] !== null);
    if (claimed.length < request.limit) {
      const blockedLanes =
        heldRows[0]!['window_saturated'] === true
          ? (JSON.parse(heldRows[0]!['held_lanes'] as string) as number[])
          : [];
      try {
        claimed.push(...(await this.#claimMore(request, claimed.length, blockedLanes)));
      } catch (error) {
        if (claimed.length === 0) {
          throw error;
        }
        this.#logger.error(
          `[PostgresEventStore](claim) Taking more lanes failed, returning the deliveries already claimed | claimed=${claimed.length} | error=${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
    return claimed.map((row) => mapClaimedDelivery(row, request.leaseExpiresAt));
  }

  /**
   * Second step of {@link PostgresEventStore.claim}, in one transaction: the slow
   * claim on the held lanes in `blockedLanes`, then free lanes. `alreadyClaimed`
   * deliveries of the first step are in flight, so they are not picked again.
   */
  async #claimMore(
    request: ClaimRequest,
    alreadyClaimed: number,
    blockedLanes: readonly number[],
  ): Promise<PostgresRow[]> {
    const now = request.now.toISOString();
    const leaseExpiresAt = request.leaseExpiresAt.toISOString();
    const handlers = JSON.stringify(request.handlers);
    return this.#executor.transaction(async (transaction) => {
      const claimed: PostgresRow[] = [];
      const remaining = (): number => request.limit - alreadyClaimed - claimed.length;
      const takenLanes = new Set<number>();
      if (blockedLanes.length > 0) {
        const locked = await this.#executor.query(
          this.#statements.lockHeldLanes,
          [request.leaseOwner, now],
          transaction,
        );
        const slowRows = await this.#executor.query(
          this.#statements.claimBehindBlockedKeys,
          [now, handlers, remaining(), JSON.stringify(locked.map((row) => readCount(row, 'lane')))],
          transaction,
        );
        claimed.push(...slowRows.filter((row) => row['delivery_id'] !== null));
      }
      for (let laneCount = INITIAL_LANES_PER_ROUND; remaining() > 0;) {
        const taken = await this.#executor.query(
          this.#statements.takeFreeLanes,
          [now, handlers, laneCount, request.leaseOwner, leaseExpiresAt],
          transaction,
        );
        if (taken.length === 0) {
          break;
        }
        const lanes = taken.map((row) => readCount(row, 'lane'));
        for (const lane of lanes) {
          takenLanes.add(lane);
        }
        claimed.push(
          ...(await this.#pickDeliveries(
            [now, handlers, remaining(), JSON.stringify(lanes)],
            transaction,
          )),
        );
        laneCount *= 2;
      }
      await this.#recordClaim(claimed, takenLanes, request, transaction);
      return claimed;
    });
  }

  /** Fast claim, then the slow one when the fast window was full of blocked followers. */
  async #pickDeliveries(
    parameters: readonly PostgresParameter[],
    transaction: TTransaction,
  ): Promise<PostgresRow[]> {
    const limit = parameters[2] as number;
    const fastRows = await this.#executor.query(this.#statements.claim, parameters, transaction);
    const picked = fastRows.filter((row) => row['delivery_id'] !== null);
    if (picked.length < limit && fastRows[0]!['window_saturated'] === true) {
      const slowRows = await this.#executor.query(
        this.#statements.claimBehindBlockedKeys,
        parameters,
        transaction,
      );
      // Nothing was written, so the slow statement sees the fast statement's picks again.
      // Both pick at most the head of a key, so the union is still one delivery per key.
      const pickedIds = new Set(picked.map((row) => row['delivery_id']));
      picked.push(
        ...slowRows.filter(
          (row) => row['delivery_id'] !== null && !pickedIds.has(row['delivery_id']),
        ),
      );
      picked.sort(
        (left, right) =>
          compareText(left['due_at'] as string, right['due_at'] as string) ||
          compareText(left['delivery_id'] as string, right['delivery_id'] as string),
      );
      picked.length = Math.min(picked.length, limit);
    }
    return picked;
  }

  /** Writes the picked ids into their lane rows and frees the lanes that picked nothing. */
  async #recordClaim(
    claimed: readonly PostgresRow[],
    lanes: ReadonlySet<number>,
    request: ClaimRequest,
    transaction: TTransaction,
  ): Promise<void> {
    const idsByLane = new Map<number, string[]>();
    for (const row of claimed) {
      const lane = readCount(row, 'lane');
      idsByLane.set(lane, [...(idsByLane.get(lane) ?? []), row['delivery_id'] as string]);
    }
    const idleLanes = [...lanes].filter((lane) => !idsByLane.has(lane));
    if (idsByLane.size === 0 && idleLanes.length === 0) {
      return;
    }
    await this.#executor.query(
      this.#statements.recordClaim,
      [
        JSON.stringify([...idsByLane].map(([lane, ids]) => ({ lane, ids }))),
        JSON.stringify(idleLanes),
        request.leaseOwner,
        request.leaseExpiresAt.toISOString(),
      ],
      transaction,
    );
  }

  extendLease(lease: LeaseReference, leaseExpiresAt: Date): Promise<boolean> {
    return this.#whenIssued(lease, (held) =>
      this.#changed(this.#statements.extendLease, [
        lease.deliveryId,
        held.lane,
        held.epoch,
        leaseExpiresAt.toISOString(),
      ]),
    );
  }

  async complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean> {
    return (await this.#completeLeases([lease], now, transaction))[0]!;
  }

  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completeLeases(leases, now, undefined);
  }

  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#whenIssued(lease, (held) =>
      this.#changed(this.#statements.reschedule, [
        lease.deliveryId,
        held.lane,
        held.epoch,
        availableAt.toISOString(),
        JSON.stringify(error),
        now.toISOString(),
      ]),
    );
  }

  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#whenIssued(lease, (held) =>
      this.#changed(this.#statements.bury, [
        lease.deliveryId,
        held.lane,
        held.epoch,
        JSON.stringify(error),
        now.toISOString(),
      ]),
    );
  }

  /** Returns a leased delivery to pending, due now, without counting the attempt. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#whenIssued(lease, (held) =>
      this.#changed(this.#statements.release, [
        lease.deliveryId,
        held.lane,
        held.epoch,
        now.toISOString(),
      ]),
    );
  }

  /** Frees the lanes `leaseOwner` holds that run no delivery, so other workers need not wait for the lease to expire. */
  async relinquish(leaseOwner: string): Promise<void> {
    await this.#executor.query(this.#statements.relinquish, [leaseOwner]);
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
    if (this.#completionHistory) {
      batches.push([this.#statements.purgeCompletionHistoryBatch(), request.completedBefore]);
    }
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
    const stored: Record<string, number> = { pending: 0, dead: 0, completed: 0 };
    for (const row of await this.#executor.query(this.#statements.countByStatus, [])) {
      stored[row['status'] as string] = readCount(row, 'count');
    }
    const [running] = await this.#executor.query(this.#statements.countRunning, []);
    const processing = readCount(running!, 'count');
    let completed = stored['completed']!;
    if (this.#completionHistory) {
      const [history] = await this.#executor.query(this.#statements.countCompletionHistory, []);
      completed += readCount(history!, 'count');
    }
    const oldestDue = await this.#executor.query(this.#statements.oldestDue, [now.toISOString()]);
    return {
      // The counts come from separate statements; a delivery that finishes in between must not make this negative.
      pending: Math.max(0, stored['pending']! - processing),
      processing,
      completed,
      dead: stored['dead']!,
      oldestDueAt: mapOldestDueAt(oldestDue),
    };
  }

  /** Ends the pool this store created from `connectionString`. Pools and executors you passed in stay open. */
  async close(): Promise<void> {
    const pool = this.#ownedPool;
    this.#ownedPool = undefined;
    await pool?.end();
  }

  async #changed(sql: string, parameters: readonly PostgresParameter[]): Promise<boolean> {
    const rows = await this.#executor.query(sql, parameters);
    return readCount(rows[0]!, 'count') > 0;
  }

  /** Runs `operation` for a lease this store issued; any other token cannot be held. */
  async #whenIssued(
    lease: LeaseReference,
    operation: (held: LaneLease) => Promise<boolean>,
  ): Promise<boolean> {
    const held = parseLeaseToken(lease.leaseToken);
    return held !== undefined && (await operation(held));
  }

  /**
   * Deletes the delivery of every lease whose lane epoch still matches. Returns
   * one flag per lease, in order; a lease listed twice completes once.
   */
  async #completeLeases(
    leases: readonly LeaseReference[],
    now: Date,
    transaction: TTransaction | undefined,
  ): Promise<boolean[]> {
    const held = leases.map((lease) => parseLeaseToken(lease.leaseToken));
    const issued = leases.flatMap((lease, index) =>
      held[index] === undefined ? [] : [{ id: lease.deliveryId, ...held[index] }],
    );
    const completedIds = new Set<string>();
    if (issued.length > 0) {
      const rows = await this.#executor.query(
        this.#statements.complete,
        this.#completionHistory
          ? [JSON.stringify(issued), now.toISOString()]
          : [JSON.stringify(issued)],
        transaction,
      );
      for (const row of rows) {
        completedIds.add(row['id'] as string);
      }
    }
    return leases.map(
      (lease, index) => held[index] !== undefined && completedIds.delete(lease.deliveryId),
    );
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

function toEventRows(plan: AppendPlan, firstSequences: ReadonlyMap<string, bigint>): EventRow[] {
  const nextSequences = new Map(firstSequences);
  const handlersByEvent = new Map<string, string[]>();
  for (const delivery of plan.deliveries) {
    handlersByEvent.set(delivery.event_id, [
      ...(handlersByEvent.get(delivery.event_id) ?? []),
      delivery.handler,
    ]);
  }
  return plan.events.map((event) => {
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
      handlers: handlersByEvent.get(event.id) ?? [],
    };
  });
}

function compareText(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}
