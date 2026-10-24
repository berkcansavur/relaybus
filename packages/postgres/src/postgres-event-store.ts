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
import { Client, Pool, type PoolClient, type PoolConfig } from 'pg';
import type {
  PostgresExecutor,
  PostgresParameter,
  PostgresQueryOptions,
  PostgresRow,
} from './executor.js';
import { resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { NotificationListener } from './notification-listener.js';
import { PgExecutor } from './pg-executor.js';
import {
  mapClaimedDelivery,
  mapDeliveryRecord,
  mapOldestDueAt,
  parseLeaseToken,
  readCount,
  type LaneLease,
} from './row-mapping.js';
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
/** Attempts of the held-lane claim before a stale statement is given up for this call (a later claim finds the work). */
const MAXIMUM_STALE_CLAIM_ATTEMPTS = 3;

/**
 * Least time between two wake-up notifications of one store. A transaction that sends `NOTIFY` holds a
 * database-wide lock from its commit until the commit is flushed, so a notification per publishing
 * transaction made concurrent publishers commit one at a time.
 */
const NOTIFICATION_INTERVAL_MS = 10;

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

interface HandedOutClaim extends LaneLease {
  readonly owner: string;
  readonly deliveryId: string;
}

function handedOutKey(deliveryId: string, leaseToken: string): string {
  return `${deliveryId}\u0000${leaseToken}`;
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
  readonly #schemaMarker: string;
  readonly #purgeBatchSize: number;
  readonly #completionHistory: boolean;
  readonly #logger: RelayBusLogger;
  /** Passed with every statement whose text is fixed: asks the executor to prepare it (`preparedStatements`). */
  readonly #queryOptions: PostgresQueryOptions | undefined;
  /** Every claim handed to a caller and not settled yet, by delivery id and exact lease token. */
  readonly #handedOut = new Map<string, HandedOutClaim>();
  readonly #unreconciled = new Set<string>();
  readonly #generations = new Map<string, bigint>();
  readonly #ownerQueues = new Map<string, Promise<void>>();
  /** Owners whose last claim was filled from the lanes they held. */
  readonly #drainingOwners = new Set<string>();
  #ownedPool: Pool | undefined;
  /** The `pg` pool behind the store, when it has one: listening for notifications needs a connection of its own. */
  readonly #pool: Pool | undefined;
  readonly #notificationChannel: string;
  #lastNotificationAt = Number.NEGATIVE_INFINITY;
  #trailingNotification: ReturnType<typeof setTimeout> | undefined;
  /** The transactions `transaction` runs right now, and whether one of their appends left its wake-up for after the commit. */
  readonly #ownTransactions = new Map<unknown, { wake: boolean }>();

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
    const preparedStatements = options.preparedStatements ?? true;
    if (typeof preparedStatements !== 'boolean') {
      throw new ConfigurationError(
        `preparedStatements must be a boolean, received ${String(preparedStatements)}`,
      );
    }
    this.#queryOptions = preparedStatements ? { prepare: true } : undefined;
    const names = resolvePostgresNames(options);
    this.#statements = new PostgresStatements(names);
    this.#schemaStatements = createPostgresSchemaStatements(options);
    this.#setupLockKey = this.#statements.setupLockKey;
    this.#schemaMarker = postgresSchemaMarker(this.#schemaStatements.slice(0, -1));
    this.#purgeBatchSize = purgeBatchSize;
    this.#completionHistory = names.completionHistory;
    this.#logger = options.logger ?? console;
    this.#notificationChannel = names.notificationChannel;
    if (options.executor !== undefined) {
      this.#executor = options.executor;
      this.#pool = undefined;
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
      this.#pool = pool;
      // Without an executor the transaction type is PoolClient, as the default type argument states.
      this.#executor = new PgExecutor(pool, {
        logger: this.#logger,
      }) as unknown as PostgresExecutor<TTransaction>;
    }
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
    await this.#executor.transaction(async (transaction) => {
      await this.#executor.query(this.#statements.setupLock, [this.#setupLockKey], transaction);
      if (await this.#schemaIsCurrent(transaction)) {
        return;
      }
      for (const statement of this.#schemaStatements) {
        await this.#executor.query(statement, [], transaction);
      }
    });
  }

  async #schemaIsCurrent(transaction?: TTransaction): Promise<boolean> {
    const rows = await this.#executor.query(
      this.#statements.schemaCurrent,
      [this.#schemaMarker],
      transaction,
    );
    return rows[0]!['current'] === true;
  }

  async transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T> {
    const own = { wake: false };
    const result = await this.#executor.transaction(async (transaction) => {
      this.#ownTransactions.set(transaction, own);
      try {
        return await work(transaction);
      } finally {
        this.#ownTransactions.delete(transaction);
      }
    });
    if (own.wake) {
      this.#wakeAfterCommit();
    }
    return result;
  }

  /**
   * Writes the entries in `transaction`, normally with one statement
   * ({@link PostgresStatements.insertFreshEvents}): keyed events reserve their sequence numbers (locking
   * the key rows in sorted order, so publishers cannot deadlock inside one call), then the new events and
   * their deliveries are inserted, ordered by primary key, and one notification wakes the dispatchers
   * when a delivery is due now. Events that already existed get their missing handlers' deliveries in
   * two more statements. Two transactions that append keys in opposite order across several calls can
   * still deadlock (40P01); the database rolls one back and the caller retries.
   * A duplicate event id still consumes a sequence number; gaps are harmless
   * because only the order of sequences matters.
   */
  async append(
    transaction: TTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    const plan = planAppend(entries);
    if (plan.events.length === 0) {
      return { appendedEventIds: [], duplicateEventIds: [], appendedDeliveryCount: 0 };
    }
    const createdAt = now.toISOString();
    const wakeAt = this.#wakeAt(plan.deliveries, now, transaction);
    const handlersByEvent = new Map<string, string[]>();
    for (const delivery of plan.deliveries) {
      handlersByEvent.set(delivery.event_id, [
        ...(handlersByEvent.get(delivery.event_id) ?? []),
        delivery.handler,
      ]);
    }
    const keyCounts = new Map<string, number>();
    for (const event of plan.events) {
      if (event.key !== null) {
        keyCounts.set(event.key, (keyCounts.get(event.key) ?? 0) + 1);
      }
    }
    const fresh = await this.#query(
      this.#statements.insertFreshEvents,
      [
        JSON.stringify(
          plan.events.map((event, ordinal) => ({
            ordinal,
            ...toEventRow(event, null, handlersByEvent.get(event.id) ?? []),
          })),
        ),
        createdAt,
        JSON.stringify(plan.deliveries),
        JSON.stringify([...keyCounts].map(([key, count]) => ({ key, count }))),
        wakeAt,
      ],
      transaction,
    );
    const insertedEventIds = new Set(
      fresh.filter((row) => row['inserted'] === true).map((row) => row['id'] as string),
    );
    let appendedDeliveryCount = readCount(fresh[0]!, 'deliveries');
    const remaining = plan.deliveries.filter(
      (delivery) => !insertedEventIds.has(delivery.event_id),
    );
    if (remaining.length > 0) {
      appendedDeliveryCount += await this.#appendToExistingEvents(
        transaction,
        plan,
        fresh,
        remaining,
        handlersByEvent,
        insertedEventIds,
        createdAt,
        wakeAt,
      );
    }
    const appendedEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    for (const { eventId, repeated } of plan.outcomes) {
      (!repeated && insertedEventIds.has(eventId) ? appendedEventIds : duplicateEventIds).push(
        eventId,
      );
    }
    return { appendedEventIds, duplicateEventIds, appendedDeliveryCount };
  }

  /**
   * The time the publishing statements notify the dispatchers for (deliveries due by then wake them),
   * or `null` for no notification. An append in a transaction of the caller's always notifies, at its
   * commit. In a transaction of this store, only the first append within {@link NOTIFICATION_INTERVAL_MS}
   * does; the others leave the wake-up to {@link #wakeAfterCommit}, which runs once their transaction
   * committed, so no committed delivery waits for the poll.
   */
  #wakeAt(deliveries: readonly DeliveryRow[], now: Date, transaction: TTransaction): string | null {
    if (!deliveries.some((delivery) => Date.parse(delivery.available_at) <= now.getTime())) {
      return null;
    }
    const own = this.#ownTransactions.get(transaction);
    if (own !== undefined && Date.now() - this.#lastNotificationAt < NOTIFICATION_INTERVAL_MS) {
      own.wake = true;
      return null;
    }
    this.#lastNotificationAt = Date.now();
    return now.toISOString();
  }

  /**
   * Wakes the dispatchers for a committed transaction whose appends sent no notification: at once when
   * none was sent within {@link NOTIFICATION_INTERVAL_MS}, otherwise once when the interval ends. A
   * wake-up already scheduled covers this transaction too, since it is sent after this commit.
   */
  #wakeAfterCommit(): void {
    if (this.#trailingNotification !== undefined) {
      return;
    }
    this.#trailingNotification = setTimeout(
      () => {
        this.#trailingNotification = undefined;
        this.#lastNotificationAt = Date.now();
        this.#query(this.#statements.notify, []).catch((error: unknown) => {
          this.#logger.error(
            `[PostgresEventStore](wakeAfterCommit) Waking the dispatchers failed, they find the deliveries when they poll | error=${error instanceof Error ? error.message : String(error)}`,
          );
        });
      },
      Math.max(0, this.#lastNotificationAt + NOTIFICATION_INTERVAL_MS - Date.now()),
    );
  }

  /**
   * Adds the deliveries of events the publishing statement found already stored. An event that already
   * exists may be getting the same new handler from another transaction at this very moment. Whoever
   * holds the event decides, so the freshness check of the insert sees what the other transaction
   * committed, even if its delivery has completed since. An existing event that a purge removed
   * meanwhile is inserted again (with the sequence reserved for it) instead of being skipped; its id is
   * added to `insertedEventIds`. Returns how many deliveries were inserted.
   */
  async #appendToExistingEvents(
    transaction: TTransaction,
    plan: AppendPlan,
    fresh: readonly PostgresRow[],
    deliveries: readonly DeliveryRow[],
    handlersByEvent: ReadonlyMap<string, string[]>,
    insertedEventIds: Set<string>,
    createdAt: string,
    wakeAt: string | null,
  ): Promise<number> {
    const sequences = new Map(
      fresh.map((row) => [row['id'] as string, row['sequence'] as string | null]),
    );
    const reinserted: string[] = [];
    let existing = plan.events
      .filter((event) => !insertedEventIds.has(event.id))
      .map((event) =>
        toEventRow(event, sequences.get(event.id)!, handlersByEvent.get(event.id) ?? []),
      );
    while (existing.length > 0) {
      const locked = new Set(
        (
          await this.#query(
            this.#statements.lockExistingEvents,
            [JSON.stringify(existing.map((row) => row.id).sort())],
            transaction,
          )
        ).map((row) => row['id'] as string),
      );
      const gone = existing.filter((row) => !locked.has(row.id));
      if (gone.length > 0) {
        const inserted = await this.#query(
          this.#statements.insertEvents,
          [JSON.stringify(gone), createdAt],
          transaction,
        );
        for (const row of inserted) {
          insertedEventIds.add(row['id'] as string);
          reinserted.push(row['id'] as string);
        }
      }
      existing = gone.filter((row) => !insertedEventIds.has(row.id));
    }
    const inserted = await this.#query(
      this.#statements.insertDeliveries,
      [JSON.stringify(deliveries), createdAt, JSON.stringify(reinserted), wakeAt],
      transaction,
    );
    return inserted.length;
  }

  /**
   * Claims up to `limit` due deliveries, normally with one statement
   * ({@link PostgresStatements.claimLanes}): from the lanes this owner already holds, then from free lanes
   * it takes in the same statement, recording the batch in the lane rows. A claim that would have to take
   * over a lane whose previous holder still runs deliveries under an expired lease continues in a
   * transaction that charges their lost attempts first, taking free lanes a few at a time (4, 8, 16, ...);
   * so does a claim whose statement kept seeing a lane it holds change under it, and one that came up
   * short with more free lanes left than it looked at. The step that fails after
   * the first one has recorded deliveries returns those instead of failing: a recorded delivery nobody
   * received would be held for a whole lease. See {@link PostgresStatements.claim} for the rules a delivery
   * must satisfy.
   */
  claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || !(request.limit > 0)) {
      return Promise.resolve([]);
    }
    return this.#serialized(request.leaseOwner, () => this.#claim(request));
  }

  /**
   * Runs `work` after the earlier claims of the same owner on this store settled. The
   * bookkeeping of what was handed out and the reconciliation of a failed claim need
   * one claim at a time per owner (the dispatcher claims one batch at a time anyway;
   * `runOnce` next to a running dispatcher is what this guards against).
   */
  #serialized<T>(owner: string, work: () => Promise<T>): Promise<T> {
    const previous = this.#ownerQueues.get(owner) ?? Promise.resolve();
    const run = previous.then(work, work);
    const tail = run.then(
      () => undefined,
      () => undefined,
    );
    this.#ownerQueues.set(owner, tail);
    void tail.then(() => {
      if (this.#ownerQueues.get(owner) === tail) {
        this.#ownerQueues.delete(owner);
      }
    });
    return run;
  }

  async #claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (
      this.#unreconciled.has(request.leaseOwner) &&
      !(await this.#reconcile(request.leaseOwner))
    ) {
      // The generation moved on and the lanes were not told: claiming now would take the owner's live lanes for foreign ones.
      throw new Error(
        `[PostgresEventStore](claim) Giving back unreturned deliveries still fails, so no claim is made | owner=${request.leaseOwner}`,
      );
    }
    // An owner whose last claim was filled from the lanes it held is draining a backlog there: those
    // lanes are asked first.
    const heldFirst = this.#drainingOwners.has(request.leaseOwner);
    let rows: readonly PostgresRow[];
    try {
      rows = await this.#claimInOneStatement(
        request,
        heldFirst ? this.#statements.claimHeldLanes : this.#statements.claimLanes,
      );
    } catch (error) {
      await this.#reconcile(request.leaseOwner);
      throw error;
    }
    const heldLanes = new Set(JSON.parse(rows[0]!['examined_lanes'] as string) as number[]);
    const claimed = this.#takeClaimed(rows, request.leaseOwner);
    if (heldFirst && claimed.length < request.limit && rows[0]!['stale'] !== true) {
      try {
        rows = await this.#claimInOneStatement(
          { ...request, limit: request.limit - claimed.length },
          this.#statements.claimLanes,
        );
      } catch (error) {
        await this.#reconcile(request.leaseOwner);
        if (claimed.length === 0) {
          this.#drainingOwners.delete(request.leaseOwner);
          throw error;
        }
        this.#logger.error(
          `[PostgresEventStore](claim) Claiming from free lanes failed, returning the deliveries already claimed | claimed=${claimed.length} | error=${error instanceof Error ? error.message : String(error)}`,
        );
        return this.#handOutInDueOrder(claimed, request, heldLanes);
      }
      claimed.push(...this.#takeClaimed(rows, request.leaseOwner));
    }
    if (
      claimed.length < request.limit &&
      (rows[0]!['takeover_waiting'] === true ||
        rows[0]!['unexamined_free_lanes'] === true ||
        rows[0]!['stale'] === true)
    ) {
      try {
        const more = await this.#claimMore(request, claimed.length);
        this.#handOut(more, request.leaseOwner);
        claimed.push(...more);
      } catch (error) {
        await this.#reconcile(request.leaseOwner);
        if (claimed.length === 0) {
          throw error;
        }
        this.#logger.error(
          `[PostgresEventStore](claim) Taking more lanes failed, returning the deliveries already claimed | claimed=${claimed.length} | error=${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
    return this.#handOutInDueOrder(claimed, request, heldLanes);
  }

  /** The deliveries a claim statement picked, registered as handed out to `leaseOwner`. */
  #takeClaimed(rows: readonly PostgresRow[], leaseOwner: string): PostgresRow[] {
    const claimed = rows.filter((row) => row['delivery_id'] !== null);
    this.#forgetFinished(rows[0]!, leaseOwner, claimed);
    this.#handOut(claimed, leaseOwner);
    return claimed;
  }

  #handOutInDueOrder(
    claimed: PostgresRow[],
    request: ClaimRequest,
    heldLanes: ReadonlySet<number>,
  ): ClaimedDelivery[] {
    if (
      claimed.length >= request.limit &&
      claimed.every((row) => heldLanes.has(readCount(row, 'lane')))
    ) {
      this.#drainingOwners.add(request.leaseOwner);
    } else {
      this.#drainingOwners.delete(request.leaseOwner);
    }
    // The steps pick from different lanes; hand the batch out in due order, like a single query would.
    claimed.sort(compareDue);
    return claimed.map((row) => mapClaimedDelivery(row, request.leaseExpiresAt));
  }

  /**
   * Runs `statement` ({@link PostgresStatements.claimLanes} or its held-lane form), again when it reports that it waited for a lane lock and
   * read deliveries with an older snapshot than the lane it got. A held lane is only locked by an
   * operation of its own owner (another claim, a reschedule), so a few attempts are enough.
   */
  async #claimInOneStatement(
    request: ClaimRequest,
    statement: string,
  ): Promise<readonly PostgresRow[]> {
    const parameters = [
      request.now.toISOString(),
      JSON.stringify(request.handlers),
      request.limit,
      request.leaseOwner,
      request.leaseExpiresAt.toISOString(),
      this.#generation(request.leaseOwner).toString(),
    ];
    let rows = await this.#query(statement, parameters);
    for (
      let attempt = 1;
      attempt < MAXIMUM_STALE_CLAIM_ATTEMPTS && rows[0]!['stale'] === true;
      attempt += 1
    ) {
      rows = await this.#query(statement, parameters);
    }
    return rows;
  }

  /**
   * Second step of {@link PostgresEventStore.claim}, in one transaction: takes free lanes a few at a
   * time, lanes to take over included (their lost attempts are charged first), and records the batch.
   * `alreadyClaimed` deliveries of the first step are in flight, so they are not picked again.
   */
  async #claimMore(request: ClaimRequest, alreadyClaimed: number): Promise<PostgresRow[]> {
    const now = request.now.toISOString();
    const leaseExpiresAt = request.leaseExpiresAt.toISOString();
    const handlers = JSON.stringify(request.handlers);
    // Taken before the transaction starts: a transaction that reaches the server late keeps its generation.
    const generation = this.#generation(request.leaseOwner).toString();
    return this.#executor.transaction(async (transaction) => {
      const claimed: PostgresRow[] = [];
      const remaining = (): number => request.limit - alreadyClaimed - claimed.length;
      const takenLanes = new Set<number>();
      for (let laneCount = INITIAL_LANES_PER_ROUND; remaining() > 0;) {
        const taken = await this.#query(
          this.#statements.takeFreeLanes,
          [now, handlers, laneCount, request.leaseOwner, leaseExpiresAt, generation],
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

  /**
   * The generation of the owner's claim statements. It only grows, also across restarts (it starts at the
   * clock), and a reconciliation moves it up, which fences off every earlier statement still in flight.
   */
  #generation(owner: string, advance = false): bigint {
    const current = this.#generations.get(owner) ?? BigInt(Date.now());
    const next = advance
      ? current + 1n > BigInt(Date.now())
        ? current + 1n
        : BigInt(Date.now())
      : current;
    this.#generations.set(owner, next);
    return next;
  }

  /** Forgets a handed-out lease once the store answered about it (any answer: the claim is over or fenced out). */
  async #settled(lease: LeaseReference, work: Promise<boolean>): Promise<boolean> {
    const result = await work;
    this.#handedOut.delete(handedOutKey(lease.deliveryId, lease.leaseToken));
    return result;
  }

  /** Remembers the claims handed to the caller of `claim` until they are settled. */
  #handOut(rows: readonly PostgresRow[], leaseOwner: string): void {
    for (const row of rows) {
      const claim = mapClaimedDelivery(row, new Date(0));
      this.#handedOut.set(handedOutKey(claim.deliveryId, claim.leaseToken), {
        owner: leaseOwner,
        deliveryId: claim.deliveryId,
        ...parseLeaseToken(claim.leaseToken)!,
      });
    }
  }

  /**
   * Drops the registrations of the owner's held lanes whose delivery no longer runs there
   * (completed inside the caller's transaction, deleted, or listed no more). Registrations of
   * lanes this claim did not see stay: their tokens may still be valid.
   */
  #forgetFinished(row: PostgresRow, leaseOwner: string, claimed: readonly PostgresRow[]): void {
    // Lanes the statement freed are examined too: nothing runs there any more.
    const heldLanes = new Set(JSON.parse(row['examined_lanes'] as string) as number[]);
    const running = new Set([
      ...(JSON.parse(row['running_ids'] as string) as string[]),
      ...claimed.map((picked) => picked['delivery_id'] as string),
    ]);
    for (const [key, entry] of this.#handedOut) {
      if (
        entry.owner === leaseOwner &&
        heldLanes.has(entry.lane) &&
        !running.has(entry.deliveryId)
      ) {
        this.#handedOut.delete(key);
      }
    }
  }

  /**
   * After a claim failed, possibly after the database recorded its batch: gives back the recorded
   * deliveries that were never handed out, keeping exactly the claims handed out and not settled.
   * A failure is remembered and retried before the owner's next claim: other deliveries keep
   * renewing the lane, so an unreturned one would otherwise never be recovered.
   */
  async #reconcile(leaseOwner: string): Promise<boolean> {
    // Claim statements of the generation before are fenced off by the new one.
    const generation = this.#generation(leaseOwner, true);
    const handedOut = [...this.#handedOut.values()]
      .filter((entry) => entry.owner === leaseOwner)
      .map(({ deliveryId, lane, epoch, claims }) => ({ id: deliveryId, lane, epoch, claims }));
    try {
      await this.#query(this.#statements.reconcileClaims, [
        leaseOwner,
        JSON.stringify(handedOut),
        generation.toString(),
      ]);
      this.#unreconciled.delete(leaseOwner);
      return true;
    } catch (error) {
      this.#unreconciled.add(leaseOwner);
      this.#logger.error(
        `[PostgresEventStore](reconcile) Giving back unreturned deliveries failed, it is retried before the next claim | owner=${leaseOwner} | error=${error instanceof Error ? error.message : String(error)}`,
      );
      return false;
    }
  }

  /** Fast claim, then the slow one when the fast window was full of blocked followers. */
  async #pickDeliveries(
    parameters: readonly PostgresParameter[],
    transaction: TTransaction,
  ): Promise<PostgresRow[]> {
    const limit = parameters[2] as number;
    const fastRows = await this.#query(this.#statements.claim, parameters, transaction);
    const picked = fastRows.filter((row) => row['delivery_id'] !== null);
    if (picked.length < limit && fastRows[0]!['window_saturated'] === true) {
      const slowRows = await this.#query(
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
      // The two statements read at different moments: a dead letter requeued in between can put a key's
      // predecessor in one result and its successor in the other. Only the lowest of a key may run.
      const seenKeys = new Set<string>();
      const kept = [...picked]
        .sort((left, right) => compareSequence(left, right))
        .filter((row) => {
          if (row['delivery_key'] === null) {
            return true;
          }
          const group = `${row['handler'] as string}\u0000${row['delivery_key'] as string}`;
          return !seenKeys.has(group) && Boolean(seenKeys.add(group));
        });
      kept.sort(compareDue);
      return kept.slice(0, limit);
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
    await this.#query(
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
        held.claims,
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
    return this.#settled(
      lease,
      this.#whenIssued(lease, (held) =>
        this.#changed(this.#statements.reschedule, [
          lease.deliveryId,
          held.lane,
          held.epoch,
          held.claims,
          availableAt.toISOString(),
          JSON.stringify(error),
          now.toISOString(),
        ]),
      ),
    );
  }

  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#settled(
      lease,
      this.#whenIssued(lease, (held) =>
        this.#changed(this.#statements.bury, [
          lease.deliveryId,
          held.lane,
          held.epoch,
          held.claims,
          JSON.stringify(error),
          now.toISOString(),
        ]),
      ),
    );
  }

  /** Returns a leased delivery to pending, due now, without counting the attempt. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#settled(
      lease,
      this.#whenIssued(lease, (held) =>
        this.#changed(this.#statements.release, [
          lease.deliveryId,
          held.lane,
          held.epoch,
          held.claims,
          now.toISOString(),
        ]),
      ),
    );
  }

  /**
   * Calls `listener` soon after a transaction that published deliveries due at once commits, in any
   * process: `append` sends one `pg_notify` per call (merged per transaction, none for delayed
   * deliveries), and this listens with `LISTEN` on a connection of its own, opened with the settings of
   * the store's pool (one connection more than the pool's `max`). A lost connection is replaced after a
   * second. Resolves with `undefined` for a store built on an `executor`, which has no connection of its
   * own to listen on: polling then finds new deliveries (and the bus still wakes its own dispatcher).
   */
  async watchNewDeliveries(listener: () => void): Promise<(() => Promise<void>) | undefined> {
    if (this.#pool === undefined) {
      return undefined;
    }
    const options = this.#pool.options;
    const watcher = new NotificationListener(
      async () => {
        // A client of its own, with the pool's connection settings: the pool keeps all its connections.
        const client = new Client(options);
        await client.connect();
        return client;
      },
      this.#notificationChannel,
      listener,
      this.#logger,
    );
    await watcher.start();
    return () => watcher.close();
  }

  /** Frees the lanes `leaseOwner` holds that run no delivery, so other workers need not wait for the lease to expire. */
  async relinquish(leaseOwner: string): Promise<void> {
    await this.#query(this.#statements.relinquish, [leaseOwner]);
  }

  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    const statement = this.#statements.listDeliveries(query);
    const rows = await this.#executor.query(statement.sql, statement.parameters);
    return rows.map(mapDeliveryRecord);
  }

  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    const rows = await this.#query(this.#statements.requeue, [
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
    const batches: Array<[string, Date | undefined, string]> = [
      [
        this.#statements.purgeCompletedBatch(),
        request.completedBefore,
        this.#statements.deleteOrphanEventsWithoutCompletion,
      ],
      [
        this.#statements.purgeDeadBatch(),
        request.deadBefore,
        this.#statements.deleteOrphanEventsWithoutCompletion,
      ],
    ];
    if (this.#completionHistory) {
      batches.push([
        this.#statements.purgeCompletionHistoryBatch(),
        request.completedBefore,
        this.#statements.deleteOrphanEventsWithoutCompletion,
      ]);
    }
    for (const [sql, cutoff, deleteOrphans] of batches) {
      if (cutoff === undefined) {
        continue;
      }
      let deletedInBatch = this.#purgeBatchSize;
      while (deletedInBatch === this.#purgeBatchSize) {
        const batch = await this.#purgeBatch(sql, cutoff, deleteOrphans);
        deletedInBatch = batch.deletedDeliveries;
        total.deletedDeliveries += batch.deletedDeliveries;
        total.deletedEvents += batch.deletedEvents;
      }
    }
    const orphanCutoff = [request.completedBefore, request.deadBefore]
      .filter((cutoff) => cutoff !== undefined)
      .sort((first, second) => first.getTime() - second.getTime())[0];
    if (orphanCutoff !== undefined) {
      total.deletedEvents += await this.#purgeOrphanEvents(
        this.#statements.lockOrphanEventsBatch,
        this.#statements.deleteOrphanEventsWithoutCompletion,
        orphanCutoff,
        false,
      );
    }
    if (request.completedBefore !== undefined) {
      total.deletedEvents += await this.#purgeOrphanEvents(
        this.#statements.lockCompletedOrphanEventsBatch,
        this.#statements.deleteExpiredCompletedOrphanEvents,
        request.completedBefore,
        true,
      );
    }
    return total;
  }

  async statistics(now: Date): Promise<EventStoreStatistics> {
    const stored: Record<string, number> = { pending: 0, dead: 0, completed: 0 };
    for (const row of await this.#query(this.#statements.countByStatus, [])) {
      stored[row['status'] as string] = readCount(row, 'count');
    }
    const [running] = await this.#query(this.#statements.countRunning, []);
    const processing = readCount(running!, 'count');
    let completed = stored['completed']!;
    if (this.#completionHistory) {
      const [history] = await this.#query(this.#statements.countCompletionHistory, []);
      completed += readCount(history!, 'count');
    }
    const oldestDue = await this.#query(this.#statements.oldestDue, [now.toISOString()]);
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
    clearTimeout(this.#trailingNotification);
    this.#trailingNotification = undefined;
    const pool = this.#ownedPool;
    this.#ownedPool = undefined;
    await pool?.end();
  }

  /** Runs a statement of {@link PostgresStatements} whose text never changes, prepared when `preparedStatements` is on. */
  #query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: TTransaction,
  ): Promise<readonly PostgresRow[]> {
    return this.#executor.query(sql, parameters, transaction, this.#queryOptions);
  }

  async #changed(sql: string, parameters: readonly PostgresParameter[]): Promise<boolean> {
    const rows = await this.#query(sql, parameters);
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
   * Deletes the delivery of every lease that is still the current claim of its
   * delivery. Returns one flag per lease, in order. Results are matched to leases
   * by position, so two leases of one delivery (a stale and a current one) get
   * their own answers; a lease listed twice completes once, at its first position.
   */
  async #completeLeases(
    leases: readonly LeaseReference[],
    now: Date,
    transaction: TTransaction | undefined,
  ): Promise<boolean[]> {
    const issued: Array<{ ord: number; id: string } & LaneLease> = [];
    const seen = new Set<string>();
    leases.forEach((lease, ord) => {
      const held = parseLeaseToken(lease.leaseToken);
      const identity = `${lease.deliveryId}\u0000${lease.leaseToken}`;
      if (held !== undefined && !seen.has(identity)) {
        seen.add(identity);
        issued.push({ ord, id: lease.deliveryId, ...held });
      }
    });
    const completed = new Set<number>();
    if (issued.length > 0) {
      const rows = await this.#query(
        this.#statements.complete,
        [JSON.stringify(issued), now.toISOString()],
        transaction,
      );
      for (const row of rows) {
        completed.add(readCount(row, 'ord'));
      }
    }
    if (transaction === undefined) {
      // Inside the caller's transaction the completion is only provisional: it may roll back and retry.
      for (const lease of leases) {
        this.#handedOut.delete(handedOutKey(lease.deliveryId, lease.leaseToken));
      }
    }
    return leases.map((_, ord) => completed.has(ord));
  }

  async #purgeOrphanEvents(
    lockStatement: string,
    deleteStatement: string,
    cutoff: Date,
    recheckCutoff: boolean,
  ): Promise<number> {
    let deletedEvents = 0;
    let removedInBatch = this.#purgeBatchSize;
    while (removedInBatch === this.#purgeBatchSize) {
      removedInBatch = await this.#executor.transaction(async (transaction) => {
        const locked = await this.#query(
          lockStatement,
          [cutoff.toISOString(), this.#purgeBatchSize],
          transaction,
        );
        const ids = JSON.stringify(locked.map((row: PostgresRow) => row['id'] as string));
        const removed = await this.#query(
          deleteStatement,
          recheckCutoff ? [ids, cutoff.toISOString()] : [ids],
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
    deleteOrphans: string,
  ): Promise<{ deletedDeliveries: number; deletedEvents: number }> {
    return this.#executor.transaction(async (transaction) => {
      const deleted = await this.#query(
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
      const locked = await this.#query(this.#statements.lockEvents, [eventIds], transaction);
      const removed = await this.#query(
        deleteOrphans,
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

function toEventRow(
  event: EventEnvelope,
  sequence: string | null,
  handlers: readonly string[],
): EventRow {
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
    handlers,
  };
}

function compareSequence(left: PostgresRow, right: PostgresRow): number {
  const first = BigInt((left['sequence'] as string | null) ?? 0);
  const second = BigInt((right['sequence'] as string | null) ?? 0);
  return Number(first > second) - Number(first < second);
}

function compareDue(left: PostgresRow, right: PostgresRow): number {
  return (
    compareText(left['due_at'] as string, right['due_at'] as string) ||
    compareText(left['delivery_id'] as string, right['delivery_id'] as string)
  );
}

function compareText(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}
