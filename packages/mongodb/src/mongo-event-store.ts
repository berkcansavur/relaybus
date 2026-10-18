import {
  ConfigurationError,
  createUuidV7,
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
  type IdentifierGenerator,
  type LeaseReference,
  type PurgeRequest,
  type PurgeResult,
  type RelayBusLogger,
} from '@relaybus/core';
import type {
  AnyBulkWriteOperation,
  ClientSession,
  Collection,
  Db,
  Document,
  Filter,
  MongoClient,
  TransactionOptions,
  UpdateFilter,
} from 'mongodb';
import { ClaimScan, orderingIdentity } from './claim-scan.js';
import {
  toCompletedDeliveryDocument,
  toCompletedDeliveryRecord,
  toDeliveryDocument,
  toDeliveryRecord,
  toEnvelope,
  toErrorInfo,
  toEventDocument,
  type CompletedDeliveryDocument,
  type DeliveryDocument,
  type EventDocument,
  type KeySequenceDocument,
  type LaneDocument,
} from './documents.js';
import { LANE_COUNT, formatLeaseToken, laneOf, parseLeaseToken, type LaneLease } from './lanes.js';
import {
  createTransactionsUnsupportedError,
  isTransactionsUnsupportedError,
} from './mongo-errors.js';
import { assertCollectionPrefix, assertDatabaseName } from './naming.js';

export interface MongoEventStoreOptions {
  /** Connected client. The store never closes it. */
  readonly client: MongoClient;
  /** Database holding the relaybus collections. Default: the client's default database. */
  readonly database?: string;
  /** Prefix of the collection names. Default `relaybus_`. */
  readonly collectionPrefix?: string;
  /**
   * Options of the transactions the store opens. Default: snapshot read
   * concern, majority write concern, primary reads.
   */
  readonly transactionOptions?: TransactionOptions;
  /** Rows deleted per purge step. Default 500. */
  readonly purgeBatchSize?: number;
  /**
   * Keep an append-only record of every completed delivery in
   * `<prefix>completed_deliveries`, listed as `completed`, counted by
   * `statistics` and removed by `purge({ completedBefore })`. Default false: a
   * completed delivery is deleted and leaves nothing behind. Use the same value
   * in every process.
   */
  readonly completionHistory?: boolean;
  /** Receives warnings about best-effort steps that failed (parking). Default `console`. */
  readonly logger?: RelayBusLogger;
  /**
   * @deprecated Ignored. Lease tokens name the lane and its epoch
   * (`<laneId>:<epoch>`) and are no longer generated.
   */
  readonly generateIdentifier?: IdentifierGenerator;
}

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
};
const NAMESPACE_EXISTS = 48;
const INDEX_NOT_FOUND = 27;
const DEFAULT_PURGE_BATCH_SIZE = 500;
const DEFAULT_LIST_LIMIT = 100;
const MINIMUM_CANDIDATE_BATCH_SIZE = 32;
/** Deliveries that waited behind one unfinished predecessor before they are parked. */
const PARK_BACKLOG_THRESHOLD = 32;
const PARK_UPDATE_CHUNK_SIZE = 1_000;
/** Documents converted per write while `setup()` upgrades a 0.1.0 database. */
const UPGRADE_BATCH_SIZE = 1_000;
/** Lanes a claim takes in its first round; every further round takes twice as many. */
const INITIAL_LANES_PER_ROUND = 4;
/**
 * Lanes one candidate query covers. A query over more lanes than the planner
 * explodes into merged index scans (200 by default) would sort every due
 * delivery of those lanes in memory instead of reading them in due order.
 */
const LANES_PER_CANDIDATE_QUERY = 128;
/** Key of the claim index. A 0.1.0 index named `claim` with another key is dropped by `setup()`. */
const CLAIM_INDEX_KEY = { laneId: 1, dueAt: 1, _id: 1, handler: 1 } as const;
/** Indexes of 0.1.0 that no query uses any more. */
const OBSOLETE_INDEXES = ['running'];

/** Fields that 0.1.0 stored while a delivery was leased. */
const LEASE_FIELDS = { leaseOwner: '', leaseToken: '', leaseExpiresAt: '' } as const;
/** Fields that exist only while a delivery is unfinished. */
const FINISHED_FIELDS = { dueAt: '', lane: '' } as const;

/** A lane a claim holds: already held before it, or taken by it. */
interface ScopeLane {
  readonly laneId: number;
  /** Epoch of this owner's lease. */
  readonly epoch: number;
  /** `inFlight` of the lane as the claim read it: deliveries this owner claimed earlier. */
  readonly listed: readonly string[];
}

/** The fields purge reads from a delivery or a completion history record. */
interface PurgeableDocument {
  readonly _id: string;
  readonly eventId: string;
  readonly status?: string;
  readonly completedAt?: Date;
  readonly updatedAt?: Date;
}

/** A lease of this store, with the delivery it names. */
interface IssuedLease extends LaneLease {
  readonly deliveryId: string;
}

/**
 * Event store on MongoDB 6+ multi-document transactions (replica set or sharded
 * cluster). The transaction handle is the driver's `ClientSession`, so the same
 * session can carry your own writes (native driver or Mongoose).
 *
 * Guarantees:
 * - **Atomic, idempotent append.** `append` is atomic with the caller's writes
 *   and idempotent on event id and on (event id, handler). An event remembers
 *   every handler it ever had a delivery for, so a redelivered or republished
 *   event never runs a completed handler again although completed deliveries are
 *   deleted. Duplicates are detected by reading, never by catching a
 *   duplicate-key error, which would abort the surrounding transaction.
 * - **Per-key order.** Per-key sequences come from a counter document inside the
 *   append transaction. Two transactions appending the same key cannot both hold
 *   the counter, so the loser conflicts, restarts after the winner committed, and
 *   its sequence is higher: sequence order is commit order. A keyed delivery is
 *   claimed only when no earlier delivery of its handler and key is pending and
 *   no other delivery of them runs.
 * - **Lane leases.** Every delivery belongs to one of 256 lanes (a hash of its
 *   ordering key, or of its event id), so all deliveries of a key share a lane.
 *   Workers lease lanes with one conditional write on a lane document, and a
 *   claim records its whole batch in the lane documents: it writes no delivery
 *   document. While a lane lease is live, other workers do not see the pending
 *   deliveries of that lane, even idle ones. A lane changes hands only when its
 *   lease expired or its owner handed it back.
 * - **Fencing.** A lease token is `<laneId>:<epoch>`; the epoch grows whenever
 *   the lane is taken. Completion, retry, burial, release and renewal apply only
 *   while the lane has that epoch and lists the delivery as in flight.
 * - **Effectively-once for transactional handlers.** A worker that takes a lane
 *   over first writes every delivery the previous owner had in flight (it
 *   charges the lost attempt) and only then reads what to claim. A completion
 *   deletes the delivery in a transaction that first checks the lane. Both write
 *   the same delivery document, so a completion and a takeover cannot both
 *   succeed: the later one either conflicts and retries (and then sees the new
 *   epoch or the missing delivery), or waits for the earlier to commit.
 *   Completions never write the lane document, so concurrent completions of one
 *   lane never conflict with each other.
 * - **Attempts survive crashes.** `attempts` counts attempts that ended. The
 *   claim result reports `attempts + 1`; a failure records it; a lane taken over
 *   charges one attempt to every delivery the previous owner still had in flight,
 *   so a poison message still reaches the retry limit after crashes.
 *
 * - **Purge forgets completed handlers.** An event is deleted by `purge` once
 *   nothing references it, and with it the list of handlers it had. A broker
 *   redelivery or publish retry of that event id after the purge is appended as
 *   a new event and runs its handlers again, as in the PostgreSQL store: keep
 *   retention longer than any redelivery window.
 *
 * `transaction` may run `work` more than once (see {@link MongoEventStore.transaction}).
 * Claims of one `leaseOwner` on one store instance must not run concurrently
 * (each relaybus dispatcher claims one batch at a time); different owners may
 * claim concurrently from any number of processes. Lanes are held per store
 * instance, so a process restarted with the same `leaseOwner` takes its
 * predecessor's lanes over only once their leases expired.
 */
export class MongoEventStore implements EventStore<ClientSession> {
  readonly #client: MongoClient;
  readonly #database: Db;
  readonly #events: Collection<EventDocument>;
  readonly #deliveries: Collection<DeliveryDocument>;
  readonly #keySequences: Collection<KeySequenceDocument>;
  readonly #lanes: Collection<LaneDocument>;
  readonly #completions: Collection<CompletedDeliveryDocument>;
  readonly #completionHistory: boolean;
  readonly #transactionOptions: TransactionOptions;
  readonly #purgeBatchSize: number;
  readonly #logger: RelayBusLogger;
  /**
   * Identifies this store instance in the lanes it holds. A process that restarts
   * with the same `leaseOwner` gets a new instance, so it never mistakes the
   * lanes of its dead predecessor for its own: it waits for their leases to
   * expire and takes them over like any other worker.
   */
  readonly #instanceId = createUuidV7();

  constructor(options: MongoEventStoreOptions) {
    const prefix = options.collectionPrefix ?? 'relaybus_';
    assertCollectionPrefix(prefix);
    if (options.database !== undefined) {
      assertDatabaseName(options.database);
    }
    const purgeBatchSize = options.purgeBatchSize ?? DEFAULT_PURGE_BATCH_SIZE;
    if (!Number.isInteger(purgeBatchSize) || purgeBatchSize < 1) {
      throw new ConfigurationError(
        `purgeBatchSize must be a positive integer, received ${String(purgeBatchSize)}`,
      );
    }
    this.#client = options.client;
    const database = options.client.db(options.database);
    this.#database = database;
    // Majority writes make a lease durable; primary reads keep the ordering check fresh.
    const collectionOptions = {
      readPreference: 'primary',
      writeConcern: { w: 'majority' },
    } as const;
    this.#events = database.collection(`${prefix}events`, collectionOptions);
    this.#deliveries = database.collection(`${prefix}deliveries`, collectionOptions);
    this.#keySequences = database.collection(`${prefix}key_sequences`, collectionOptions);
    this.#lanes = database.collection(`${prefix}lanes`, collectionOptions);
    this.#completions = database.collection(`${prefix}completed_deliveries`, collectionOptions);
    this.#completionHistory = options.completionHistory ?? false;
    this.#transactionOptions = options.transactionOptions ?? DEFAULT_TRANSACTION_OPTIONS;
    this.#purgeBatchSize = purgeBatchSize;
    this.#logger = options.logger ?? console;
  }

  /**
   * Verifies the deployment supports transactions, creates the collections and
   * indexes and seeds the lane documents. Upgrades a 0.1.0 database in place
   * first (see {@link MongoEventStore.#upgradeFromVersion010}); the seeded lanes
   * mark the upgrade as done, so later runs skip it. Safe to run repeatedly and
   * from several processes. Stop 0.1.0 workers before the first run.
   *
   * @throws ConfigurationError when the server is a standalone mongod.
   */
  async setup(): Promise<void> {
    const hello = await this.#client.db('admin').command({ hello: 1 });
    const supportsTransactions =
      typeof hello['setName'] === 'string' || hello['msg'] === 'isdbgrid';
    if (!supportsTransactions) {
      throw createTransactionsUnsupportedError(undefined);
    }
    // Created up front: collections created implicitly inside concurrent transactions conflict.
    const collectionNames = [
      this.#events.collectionName,
      this.#deliveries.collectionName,
      this.#keySequences.collectionName,
      this.#lanes.collectionName,
      ...(this.#completionHistory ? [this.#completions.collectionName] : []),
    ];
    for (const name of collectionNames) {
      await this.#createCollectionIfMissing(name);
    }
    if ((await this.#lanes.countDocuments()) < LANE_COUNT) {
      await this.#upgradeFromVersion010();
      await this.#seedLanes();
    }
    await this.#dropObsoleteIndexes();
    await this.#deliveries.createIndexes([
      // Idempotent appends and lookups by event.
      { key: { eventId: 1, handler: 1 }, name: 'event_handler', unique: true },
      // Claim: due work of a lane in due order. Holds ready deliveries only. The handler is
      // last, so a query over many lanes and handlers stays a merge of one scan per lane.
      { key: CLAIM_INDEX_KEY, name: 'claim', partialFilterExpression: { lane: 'ready' } },
      // Ordering: unfinished predecessors of a keyed delivery, parked or not.
      {
        key: { handler: 1, key: 1, sequence: 1 },
        name: 'ordering',
        partialFilterExpression: { dueAt: { $exists: true }, sequence: { $exists: true } },
      },
      // Parked deliveries: the lowest parked delivery of each key, to move it back once it is next.
      {
        key: { handler: 1, key: 1, sequence: 1 },
        name: 'parked',
        partialFilterExpression: { lane: 'parked' },
      },
      // Statistics: oldest due delivery across handlers.
      { key: { dueAt: 1 }, name: 'due', partialFilterExpression: { dueAt: { $exists: true } } },
      // Purge of completed deliveries left by 0.1.0.
      {
        key: { completedAt: 1 },
        name: 'completed',
        partialFilterExpression: { completedAt: { $exists: true } },
      },
      // Listing and status counts.
      { key: { status: 1, createdAt: 1 }, name: 'status_created' },
      // Purge of dead deliveries.
      { key: { status: 1, updatedAt: 1 }, name: 'status_updated' },
      { key: { handler: 1, createdAt: 1 }, name: 'handler_created' },
      { key: { eventType: 1, createdAt: 1 }, name: 'type_created' },
    ]);
    // Purge of events no delivery references any more.
    await this.#events.createIndexes([{ key: { createdAt: 1, _id: 1 }, name: 'created' }]);
    if (this.#completionHistory) {
      await this.#completions.createIndexes([
        { key: { completedAt: 1, _id: 1 }, name: 'completed' },
        { key: { eventId: 1 }, name: 'event' },
        { key: { createdAt: 1, _id: 1 }, name: 'created' },
      ]);
    }
  }

  async #createCollectionIfMissing(name: string): Promise<void> {
    try {
      await this.#database.createCollection(name);
    } catch (error) {
      if (!hasErrorCode(error, NAMESPACE_EXISTS)) {
        throw error;
      }
    }
  }

  /**
   * Converts the documents of a 0.1.0 database; every step is idempotent, so a
   * run that crashed is simply repeated. Deliveries that were `processing`
   * become `pending`, due at once (their attempt stays counted, as a lost lease
   * would count it). The per-delivery lease fields go, every delivery gets its
   * lane, and every event learns which handlers it already has deliveries for
   * and when it was created. Deliveries completed under 0.1.0 stay as
   * `completed` documents until a purge removes them.
   */
  async #upgradeFromVersion010(): Promise<void> {
    // Untyped view: these documents have the 0.1.0 shape until this method converted them.
    const legacy = this.#database.collection(this.#deliveries.collectionName);
    await legacy.updateMany({ status: 'processing' }, [
      { $set: { status: 'pending', lane: 'ready', dueAt: '$availableAt' } },
    ]);
    await legacy.updateMany(
      {
        $or: [
          { leaseOwner: { $exists: true } },
          { leaseToken: { $exists: true } },
          { leaseExpiresAt: { $exists: true } },
        ],
      },
      { $unset: LEASE_FIELDS },
    );
    await inBatches(
      this.#deliveries.find(
        { laneId: { $exists: false } },
        { projection: { eventId: 1, key: 1 }, batchSize: UPGRADE_BATCH_SIZE },
      ),
      async (documents) => {
        await this.#deliveries.bulkWrite(
          documents.map((document) => ({
            updateOne: {
              filter: { _id: document._id },
              update: { $set: { laneId: laneOf(document.key ?? null, document.eventId) } },
            },
          })),
          { ordered: false },
        );
      },
    );
    await inBatches(
      this.#events.find(
        { handlers: { $exists: false } },
        { projection: { occurredAt: 1 }, batchSize: UPGRADE_BATCH_SIZE },
      ),
      async (events) => {
        const known = new Map(
          (
            await this.#deliveries
              .aggregate<{ _id: string; handlers: string[]; createdAt: Date }>([
                { $match: { eventId: { $in: events.map((event) => event._id) } } },
                {
                  $group: {
                    _id: '$eventId',
                    handlers: { $addToSet: '$handler' },
                    createdAt: { $min: '$createdAt' },
                  },
                },
              ])
              .toArray()
          ).map((group) => [group._id, group]),
        );
        await this.#events.bulkWrite(
          events.map((event) => {
            const group = known.get(event._id);
            return {
              updateOne: {
                filter: { _id: event._id },
                update: {
                  $set:
                    group === undefined
                      ? { handlers: [], createdAt: event.occurredAt }
                      : { handlers: group.handlers, createdAt: group.createdAt },
                },
              },
            };
          }),
          { ordered: false },
        );
      },
    );
  }

  /** Creates the lane documents that do not exist yet. */
  async #seedLanes(): Promise<void> {
    await this.#lanes.bulkWrite(
      Array.from({ length: LANE_COUNT }, (_, laneId) => ({
        updateOne: {
          filter: { _id: laneId },
          update: {
            $setOnInsert: {
              epoch: 0,
              owner: null,
              holder: null,
              leaseExpiresAt: null,
              inFlight: [],
              lost: [],
            },
          },
          upsert: true,
        },
      })),
      { ordered: false },
    );
  }

  /** Drops the 0.1.0 indexes that no query uses, and a `claim` index with the 0.1.0 key. */
  async #dropObsoleteIndexes(): Promise<void> {
    for (const index of await this.#deliveries.indexes()) {
      const obsolete =
        OBSOLETE_INDEXES.includes(index.name!) ||
        (index.name === 'claim' &&
          Object.keys(index.key).join() !== Object.keys(CLAIM_INDEX_KEY).join());
      if (obsolete) {
        try {
          await this.#deliveries.dropIndex(index.name!);
        } catch (error) {
          // Another process running setup() dropped it first.
          if (!hasErrorCode(error, INDEX_NOT_FOUND)) {
            throw error;
          }
        }
      }
    }
  }

  /**
   * Runs `work` in a transaction on a new session and commits when it resolves.
   *
   * `work` can run more than once. The driver restarts the whole callback after
   * a transient failure such as a write conflict, and re-sends only the commit
   * after an unknown commit result. A restart is safe for everything written
   * through the session, because the aborted attempt left nothing behind. It is
   * not safe for side effects outside the session (HTTP calls, other databases),
   * so keep those out of `work`; relaybus handlers are already at-least-once.
   *
   * @throws ConfigurationError when the deployment does not support transactions.
   */
  async transaction<T>(work: (transaction: ClientSession) => Promise<T>): Promise<T> {
    try {
      return await this.#client.withSession(async (session) => {
        let result!: T;
        await session.withTransaction(async () => {
          result = await work(session);
        }, this.#transactionOptions);
        return result;
      });
    } catch (error) {
      if (isTransactionsUnsupportedError(error)) {
        throw createTransactionsUnsupportedError(error);
      }
      throw error;
    }
  }

  /**
   * Writes the entries in `session`. An existing event gets a delivery only for
   * handlers it does not list yet; the added handlers are appended to its list
   * together with a `revision` increment, a real write that makes a concurrent
   * purge of the event conflict with this transaction.
   */
  async append(
    session: ClientSession,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    assertInTransaction(session, 'append');
    const eventIds = [...new Set(entries.map((entry) => entry.event.id))];
    const storedEvents = new Map(
      (
        await this.#events
          .find(
            { _id: { $in: eventIds } },
            { session, projection: { key: 1, sequence: 1, handlers: 1 } },
          )
          .toArray()
      ).map((stored) => [stored._id, stored]),
    );

    const appendedEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    const newEvents: Array<{ readonly event: EventEnvelope; readonly sequence: number | null }> =
      [];
    const newDeliveries: DeliveryDocument[] = [];
    /** Handlers every event of the call has, or gets in this call. */
    const knownHandlers = new Map<string, Set<string>>();
    /** Handlers this call adds, per event. */
    const addedHandlers = new Map<string, string[]>();
    const orderingByEventId = new Map<
      string,
      { readonly key: string | null; readonly sequence: number | null }
    >();
    for (const [eventId, stored] of storedEvents) {
      orderingByEventId.set(eventId, { key: stored.key, sequence: stored.sequence });
      knownHandlers.set(eventId, new Set(stored.handlers));
    }

    for (const { event, deliveries } of entries) {
      let ordering = orderingByEventId.get(event.id);
      if (ordering === undefined) {
        const sequence = event.key === null ? null : await this.#nextSequence(event.key, session);
        ordering = { key: event.key, sequence };
        orderingByEventId.set(event.id, ordering);
        knownHandlers.set(event.id, new Set());
        newEvents.push({ event, sequence });
        appendedEventIds.push(event.id);
      } else {
        duplicateEventIds.push(event.id);
      }
      const handlers = knownHandlers.get(event.id)!;
      for (const delivery of deliveries) {
        if (handlers.has(delivery.handler)) {
          continue;
        }
        handlers.add(delivery.handler);
        addedHandlers.set(event.id, [...(addedHandlers.get(event.id) ?? []), delivery.handler]);
        newDeliveries.push(toDeliveryDocument(delivery, event, ordering, now));
      }
    }

    if (newEvents.length > 0) {
      // Upserts, not inserts: a duplicate-key error would abort the whole transaction,
      // whereas a concurrent writer of the same id surfaces as a retryable write conflict.
      await this.#events.bulkWrite(
        newEvents.map(({ event, sequence }) => {
          const document = toEventDocument(event, sequence, addedHandlers.get(event.id) ?? [], now);
          return {
            updateOne: {
              filter: { _id: document._id },
              update: { $setOnInsert: document },
              upsert: true,
            },
          };
        }),
        { session, ordered: true },
      );
    }
    const extendedEvents = [...addedHandlers].filter(([eventId]) => storedEvents.has(eventId));
    if (extendedEvents.length > 0) {
      await this.#events.bulkWrite(
        extendedEvents.map(([eventId, handlers]) => ({
          updateOne: {
            filter: { _id: eventId },
            update: { $inc: { revision: 1 }, $push: { handlers: { $each: handlers } } },
          },
        })),
        { session, ordered: true },
      );
    }
    if (newDeliveries.length > 0) {
      await this.#deliveries.bulkWrite(
        newDeliveries.map((document) => ({
          updateOne: {
            filter: { eventId: document.eventId, handler: document.handler },
            update: { $setOnInsert: document },
            upsert: true,
          },
        })),
        { session, ordered: true },
      );
    }
    return { appendedEventIds, duplicateEventIds, appendedDeliveryCount: newDeliveries.length };
  }

  /**
   * Claims up to `limit` due deliveries. It claims from the lanes this owner
   * (and this store instance) holds under a live lease first, then takes free
   * lanes with due work, a few at a time (4, 8, 16, ...) and the lane with the
   * oldest due delivery first, so a worker holds only as many lanes as it
   * needs. It then records the batch with one write per lane and hands back the
   * lanes that yielded nothing and run nothing. No delivery document is
   * written, except for the attempt charged to deliveries of a lane taken over
   * and for parking (see {@link MongoEventStore.#parkDeliveries}).
   *
   * Taking a lane is one conditional `findOneAndUpdate` that also moves the
   * previous owner's in-flight list to the lane's `lost` list; a lane another
   * claimer took first is skipped, never waited for. Lost deliveries are charged
   * one attempt before anything is read from the lane, and `lost` is cleared
   * only after that. Whoever holds the lane next charges a `lost` list it finds
   * first, so a claim that fails anywhere after the take loses no charge (a
   * failure between charging and clearing charges those deliveries once more).
   * Nothing a failed claim did leaves a delivery listed as running.
   */
  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || request.limit <= 0) {
      return [];
    }
    await this.#unparkNextDeliveries(request.handlers);
    const held = await this.#lanes
      .find({
        owner: request.leaseOwner,
        holder: this.#instanceId,
        leaseExpiresAt: { $gt: request.now },
      })
      .toArray();
    await this.#chargeLost(held);
    const scope = new Map<number, ScopeLane>(
      held.map((lane) => [
        lane._id,
        { laneId: lane._id, epoch: lane.epoch, listed: lane.inFlight },
      ]),
    );
    const running = await this.#findRunning(held.flatMap((lane) => lane.inFlight));
    const runningIds = new Set(running.map((document) => document._id));
    const runningKeys = new Set(
      running.flatMap((document) =>
        document.key === undefined ? [] : [orderingIdentity(document.handler, document.key)],
      ),
    );
    const scan = new ClaimScan();
    const picked: DeliveryDocument[] = [];
    const pickFrom = async (lanes: readonly ScopeLane[]): Promise<void> => {
      for (let offset = 0; offset < lanes.length; offset += LANES_PER_CANDIDATE_QUERY) {
        if (picked.length < request.limit) {
          await this.#pickCandidates(
            lanes.slice(offset, offset + LANES_PER_CANDIDATE_QUERY),
            request,
            scan,
            runningKeys,
            picked,
          );
        }
      }
    };
    await pickFrom([...scope.values()]);
    const passed = new Set(scope.keys());
    for (let laneCount = INITIAL_LANES_PER_ROUND; picked.length < request.limit; laneCount *= 2) {
      const free = await this.#findFreeLanes(request, laneCount, [...passed]);
      if (free.length === 0) {
        break;
      }
      for (const laneId of free) {
        passed.add(laneId);
      }
      const taken = await this.#takeLanes(free, request);
      for (const lane of taken) {
        scope.set(lane.laneId, lane);
      }
      await pickFrom(taken);
    }
    // Read before recording: a failure from here on must not leave picks listed as running.
    const events = await this.#readEnvelopes(picked.map((document) => document.eventId));
    const recorded = await this.#recordClaim(scope, picked, runningIds, request);
    let claimed = picked.filter((document) => recorded.has(document.laneId));
    try {
      claimed = await this.#releaseOvertaken(claimed, scope);
    } catch (error) {
      // Best effort: whatever stays listed is charged and handed out again when the lease expires.
      await Promise.allSettled([this.#releasePicks(claimed, scope)]);
      throw error;
    }
    await this.#parkWaiting(scan.waitingBacklogs(PARK_BACKLOG_THRESHOLD));
    return claimed.map((document) => ({
      deliveryId: document._id,
      leaseToken: formatLeaseToken(document.laneId, scope.get(document.laneId)!.epoch),
      handler: document.handler,
      attempt: document.attempts + 1,
      leaseExpiresAt: request.leaseExpiresAt,
      event: events.get(document.eventId)!,
    }));
  }

  /**
   * Extends the lease of the lane a running delivery belongs to. The lane lease
   * never shrinks, so a repeated renewal with the same value still holds. An
   * expired lease nobody took over is still renewed, so `_now` is not needed.
   */
  async extendLease(lease: LeaseReference, leaseExpiresAt: Date, _now?: Date): Promise<boolean> {
    const held = parseLeaseToken(lease.leaseToken);
    if (held === undefined) {
      return false;
    }
    const deliveryId = String(lease.deliveryId);
    const [renewed, running] = await Promise.all([
      this.#lanes.updateOne(
        { _id: held.laneId, epoch: held.epoch, inFlight: deliveryId },
        { $max: { leaseExpiresAt } },
      ),
      this.#deliveries.findOne({ _id: deliveryId, status: 'pending' }, { projection: { _id: 1 } }),
    ]);
    return renewed.matchedCount === 1 && running !== null;
  }

  /**
   * Deletes the delivery (and records it in the completion history, when kept).
   * With `session` the deletion commits with the caller's transaction; without,
   * it runs in a transaction of its own. Returns false when the lease was lost.
   */
  async complete(lease: LeaseReference, now: Date, session?: ClientSession): Promise<boolean> {
    if (session !== undefined) {
      assertInTransaction(session, 'complete');
    }
    return (await this.#completeLeases([lease], now, session))[0]!;
  }

  /**
   * Completes many leases in one transaction, whatever lanes they belong to.
   * Returns one flag per lease in input order; a lease listed twice completes
   * once, and a token this store cannot have issued is refused.
   */
  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completeLeases(leases, now, undefined);
  }

  /** Returns a running delivery to pending, due at `availableAt`, and counts the attempt that ended. */
  async reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { availableAt, dueAt: availableAt, lastError: toErrorInfo(error), updatedAt: now },
      $inc: { attempts: 1 },
    });
  }

  /** Gives a running delivery back without counting the attempt: pending, due `now`. */
  async release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { availableAt: now, dueAt: now, updatedAt: now },
    });
  }

  /** Moves a running delivery to dead and counts the attempt that ended. */
  async bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { status: 'dead', lastError: toErrorInfo(error), updatedAt: now },
      $unset: FINISHED_FIELDS,
      $inc: { attempts: 1 },
    });
  }

  /**
   * Frees every lane `leaseOwner` holds that runs no delivery, so other workers
   * need not wait for the lease to expire. Lanes with running deliveries stay leased.
   */
  async relinquish(leaseOwner: string): Promise<void> {
    const held = await this.#lanes
      .find({ owner: String(leaseOwner), holder: this.#instanceId })
      .toArray();
    const runningIds = new Set(
      (await this.#findRunning(held.flatMap((lane) => lane.inFlight))).map(
        (document) => document._id,
      ),
    );
    const idle = held.filter((lane) => lane.inFlight.every((id) => !runningIds.has(id)));
    if (idle.length > 0) {
      await this.#lanes.bulkWrite(
        idle.map((lane) => freeLane(lane._id, lane.epoch, lane.inFlight)),
        { ordered: false },
      );
    }
  }

  /**
   * Lists deliveries in creation order. A delivery is `processing` while its
   * document exists as `pending` and its lane lists it; its attempt count then
   * includes the running attempt, and owner and expiry are those of the lane
   * lease. Completion history records (when kept) are listed as `completed`.
   */
  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    // Strings only: an object here (say, parsed from an admin request) would be a query operator.
    const status = query.status === undefined ? undefined : String(query.status);
    const common = {
      ...(query.handler === undefined ? {} : { handler: String(query.handler) }),
      ...(query.eventType === undefined ? {} : { eventType: String(query.eventType) }),
      ...(query.eventId === undefined ? {} : { eventId: String(query.eventId) }),
    };
    const limit = query.limit ?? DEFAULT_LIST_LIMIT;
    const lanes = await this.#lanes
      .find({ 'inFlight.0': { $exists: true } }, { projection: { epoch: 0 } })
      .toArray();
    const listed = lanes.flatMap((lane) => lane.inFlight);
    // A Map, not an object: an untrusted status such as 'constructor' must not find anything.
    const derivedStatuses = new Map<string, Filter<DeliveryDocument>>([
      ['processing', { status: 'pending', _id: { $in: listed } }],
      ['pending', { status: 'pending', _id: { $nin: listed } }],
    ]);
    const filter: Filter<DeliveryDocument> = {
      ...common,
      ...(status === undefined
        ? {}
        : (derivedStatuses.get(status) ?? { status: status as DeliveryDocument['status'] })),
    };
    const sort = { createdAt: 1, _id: 1 } as const;
    const documents = await this.#deliveries.find(filter, { sort, limit }).toArray();
    const history =
      this.#completionHistory && (status === undefined || status === 'completed')
        ? await this.#completions.find(common, { sort, limit }).toArray()
        : [];
    const events = await this.#readEnvelopes([
      ...documents.map((document) => document.eventId),
      ...history.map((document) => document.eventId),
    ]);
    const laneById = new Map(lanes.map((lane) => [lane._id, lane]));
    const records = [
      ...documents.map((document) => {
        const lane = laneById.get(document.laneId);
        const runs =
          document.status === 'pending' && lane?.inFlight.includes(document._id) === true;
        return toDeliveryRecord(document, events.get(document.eventId)!, runs ? lane : undefined);
      }),
      ...history.map((document) =>
        toCompletedDeliveryRecord(document, events.get(document.eventId)!),
      ),
    ];
    return records
      .sort(
        (left, right) =>
          left.createdAt.getTime() - right.createdAt.getTime() || compareText(left.id, right.id),
      )
      .slice(0, limit);
  }

  /** Moves dead deliveries back to pending with a fresh attempt budget. */
  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    const result = await this.#deliveries.updateMany(
      { _id: { $in: deliveryIds.map(String) }, status: 'dead' },
      {
        $set: {
          status: 'pending',
          lane: 'ready',
          attempts: 0,
          availableAt: now,
          dueAt: now,
          updatedAt: now,
        },
      },
    );
    return result.modifiedCount;
  }

  /**
   * Deletes, in batches of `purgeBatchSize`, dead deliveries last updated before
   * `deadBefore`, and completion history records (and `completed` documents left
   * by 0.1.0) completed before `completedBefore`, each batch in one transaction
   * together with the events it left without any reference. Finally, events
   * created before the earliest given cutoff that nothing references are
   * deleted: what a completed delivery leaves behind, or an event no handler
   * was subscribed to.
   */
  async purge(request: PurgeRequest): Promise<PurgeResult> {
    const total = { deletedDeliveries: 0, deletedEvents: 0 };
    const steps: Array<() => Promise<PurgeResult>> = [];
    const { completedBefore, deadBefore } = request;
    if (completedBefore !== undefined) {
      steps.push(() =>
        this.#purgeMatching(this.#purgeable(this.#deliveries.collectionName), {
          status: 'completed',
          completedAt: { $lt: completedBefore },
        }),
      );
      if (this.#completionHistory) {
        steps.push(() =>
          this.#purgeMatching(this.#purgeable(this.#completions.collectionName), {
            completedAt: { $lt: completedBefore },
          }),
        );
      }
    }
    if (deadBefore !== undefined) {
      steps.push(() =>
        this.#purgeMatching(this.#purgeable(this.#deliveries.collectionName), {
          status: 'dead',
          updatedAt: { $lt: deadBefore },
        }),
      );
    }
    for (const step of steps) {
      const result = await step();
      total.deletedDeliveries += result.deletedDeliveries;
      total.deletedEvents += result.deletedEvents;
    }
    const orphanCutoff = [completedBefore, deadBefore]
      .filter((cutoff) => cutoff !== undefined)
      .sort((first, second) => first.getTime() - second.getTime())[0];
    if (orphanCutoff !== undefined) {
      total.deletedEvents += await this.#purgeOrphanEvents(orphanCutoff);
    }
    return total;
  }

  /**
   * Counts by derived status. `pending` excludes running deliveries; `completed`
   * counts history records and 0.1.0 `completed` documents. `oldestDueAt` is the
   * oldest instant at which a delivery became claimable: the due time of a
   * pending delivery nobody runs, or the lease expiry of a lane that still runs
   * deliveries.
   */
  async statistics(now: Date): Promise<EventStoreStatistics> {
    const lanes = await this.#lanes
      .find({ 'inFlight.0': { $exists: true } }, { projection: { inFlight: 1, leaseExpiresAt: 1 } })
      .toArray();
    // Index-only counts in parallel: measured at 300k deliveries they take 27 ms, against
    // 121 ms for one $group, which has to read every document or index entry.
    const [stored, dead, legacyCompleted, history, running] = await Promise.all([
      this.#deliveries.countDocuments({ status: 'pending' }),
      this.#deliveries.countDocuments({ status: 'dead' }),
      this.#deliveries.countDocuments({ status: 'completed' }),
      this.#completionHistory ? this.#completions.countDocuments() : Promise.resolve(0),
      this.#findRunning(lanes.flatMap((lane) => lane.inFlight)),
    ]);
    const runningIds = new Set(running.map((document) => document._id));
    const waiting = await this.#deliveries.findOne(
      { dueAt: { $lte: now }, _id: { $nin: [...runningIds] } },
      { sort: { dueAt: 1 }, projection: { dueAt: 1 } },
    );
    const dueTimes = lanes
      .filter(
        (lane) =>
          lane.leaseExpiresAt !== null &&
          lane.leaseExpiresAt <= now &&
          lane.inFlight.some((id) => runningIds.has(id)),
      )
      .map((lane) => lane.leaseExpiresAt!.getTime());
    if (waiting !== null) {
      dueTimes.push(waiting.dueAt!.getTime());
    }
    return {
      // The counts are separate reads; a completion between them must not make this negative.
      pending: Math.max(0, stored - running.length),
      processing: running.length,
      completed: legacyCompleted + history,
      dead,
      oldestDueAt: dueTimes.length === 0 ? null : new Date(Math.min(...dueTimes)),
    };
  }

  /** Atomically increments the counter of `key`; conflicting appends of the key serialize here. */
  async #nextSequence(key: string, session: ClientSession): Promise<number> {
    const counter = await this.#keySequences.findOneAndUpdate(
      { _id: key },
      { $inc: { value: 1 } },
      { upsert: true, returnDocument: 'after', session },
    );
    return counter!.value;
  }

  /** The listed deliveries that run: those whose document still exists as pending. */
  async #findRunning(listed: readonly string[]): Promise<DeliveryDocument[]> {
    if (listed.length === 0) {
      return [];
    }
    return this.#deliveries
      .find(
        { _id: { $in: [...listed] }, status: 'pending' },
        { projection: { handler: 1, key: 1, laneId: 1 } },
      )
      .toArray();
  }

  /**
   * Up to `laneCount` lanes nobody holds under a live lease that have work for
   * the requested handlers, leaving out `passed`, the lane with the oldest due
   * delivery first. One round trip: the server probes each lane's first
   * {@link PARK_BACKLOG_THRESHOLD} due, ready deliveries in due order. A lane
   * whose probed deliveries are all keyed followers waiting behind an
   * unfinished predecessor has nothing to run and is left alone, so it is not
   * taken and handed back by every claim; when all of the probed ones wait, the
   * lane is taken anyway, because its backlog is long enough to be parked.
   */
  async #findFreeLanes(
    request: ClaimRequest,
    laneCount: number,
    passed: readonly number[],
  ): Promise<number[]> {
    const lanes = await this.#lanes
      .aggregate<{ _id: number }>([
        {
          $match: {
            _id: { $nin: passed },
            $or: [{ leaseExpiresAt: null }, { leaseExpiresAt: { $lte: request.now } }],
          },
        },
        {
          $lookup: {
            from: this.#deliveries.collectionName,
            localField: '_id',
            foreignField: 'laneId',
            pipeline: [
              {
                $match: {
                  handler: { $in: [...request.handlers] },
                  lane: 'ready',
                  dueAt: { $lte: request.now },
                },
              },
              { $sort: { dueAt: 1, _id: 1 } },
              { $limit: PARK_BACKLOG_THRESHOLD },
              {
                $lookup: {
                  from: this.#deliveries.collectionName,
                  let: { handler: '$handler', key: '$key', sequence: '$sequence' },
                  pipeline: [
                    {
                      $match: {
                        dueAt: { $exists: true },
                        sequence: { $exists: true },
                        $expr: {
                          $and: [
                            { $eq: ['$handler', '$$handler'] },
                            { $eq: ['$key', '$$key'] },
                            { $lt: ['$sequence', '$$sequence'] },
                          ],
                        },
                      },
                    },
                    { $limit: 1 },
                    { $project: { _id: 1 } },
                  ],
                  as: 'blockers',
                },
              },
              { $project: { dueAt: 1, waits: { $gt: [{ $size: '$blockers' }, 0] } } },
            ],
            as: 'due',
          },
        },
        {
          $set: {
            first: {
              $let: {
                vars: { open: { $filter: { input: '$due', cond: { $not: ['$$this.waits'] } } } },
                in: {
                  $cond: [
                    { $gt: [{ $size: '$$open' }, 0] },
                    { $first: '$$open' },
                    {
                      $cond: [
                        { $eq: [{ $size: '$due' }, PARK_BACKLOG_THRESHOLD] },
                        { $first: '$due' },
                        null,
                      ],
                    },
                  ],
                },
              },
            },
          },
        },
        { $match: { first: { $ne: null } } },
        { $sort: { 'first.dueAt': 1, 'first._id': 1 } },
        { $limit: laneCount },
        { $project: { _id: 1 } },
      ])
      .toArray();
    return lanes.map((lane) => lane._id);
  }

  /**
   * Takes the lanes with one conditional write each, run in parallel; a lane
   * taken by someone else meanwhile is skipped. The same write moves the
   * previous owner's in-flight list to `lost`, which is charged before any
   * delivery of the lanes is read (see {@link MongoEventStore.#chargeLost}).
   */
  async #takeLanes(laneIds: readonly number[], request: ClaimRequest): Promise<ScopeLane[]> {
    const after = await Promise.all(
      laneIds.map((laneId) =>
        this.#lanes.findOneAndUpdate(
          {
            _id: laneId,
            $or: [{ leaseExpiresAt: null }, { leaseExpiresAt: { $lte: request.now } }],
          },
          [
            {
              $set: {
                epoch: { $add: ['$epoch', 1] },
                owner: { $literal: request.leaseOwner },
                holder: { $literal: this.#instanceId },
                leaseExpiresAt: { $literal: request.leaseExpiresAt },
                lost: { $setUnion: [{ $ifNull: ['$lost', []] }, '$inFlight'] },
                inFlight: { $literal: [] },
              },
            },
          ],
          { returnDocument: 'after' },
        ),
      ),
    );
    const taken = after.filter((lane) => lane !== null);
    await this.#chargeLost(taken);
    return taken.map((lane) => ({ laneId: lane._id, epoch: lane.epoch, listed: lane.inFlight }));
  }

  /**
   * Charges one attempt to every pending delivery listed in the `lost` lists of
   * `lanes` (deliveries whose lane was taken while they ran), then clears those
   * lists under the lanes' current epochs. The charge is a write on each lost
   * delivery that comes before any read of the lane's deliveries: a completion
   * of the previous owner deletes the same documents, so the two conflict and
   * cannot both succeed. A failure before the clear leaves the list in place for
   * the next holder, which charges it again.
   */
  async #chargeLost(lanes: readonly LaneDocument[]): Promise<void> {
    const losing = lanes.filter((lane) => lane.lost.length > 0);
    if (losing.length === 0) {
      return;
    }
    await this.#deliveries.updateMany(
      { _id: { $in: losing.flatMap((lane) => lane.lost) }, status: 'pending' },
      { $inc: { attempts: 1 } },
    );
    await this.#lanes.bulkWrite(
      losing.map((lane) => ({
        updateOne: { filter: { _id: lane._id, epoch: lane.epoch }, update: { $set: { lost: [] } } },
      })),
      { ordered: false },
    );
  }

  /**
   * Adds the claimable candidates of `lanes` to `picked`, in due order, until
   * `request.limit` is reached. A delivery is a candidate when it is ready, due
   * and not in flight in its lane. A keyed candidate is claimable only when
   * (a) no earlier delivery of its handler and key is pending (a running one is
   * still a pending document) and (b) no other delivery of them runs. Running
   * deliveries of a key live in the key's lane, which this owner holds, so
   * `runningKeys` (loaded once from the held lanes) and this claim's own picks
   * are all of them.
   */
  async #pickCandidates(
    lanes: readonly ScopeLane[],
    request: ClaimRequest,
    scan: ClaimScan,
    runningKeys: ReadonlySet<string>,
    picked: DeliveryDocument[],
  ): Promise<void> {
    const candidates = this.#deliveries.find(
      {
        laneId: { $in: lanes.map((lane) => lane.laneId) },
        handler: { $in: [...request.handlers] },
        lane: 'ready',
        dueAt: { $lte: request.now },
        _id: { $nin: lanes.flatMap((lane) => lane.listed) },
      },
      {
        sort: { dueAt: 1, _id: 1 },
        batchSize: Math.max(request.limit * 2, MINIMUM_CANDIDATE_BATCH_SIZE),
      },
    );
    try {
      for await (const candidate of candidates) {
        if (await this.#isClaimable(candidate, scan, runningKeys)) {
          picked.push(candidate);
          if (picked.length >= request.limit) {
            break;
          }
        }
      }
    } finally {
      await candidates.close();
    }
  }

  async #isClaimable(
    candidate: DeliveryDocument,
    scan: ClaimScan,
    runningKeys: ReadonlySet<string>,
  ): Promise<boolean> {
    const { key, sequence } = candidate;
    if (key === undefined) {
      return true;
    }
    const orderingId = orderingIdentity(candidate.handler, key);
    const known = scan.verdict(orderingId, sequence!);
    if (known === 'open-predecessor') {
      scan.noteWaiting(orderingId, candidate._id);
      return false;
    }
    if (known === 'running') {
      return false;
    }
    const predecessor = await this.#findUnfinishedPredecessor(candidate.handler, key, sequence!);
    if (predecessor !== undefined) {
      scan.noteOpen(orderingId, predecessor);
      scan.noteWaiting(orderingId, candidate._id);
      return false;
    }
    if (runningKeys.has(orderingId)) {
      scan.noteRunning(orderingId);
      return false;
    }
    scan.noteClaimed(orderingId, sequence!);
    return true;
  }

  /**
   * Records the picks in their lanes, one conditional write per lane and one
   * round trip for all of them, and frees the lanes that yielded nothing and run
   * nothing. A picked lane lists its running deliveries plus the new ones (ids
   * whose documents are gone are pruned) and its lease reaches
   * `request.leaseExpiresAt`. Returns the lanes whose picks were recorded: every
   * write is conditional on the claim's epoch, owner and store instance, so a
   * lane that changed hands, or was handed back by `relinquish`, in the meantime
   * keeps its deliveries for whoever takes it next.
   */
  async #recordClaim(
    scope: ReadonlyMap<number, ScopeLane>,
    picked: readonly DeliveryDocument[],
    runningIds: ReadonlySet<string>,
    request: ClaimRequest,
  ): Promise<Set<number>> {
    const picksByLane = new Map<number, string[]>();
    for (const document of picked) {
      picksByLane.set(document.laneId, [...(picksByLane.get(document.laneId) ?? []), document._id]);
    }
    const operations: Array<AnyBulkWriteOperation<LaneDocument>> = [];
    for (const lane of scope.values()) {
      const gone = lane.listed.filter((id) => !runningIds.has(id));
      const ids = picksByLane.get(lane.laneId);
      if (ids !== undefined) {
        operations.push({
          updateOne: {
            filter: {
              _id: lane.laneId,
              epoch: lane.epoch,
              owner: request.leaseOwner,
              holder: this.#instanceId,
            },
            update: [
              {
                $set: {
                  inFlight: {
                    $concatArrays: [
                      {
                        $filter: {
                          input: '$inFlight',
                          cond: { $not: [{ $in: ['$$this', { $literal: gone }] }] },
                        },
                      },
                      { $literal: ids },
                    ],
                  },
                  leaseExpiresAt: { $max: ['$leaseExpiresAt', request.leaseExpiresAt] },
                },
              },
            ],
          },
        });
      } else if (gone.length === lane.listed.length) {
        operations.push(freeLane(lane.laneId, lane.epoch, gone));
      }
    }
    if (operations.length === 0) {
      return new Set();
    }
    const result = await this.#lanes.bulkWrite(operations, { ordered: false });
    if (result.matchedCount === operations.length) {
      return new Set(picksByLane.keys());
    }
    const current = await this.#lanes
      .find(
        { _id: { $in: [...picksByLane.keys()] } },
        { projection: { epoch: 1, owner: 1, holder: 1 } },
      )
      .toArray();
    return new Set(
      current
        .filter(
          (lane) =>
            lane.epoch === scope.get(lane._id)!.epoch &&
            lane.owner === request.leaseOwner &&
            lane.holder === this.#instanceId,
        )
        .map((lane) => lane._id),
    );
  }

  /**
   * Gives back the recorded keyed picks that a delivery requeued during the
   * claim now precedes, without charging an attempt, and returns the rest. The
   * claim checked their predecessors before recording them; a dead letter
   * requeued in between would otherwise wait behind its own successor. Once a
   * pick is recorded, a later requeue waits for it like for any running delivery.
   */
  async #releaseOvertaken(
    claimed: readonly DeliveryDocument[],
    scope: ReadonlyMap<number, ScopeLane>,
  ): Promise<DeliveryDocument[]> {
    const overtaken = new Set<string>();
    await Promise.all(
      claimed.map(async (document) => {
        if (
          document.key !== undefined &&
          (await this.#findUnfinishedPredecessor(
            document.handler,
            document.key,
            document.sequence!,
          )) !== undefined
        ) {
          overtaken.add(document._id);
        }
      }),
    );
    if (overtaken.size === 0) {
      return [...claimed];
    }
    await this.#releasePicks(
      claimed.filter((document) => overtaken.has(document._id)),
      scope,
    );
    return claimed.filter((document) => !overtaken.has(document._id));
  }

  /** Takes recorded picks out of their lanes' in-flight lists again, under the claim's epochs. */
  async #releasePicks(
    documents: readonly DeliveryDocument[],
    scope: ReadonlyMap<number, ScopeLane>,
  ): Promise<void> {
    const idsByLane = new Map<number, string[]>();
    for (const document of documents) {
      idsByLane.set(document.laneId, [...(idsByLane.get(document.laneId) ?? []), document._id]);
    }
    await Promise.all(
      [...idsByLane].map(([laneId, ids]) =>
        this.#lanes.updateOne(
          { _id: laneId, epoch: scope.get(laneId)!.epoch },
          { $pull: { inFlight: { $in: ids } } },
        ),
      ),
    );
  }

  /**
   * Parks the waiting backlogs a claim found. Best effort: parking only saves
   * later claims work, so a failure is logged and the claim still returns its
   * batch; the next claim that meets the backlog parks it.
   */
  async #parkWaiting(deliveryIds: readonly string[]): Promise<void> {
    try {
      await this.#parkDeliveries(deliveryIds);
    } catch (error) {
      this.#logger.warn(
        `[MongoEventStore](parkWaiting) Parking waiting deliveries failed, a later claim parks them | count=${deliveryIds.length} | error=${String(error)}`,
      );
    }
  }

  /** Sequence of the lowest unfinished delivery of the handler and key before `sequence`, if any. */
  async #findUnfinishedPredecessor(
    handler: string,
    key: string,
    sequence: number,
  ): Promise<number | undefined> {
    const predecessor = await this.#deliveries.findOne(
      { handler, key, sequence: { $lt: sequence }, dueAt: { $exists: true } },
      { sort: { sequence: 1 }, projection: { sequence: 1 } },
    );
    return predecessor?.sequence;
  }

  /**
   * Parks pending deliveries that waited behind an unfinished predecessor, which
   * removes them from the claim index until their turn. Parking is a hint, not a
   * guarantee: a predecessor that finishes meanwhile is found by the next
   * {@link MongoEventStore.claim}, which moves the lowest parked delivery back.
   */
  async #parkDeliveries(deliveryIds: readonly string[]): Promise<void> {
    for (let offset = 0; offset < deliveryIds.length; offset += PARK_UPDATE_CHUNK_SIZE) {
      await this.#deliveries.updateMany(
        {
          _id: { $in: deliveryIds.slice(offset, offset + PARK_UPDATE_CHUNK_SIZE) },
          status: 'pending',
          lane: 'ready',
        },
        { $set: { lane: 'parked' } },
      );
    }
  }

  /**
   * Moves the lowest parked delivery of every parked key of `handlers` back into
   * the claim index when nothing unfinished precedes it. Level-triggered on
   * purpose: it looks at the current state only, so a crash anywhere, or a
   * predecessor finishing while its followers were being parked, can delay a key
   * until the next claim but never strand it.
   */
  async #unparkNextDeliveries(handlers: readonly string[]): Promise<void> {
    const anyParked = await this.#deliveries.findOne(
      { handler: { $in: [...handlers] }, lane: 'parked' },
      // The sort lets the planner pick the small partial index over the handler's whole history.
      { sort: { handler: 1, key: 1, sequence: 1 }, projection: { _id: 1 } },
    );
    if (anyParked === null) {
      return;
    }
    for (const handler of new Set(handlers)) {
      let afterKey: string | undefined;
      for (;;) {
        const lowest = await this.#deliveries.findOne(
          {
            handler,
            lane: 'parked',
            ...(afterKey === undefined ? {} : { key: { $gt: afterKey } }),
          },
          { sort: { key: 1, sequence: 1 }, projection: { key: 1, sequence: 1 } },
        );
        if (lowest === null) {
          break;
        }
        afterKey = lowest.key!;
        if (
          (await this.#findUnfinishedPredecessor(handler, afterKey, lowest.sequence!)) === undefined
        ) {
          await this.#deliveries.updateOne(
            { _id: lowest._id, lane: 'parked' },
            { $set: { lane: 'ready' } },
          );
        }
      }
    }
  }

  async #readEnvelopes(eventIds: readonly string[]): Promise<Map<string, EventEnvelope>> {
    const documents = await this.#events.find({ _id: { $in: [...new Set(eventIds)] } }).toArray();
    return new Map(documents.map((document) => [document._id, toEnvelope(document)]));
  }

  /**
   * Deletes the delivery of every lease whose lane still has the lease's epoch
   * and lists the delivery, in one transaction (the caller's, when given).
   * Returns one flag per lease, in order; a lease listed twice completes once.
   */
  async #completeLeases(
    leases: readonly LeaseReference[],
    now: Date,
    session: ClientSession | undefined,
  ): Promise<boolean[]> {
    const parsed = leases.map((lease) => parseLeaseToken(lease.leaseToken));
    const issued = leases.flatMap((lease, index) =>
      parsed[index] === undefined
        ? []
        : [{ deliveryId: String(lease.deliveryId), ...parsed[index] }],
    );
    let completed = new Set<string>();
    if (issued.length > 0) {
      completed =
        session === undefined
          ? await this.transaction((own) => this.#deleteCompleted(issued, now, own))
          : await this.#deleteCompleted(issued, now, session);
    }
    return leases.map(
      (lease, index) =>
        parsed[index] !== undefined &&
        completed.delete(leaseIdentity({ deliveryId: String(lease.deliveryId), ...parsed[index] })),
    );
  }

  /**
   * The transactional part of a completion. The lanes are only read: the
   * conflict with a takeover comes from the deleted delivery documents, which a
   * takeover writes before it reads them (see {@link MongoEventStore.#takeLanes}).
   * So whichever of the two commits second either fails with a write conflict
   * and is retried against the new state, or waits for the first.
   */
  async #deleteCompleted(
    issued: readonly IssuedLease[],
    now: Date,
    session: ClientSession,
  ): Promise<Set<string>> {
    const lanes = new Map(
      (
        await this.#lanes
          .find(
            { _id: { $in: [...new Set(issued.map((lease) => lease.laneId))] } },
            { session, projection: { epoch: 1, inFlight: 1 } },
          )
          .toArray()
      ).map((lane) => [lane._id, lane]),
    );
    const fenced = issued.filter((lease) => {
      const lane = lanes.get(lease.laneId)!;
      return lane.epoch === lease.epoch && lane.inFlight.includes(lease.deliveryId);
    });
    if (fenced.length === 0) {
      return new Set();
    }
    const documents = await this.#deliveries
      .find(
        { _id: { $in: [...new Set(fenced.map((lease) => lease.deliveryId))] }, status: 'pending' },
        { session },
      )
      .toArray();
    if (documents.length > 0) {
      await this.#deliveries.deleteMany(
        { _id: { $in: documents.map((document) => document._id) }, status: 'pending' },
        { session },
      );
      if (this.#completionHistory) {
        await this.#completions.insertMany(
          documents.map((document) => toCompletedDeliveryDocument(document, now)),
          { session },
        );
      }
    }
    const deleted = new Set(documents.map((document) => document._id));
    return new Set(
      fenced.filter((lease) => deleted.has(lease.deliveryId)).map((lease) => leaseIdentity(lease)),
    );
  }

  /**
   * Applies `update` to a running delivery and takes it out of its lane's
   * in-flight list, if the lane still has the lease's epoch. One transaction:
   * the lane write fences the change against a takeover.
   */
  async #leaveFlight(
    lease: LeaseReference,
    update: UpdateFilter<DeliveryDocument>,
  ): Promise<boolean> {
    const held = parseLeaseToken(lease.leaseToken);
    if (held === undefined) {
      return false;
    }
    const deliveryId = String(lease.deliveryId);
    return this.transaction(async (session) => {
      const fence = await this.#lanes.updateOne(
        { _id: held.laneId, epoch: held.epoch, inFlight: deliveryId },
        { $pull: { inFlight: deliveryId } },
        { session },
      );
      if (fence.matchedCount === 0) {
        return false;
      }
      const changed = await this.#deliveries.updateOne(
        { _id: deliveryId, status: 'pending' },
        update,
        { session },
      );
      return changed.matchedCount === 1;
    });
  }

  /**
   * Deletes batches of matching documents (deliveries, or completion history
   * records) together with the events nothing references any more, each batch
   * in one transaction, so a crash can never leave an event without deliveries
   * behind. An append adding a delivery to the same event writes the event
   * document, so the two conflict and one restarts instead of leaving a delivery
   * without event. A delivery requeued since the batch was read conflicts with
   * its deletion and is read again.
   */
  async #purgeMatching(
    collection: Collection<PurgeableDocument>,
    filter: Filter<PurgeableDocument>,
  ): Promise<PurgeResult> {
    let deletedDeliveries = 0;
    let deletedEvents = 0;
    for (;;) {
      const step = await this.transaction(async (session) => {
        const batch = await collection
          .find(filter, { session, projection: { eventId: 1 }, limit: this.#purgeBatchSize })
          .toArray();
        if (batch.length === 0) {
          return { batchSize: 0, deletedDeliveries: 0, deletedEvents: 0 };
        }
        const eventIds = [...new Set(batch.map((document) => document.eventId))];
        const deleted = await collection.deleteMany(
          { _id: { $in: batch.map((document) => document._id) } },
          { session },
        );
        const stillReferenced = await this.#referencedEventIds(eventIds, session);
        const events = await this.#events.deleteMany(
          { _id: { $in: eventIds.filter((eventId) => !stillReferenced.has(eventId)) } },
          { session },
        );
        return {
          batchSize: batch.length,
          deletedDeliveries: deleted.deletedCount,
          deletedEvents: events.deletedCount,
        };
      });
      deletedDeliveries += step.deletedDeliveries;
      deletedEvents += step.deletedEvents;
      if (step.batchSize < this.#purgeBatchSize) {
        return { deletedDeliveries, deletedEvents };
      }
    }
  }

  /** A view of a collection that purge deletes from: deliveries or completion history records. */
  #purgeable(name: string): Collection<PurgeableDocument> {
    return this.#database.collection<PurgeableDocument>(name, {
      readPreference: 'primary',
      writeConcern: { w: 'majority' },
    });
  }

  /** Those of `eventIds` that a delivery or a completion history record references. */
  async #referencedEventIds(
    eventIds: readonly string[],
    session: ClientSession,
  ): Promise<Set<string>> {
    const filter = { eventId: { $in: [...eventIds] } };
    const options = { session, projection: { eventId: 1 } };
    const references = [
      ...(await this.#deliveries.find(filter, options).toArray()),
      ...(this.#completionHistory ? await this.#completions.find(filter, options).toArray() : []),
    ];
    return new Set(references.map((reference) => reference.eventId));
  }

  /**
   * Deletes, in batches, events created before `cutoff` that no delivery (or
   * completion history record) references. Each batch is found and deleted in
   * one transaction: an append adding a delivery to one of them writes the
   * event, so it conflicts with the deletion, which then restarts and sees the
   * delivery. Returns how many events were deleted.
   */
  async #purgeOrphanEvents(cutoff: Date): Promise<number> {
    const references = [
      this.#deliveries.collectionName,
      ...(this.#completionHistory ? [this.#completions.collectionName] : []),
    ];
    let deletedEvents = 0;
    for (;;) {
      const step = await this.transaction(async (session) => {
        const orphans = await this.#events
          .aggregate<{ _id: string }>(
            [
              { $match: { createdAt: { $lt: cutoff } } },
              { $sort: { createdAt: 1, _id: 1 } },
              ...references.flatMap((from) => [
                {
                  $lookup: {
                    from,
                    localField: '_id',
                    foreignField: 'eventId',
                    pipeline: [{ $limit: 1 }, { $project: { _id: 1 } }],
                    as: 'references',
                  },
                },
                { $match: { references: { $size: 0 } } },
              ]),
              { $limit: this.#purgeBatchSize },
              { $project: { _id: 1 } },
            ],
            { session },
          )
          .toArray();
        const deleted = await this.#events.deleteMany(
          { _id: { $in: orphans.map((orphan) => orphan._id) } },
          { session },
        );
        return { found: orphans.length, deleted: deleted.deletedCount };
      });
      deletedEvents += step.deleted;
      if (step.found < this.#purgeBatchSize) {
        return deletedEvents;
      }
    }
  }
}

/**
 * Frees a lane, provided it still has `epoch` and lists nothing beyond
 * `finished` (ids of deliveries known not to run): a delivery recorded
 * meanwhile keeps the lane leased.
 */
function freeLane(
  laneId: number,
  epoch: number,
  finished: readonly string[],
): AnyBulkWriteOperation<LaneDocument> {
  return {
    updateOne: {
      filter: {
        _id: laneId,
        epoch,
        inFlight: { $not: { $elemMatch: { $nin: [...finished] } } },
      },
      update: { $set: { owner: null, holder: null, leaseExpiresAt: null, inFlight: [] } },
    },
  };
}

/** Feeds the documents of `cursor` to `work` in batches of the cursor's batch size. */
async function inBatches<T extends Document>(
  cursor: AsyncIterable<T>,
  work: (batch: T[]) => Promise<void>,
): Promise<void> {
  let batch: T[] = [];
  for await (const document of cursor) {
    batch.push(document);
    if (batch.length === UPGRADE_BATCH_SIZE) {
      await work(batch);
      batch = [];
    }
  }
  if (batch.length > 0) {
    await work(batch);
  }
}

function leaseIdentity(lease: IssuedLease): string {
  return `${lease.deliveryId}\u0000${formatLeaseToken(lease.laneId, lease.epoch)}`;
}

function hasErrorCode(error: unknown, code: number): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === code;
}

function compareText(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}

function assertInTransaction(session: ClientSession, operation: string): void {
  if (!session.inTransaction()) {
    throw new ConfigurationError(
      `MongoEventStore.${operation} needs a ClientSession inside a started transaction (session.withTransaction or startTransaction). Without one, MongoDB commits each write immediately and the write is no longer atomic with yours`,
    );
  }
}
