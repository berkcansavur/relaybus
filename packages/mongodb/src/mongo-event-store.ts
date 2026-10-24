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
import { assertPositiveInteger } from '@relaybus/core/internal';
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
   * (`<laneId>:<epoch>:<claims>`) and are no longer generated.
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
  /** `revision` of the lane as the claim read it; the record only applies while it is unchanged. */
  readonly revision: number;
}

/** The fields purge reads from a delivery or a completion history record. */
interface PurgeableDocument {
  readonly _id: string;
  readonly eventId: string;
  readonly status?: string;
  readonly completedAt?: Date;
  readonly updatedAt?: Date;
}

/** A claim this store instance handed out and has not seen settled. */
interface HandedOutClaim {
  readonly deliveryId: string;
  readonly owner: string;
  readonly laneId: number;
  readonly epoch: number;
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
 * - **Fencing.** A lease token is `<laneId>:<epoch>:<claims>`. The epoch grows
 *   whenever the lane is taken; `claims`, the delivery's claim generation, grows
 *   whenever a claim of it ends (retry, release, burial, lost lease), so the
 *   lease of an earlier claim never matches a later claim of the same delivery
 *   in the same lane epoch. Completion, retry, burial, release and renewal apply
 *   only while the lane has that epoch, lists the delivery as in flight, and the
 *   delivery has that claim generation.
 * - **Effectively-once for transactional handlers.** A worker that takes a lane
 *   over first writes every delivery the previous owner had in flight (it
 *   charges the lost attempt) and only then reads what to claim. A completion
 *   inside a transaction (a transactional handler's, or any completion when a
 *   completion history is kept) deletes the delivery after checking the lane:
 *   both write the same delivery document, so the later of the two either
 *   conflicts and retries (and then sees the new epoch or the missing
 *   delivery), or waits for the earlier to commit. Other completions run
 *   without a transaction: they read the deliveries' claim generations, then
 *   the lanes, and delete only deliveries whose generation did not change,
 *   which a takeover, a retry or a release always changes. Either way a completion and a takeover
 *   cannot both succeed. Completions never write the lane document, so
 *   concurrent completions of one lane never conflict with each other.
 * - **Attempts survive crashes.** `attempts` counts attempts that ended. The
 *   claim result reports `attempts + 1`; a failure records it; a lane taken over
 *   charges one attempt to every delivery the previous owner still had in flight,
 *   so a poison message still reaches the retry limit after crashes.
 *
 * - **Deduplication record.** A completion records its time on the event
 *   (`completedAt`). An event nothing references any more stays until
 *   `purge({ completedBefore })` passes that time, however old the event is, and
 *   `deadBefore` alone never removes it. Once purged, the event takes its list
 *   of handlers with it: a broker redelivery or publish retry of that event id
 *   afterwards is appended as a new event and runs its handlers again, as in
 *   the PostgreSQL store, so keep completed retention longer than any
 *   redelivery window.
 *
 * `transaction` may run `work` more than once (see {@link MongoEventStore.transaction}).
 * Claims of one `leaseOwner` on one store instance run one after the other
 * (a claim that overlaps another of the same owner waits for it); different
 * owners claim concurrently, from any number of processes. Lanes are held per store
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
  /** The last claim of each owner still running on this instance; the next one waits for it. */
  readonly #claimsInProgress = new Map<string, Promise<unknown>>();
  /**
   * Claims this instance handed out that are not settled yet, keyed by
   * {@link leaseKey} (delivery id and exact lease token), with the owner. A
   * claim is settled when a completion, retry, release or burial of exactly
   * that lease returned, or, for a completion inside the caller's transaction
   * (which may still retry or roll back), when a later claim of the owner finds
   * the delivery gone. After a failed claim, a listed delivery stays listed only
   * if one of these is its current claim (current lane epoch and claim generation).
   */
  readonly #handedOut = new Map<string, HandedOutClaim>();
  /** Owners whose lanes on this instance still need reconciling after a failed claim. */
  readonly #unreconciledOwners = new Set<string>();

  constructor(options: MongoEventStoreOptions) {
    const prefix = options.collectionPrefix ?? 'relaybus_';
    assertCollectionPrefix(prefix);
    if (options.database !== undefined) {
      assertDatabaseName(options.database);
    }
    const purgeBatchSize = assertPositiveInteger(
      options.purgeBatchSize ?? DEFAULT_PURGE_BATCH_SIZE,
      'purgeBatchSize',
    );
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
    await this.#events.createIndexes([
      { key: { createdAt: 1, _id: 1 }, name: 'created' },
      // Purge of events whose deliveries all completed: they are kept until `completedBefore` passes this.
      {
        key: { completedAt: 1, _id: 1 },
        name: 'completed',
        partialFilterExpression: { completedAt: { $exists: true } },
      },
    ]);
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
                // Only an event nobody converted yet: another process may have finished and subscribed a handler.
                filter: { _id: event._id, handlers: { $exists: false } },
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
      orderingByEventId.set(eventId, {
        key: stored.key ?? null,
        sequence: stored.sequence ?? null,
      });
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
   * first, so a claim that fails anywhere after the take loses no charge; the
   * charge of a lost claim is recorded on the delivery, so charging the list
   * again after a failure before the clear charges nothing twice.
   * Nothing a failed claim did leaves a delivery listed as running.
   */
  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || request.limit <= 0) {
      return [];
    }
    // Claims of one owner read the same held lanes and in-flight lists, so they must not overlap
    // (bus.runOnce() next to a running dispatcher, for instance): they run one after the other.
    const owner = request.leaseOwner;
    const previous = this.#claimsInProgress.get(owner) ?? Promise.resolve();
    const claimed = previous.then(() => this.#claimReconciled(request));
    const settled = Promise.allSettled([claimed]);
    this.#claimsInProgress.set(owner, settled);
    void settled.then(() => {
      if (this.#claimsInProgress.get(owner) === settled) {
        this.#claimsInProgress.delete(owner);
      }
    });
    return claimed;
  }

  /**
   * Runs one claim and remembers what it hands out. When the claim fails at any
   * point, also after its record reached the database without the reply
   * reaching this process, the lanes this owner holds on this instance are
   * reconciled: every listed delivery this instance has not handed out, or has
   * handed out and seen settled, is taken out of them, so nothing stays listed
   * as running that nobody runs.
   */
  async #claimReconciled(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    const owner = request.leaseOwner;
    if (this.#unreconciledOwners.has(owner)) {
      // An earlier reconciliation failed: a delivery may still be listed that nobody runs, and the
      // owner's other deliveries may keep the lane alive, so it is done before claiming anything.
      await this.#reconcileLanes(owner);
      this.#unreconciledOwners.delete(owner);
    }
    try {
      const claimed = await this.#claimNow(request);
      for (const delivery of claimed) {
        const { laneId, epoch } = parseLeaseToken(delivery.leaseToken)!;
        this.#handedOut.set(leaseKey(delivery.deliveryId, delivery.leaseToken), {
          deliveryId: delivery.deliveryId,
          owner: request.leaseOwner,
          laneId,
          epoch,
        });
      }
      return claimed;
    } catch (error) {
      const [reconciled] = await Promise.allSettled([this.#reconcileLanes(owner)]);
      if (reconciled.status === 'rejected') {
        this.#unreconciledOwners.add(owner);
      }
      throw error;
    }
  }

  /**
   * Takes every delivery this instance does not run out of the lanes `owner`
   * holds on it: every listed delivery whose current claim (lane epoch and
   * claim generation now) was not handed out by this instance, or was settled.
   * One transaction, and the lanes are written first under their epoch and
   * holder: a worker taking a lane over meanwhile conflicts with it, so its
   * claim is never touched. The claim generation of every delivery taken out
   * moves on, so no lease of those claims (a completion that failed and is
   * retried later, say) can ever match a later claim of the same delivery.
   * Registrations of this owner that are not the current claim of a listed
   * delivery are dropped.
   */
  async #reconcileLanes(owner: string): Promise<void> {
    await this.transaction(async (session) => {
      const held = await this.#lanes
        .find({ owner, holder: this.#instanceId }, { session })
        .toArray();
      const listed = held.flatMap((lane) => lane.inFlight);
      const claimsById = new Map(
        (
          await this.#deliveries
            .find(
              { _id: { $in: listed }, status: 'pending' },
              { session, projection: { claims: 1 } },
            )
            .toArray()
        ).map((document) => [document._id, document.claims ?? 0]),
      );
      const current = new Set<string>();
      const unclaimed = held.map((lane) => ({
        lane,
        ids: lane.inFlight.filter((id) => {
          const key = leaseKey(id, formatLeaseToken(lane._id, lane.epoch, claimsById.get(id) ?? 0));
          current.add(key);
          return !this.#handedOut.has(key);
        }),
      }));
      for (const [key, entry] of this.#handedOut) {
        if (entry.owner === owner && !current.has(key)) {
          this.#handedOut.delete(key);
        }
      }
      if (held.length === 0) {
        return;
      }
      // Every lane gets a new revision, also one with nothing to take out: a record of the failed
      // claim that is still on its way to the server then matches nothing when it arrives.
      await this.#lanes.bulkWrite(
        unclaimed.map(({ lane, ids }) => ({
          updateOne: {
            filter: { _id: lane._id, epoch: lane.epoch, holder: this.#instanceId },
            update: { $pull: { inFlight: { $in: ids } }, $inc: { revision: 1 } },
          },
        })),
        { session, ordered: false },
      );
      await this.#deliveries.updateMany(
        { _id: { $in: unclaimed.flatMap((entry) => entry.ids) }, status: 'pending' },
        { $inc: { claims: 1 } },
        { session },
      );
    });
  }

  /**
   * Runs `operation`, which ends the claims of `leases` (completion, retry,
   * release, burial), and once it returned forgets exactly those leases as
   * handed out. When it throws, they stay registered: the caller may retry
   * with the same lease, which must still find its claim.
   */
  async #settle<T>(leases: readonly LeaseReference[], operation: () => Promise<T>): Promise<T> {
    const result = await operation();
    for (const lease of leases) {
      this.#handedOut.delete(leaseKey(String(lease.deliveryId), String(lease.leaseToken)));
    }
    return result;
  }

  async #claimNow(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    // Independent reads, so they share one round trip: unparking only has to finish before candidates are read.
    const [held] = await Promise.all([
      this.#lanes
        .find({
          owner: request.leaseOwner,
          holder: this.#instanceId,
          leaseExpiresAt: { $gt: request.now },
        })
        .toArray(),
      this.#unparkNextDeliveries(request.handlers),
    ]);
    await this.#chargeLost(held);
    const scope = new Map<number, ScopeLane>(
      held.map((lane) => [
        lane._id,
        {
          laneId: lane._id,
          epoch: lane.epoch,
          listed: lane.inFlight,
          revision: lane.revision ?? 0,
        },
      ]),
    );
    // Loaded while the first candidates are read; only keyed candidates and the record need it.
    const running = this.#findRunning(held.flatMap((lane) => lane.inFlight)).then((documents) => ({
      ids: new Set(documents.map((document) => document._id)),
      keys: new Set(
        documents.flatMap((document) =>
          document.key === undefined ? [] : [orderingIdentity(document.handler, document.key)],
        ),
      ),
    }));
    const scan = new ClaimScan();
    const picked: DeliveryDocument[] = [];
    const pickFrom = async (lanes: readonly ScopeLane[]): Promise<void> => {
      for (let offset = 0; offset < lanes.length; offset += LANES_PER_CANDIDATE_QUERY) {
        if (picked.length < request.limit) {
          await this.#pickCandidates(
            lanes.slice(offset, offset + LANES_PER_CANDIDATE_QUERY),
            request,
            scan,
            running,
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
    const runningIds = (await running).ids;
    // Listed but no longer pending: completed (a burial or retry would have unlisted it). Such a
    // claim is settled, also when it was completed inside a caller's transaction that committed.
    // Only this owner's registrations of the lane epochs it read: the same delivery may meanwhile
    // have been requeued and handed out again, under another owner or lane epoch.
    const gone = new Set(
      held.flatMap((lane) =>
        lane.inFlight
          .filter((id) => !runningIds.has(id))
          .map((id) => `${id}\u0000${lane._id}:${lane.epoch}`),
      ),
    );
    for (const [key, entry] of this.#handedOut) {
      if (
        entry.owner === request.leaseOwner &&
        gone.has(`${entry.deliveryId}\u0000${entry.laneId}:${entry.epoch}`)
      ) {
        this.#handedOut.delete(key);
      }
    }
    const { recorded, handedBack } = await this.#recordClaim(scope, picked, runningIds, request);
    const claimed = await this.#releaseOvertaken(
      picked.filter((document) => recorded.has(document.laneId)),
      scope,
    );
    await this.#parkWaiting(scan.waitingBacklogs(PARK_BACKLOG_THRESHOLD, handedBack));
    return claimed.map((document) => ({
      deliveryId: document._id,
      leaseToken: formatLeaseToken(
        document.laneId,
        scope.get(document.laneId)!.epoch,
        document.claims ?? 0,
      ),
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
    // One transaction: a stale lease must not extend the lane of a newer claim of the delivery.
    // A claim ending or starting meanwhile writes the delivery or the lane, so the two conflict.
    return this.transaction(async (session) => {
      const running = await this.#deliveries.findOne(ofClaim({ ...held, deliveryId }), {
        session,
        projection: { _id: 1 },
      });
      if (running === null) {
        return false;
      }
      const renewed = await this.#lanes.updateOne(
        { _id: held.laneId, epoch: held.epoch, inFlight: deliveryId },
        { $max: { leaseExpiresAt } },
        { session },
      );
      return renewed.matchedCount === 1;
    });
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
   * Completes many leases with a fixed number of round trips, whatever lanes
   * they belong to: three commands without a completion history, one
   * transaction with it (see {@link MongoEventStore.#deleteFenced}).
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
      $inc: { attempts: 1, claims: 1 },
    });
  }

  /** Gives a running delivery back without counting the attempt: pending, due `now`. */
  async release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { availableAt: now, dueAt: now, updatedAt: now },
      $inc: { claims: 1 },
    });
  }

  /** Moves a running delivery to dead and counts the attempt that ended. */
  async bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { status: 'dead', lastError: toErrorInfo(error), updatedAt: now },
      $unset: FINISHED_FIELDS,
      $inc: { attempts: 1, claims: 1 },
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
        idle.map((lane) => freeLane(lane._id, lane.epoch, lane.revision ?? 0, lane.inFlight)),
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
   * nothing references are deleted: one with a completed delivery once its last
   * completion is before `completedBefore` (never by `deadBefore` alone), one
   * that never had a completion (no subscribers, or only dead letters) once it
   * was created before the earliest given cutoff. The batch step applies the
   * same rule to events whose last completion is recorded.
   */
  async purge(request: PurgeRequest): Promise<PurgeResult> {
    const total = { deletedDeliveries: 0, deletedEvents: 0 };
    const steps: Array<() => Promise<PurgeResult>> = [];
    const { completedBefore, deadBefore } = request;
    if (completedBefore !== undefined) {
      steps.push(() =>
        this.#purgeMatching(
          this.#purgeable(this.#deliveries.collectionName),
          { status: 'completed', completedAt: { $lt: completedBefore } },
          completedBefore,
        ),
      );
      if (this.#completionHistory) {
        steps.push(() =>
          this.#purgeMatching(
            this.#purgeable(this.#completions.collectionName),
            { completedAt: { $lt: completedBefore } },
            completedBefore,
          ),
        );
      }
    }
    if (deadBefore !== undefined) {
      steps.push(() =>
        this.#purgeMatching(
          this.#purgeable(this.#deliveries.collectionName),
          { status: 'dead', updatedAt: { $lt: deadBefore } },
          completedBefore,
        ),
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
      total.deletedEvents += await this.#purgeOrphanEvents(orphanCutoff, completedBefore);
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
   * Up to `laneCount` lanes nobody holds under a live lease that have a due,
   * ready delivery of the requested handlers, the lane with the oldest such
   * delivery first, leaving out `passed`. One round trip: the oldest delivery
   * of each lane is one index probe on the server. A lane whose due deliveries
   * all wait behind an unfinished predecessor is taken at most once: the claim
   * that hands it back parks them (see {@link ClaimScan.waitingBacklogs}).
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
              { $limit: 1 },
              { $project: { dueAt: 1 } },
            ],
            as: 'oldest',
          },
        },
        { $unwind: '$oldest' },
        { $sort: { 'oldest.dueAt': 1, 'oldest._id': 1 } },
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
                // Each lost delivery keeps the epoch it was claimed under: the key of its charge.
                lost: {
                  $setUnion: [
                    { $ifNull: ['$lost', []] },
                    { $map: { input: '$inFlight', in: { id: '$$this', epoch: '$epoch' } } },
                  ],
                },
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
    return taken.map((lane) => ({
      laneId: lane._id,
      epoch: lane.epoch,
      listed: lane.inFlight,
      revision: lane.revision ?? 0,
    }));
  }

  /**
   * Charges one attempt to every pending delivery listed in the `lost` lists of
   * `lanes` (deliveries whose lane was taken while they ran), then clears those
   * lists under the lanes' current epochs. The charge is a write on each lost
   * delivery that comes before any read of the lane's deliveries: a completion
   * of the previous owner deletes the same documents, so the two conflict and
   * cannot both succeed. The charge also moves the claim generation on, which
   * fences every lease of the lost claim, and records the lost claim
   * (`lostAt` = lane and epoch it was claimed under): a holder that charges the
   * list again after a failure before the clear skips deliveries already
   * charged for that claim.
   */
  async #chargeLost(lanes: readonly LaneDocument[]): Promise<void> {
    const losing = lanes.filter((lane) => lane.lost.length > 0);
    if (losing.length === 0) {
      return;
    }
    // Oldest lost claims first: a delivery's lane never changes and its epochs only grow, so
    // `lostAt` (the epoch of the last lost claim charged) only moves forward, and a charge of a
    // claim at or before it, repeated or delayed, matches nothing.
    const lost = losing
      .flatMap((lane) => lane.lost)
      .sort((left, right) => left.epoch - right.epoch);
    await this.#deliveries.bulkWrite(
      lost.map(({ id, epoch }) => ({
        updateOne: {
          filter: { _id: id, status: 'pending', lostAt: { $not: { $gte: epoch } } },
          update: { $inc: { attempts: 1, claims: 1 }, $set: { lostAt: epoch } },
        },
      })),
      { ordered: true },
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
   * deliveries of a key live in the key's lane, which this owner holds, so the
   * running keys (loaded once from the held lanes) and this claim's own picks
   * are all of them.
   *
   * Candidates are read in pages with a limit, so no cursor is left open, and
   * the predecessors of the keys of a page are looked up in parallel, one
   * lookup per key.
   */
  async #pickCandidates(
    lanes: readonly ScopeLane[],
    request: ClaimRequest,
    scan: ClaimScan,
    running: Promise<{ readonly keys: Set<string> }>,
    picked: DeliveryDocument[],
  ): Promise<void> {
    const pageSize = Math.max((request.limit - picked.length) * 2, MINIMUM_CANDIDATE_BATCH_SIZE);
    const filter: Filter<DeliveryDocument> = {
      laneId: { $in: lanes.map((lane) => lane.laneId) },
      handler: { $in: [...request.handlers] },
      lane: 'ready',
      dueAt: { $lte: request.now },
      _id: { $nin: lanes.flatMap((lane) => lane.listed) },
    };
    let after: DeliveryDocument | undefined;
    for (;;) {
      const [page, { keys }] = await Promise.all([
        this.#deliveries
          .find(
            after === undefined
              ? filter
              : {
                  $and: [
                    filter,
                    {
                      $or: [
                        { dueAt: { $gt: after.dueAt! } },
                        { dueAt: after.dueAt!, _id: { $gt: after._id } },
                      ],
                    },
                  ],
                },
            { sort: { dueAt: 1, _id: 1 }, limit: pageSize },
          )
          .toArray(),
        running,
      ]);
      // Look up predecessors only for as many candidates as the claim still needs, then more.
      for (let offset = 0; offset < page.length;) {
        const slice = page.slice(offset, offset + request.limit - picked.length);
        offset += slice.length;
        await this.#notePredecessors(slice, scan);
        for (const candidate of slice) {
          if (isClaimable(candidate, scan, keys)) {
            picked.push(candidate);
          }
        }
        if (picked.length >= request.limit) {
          return;
        }
      }
      if (page.length < pageSize) {
        return;
      }
      after = page.at(-1);
    }
  }

  /**
   * Looks up, in parallel and once per key, whether an unfinished delivery
   * precedes the earliest candidate of each keyed (handler, key) of `page` that
   * this claim knows nothing about yet, and tells `scan`. The earliest candidate
   * is itself unfinished, so every later candidate of its key waits at least for it.
   */
  async #notePredecessors(page: readonly DeliveryDocument[], scan: ClaimScan): Promise<void> {
    const earliest = new Map<string, DeliveryDocument>();
    for (const candidate of page) {
      if (candidate.key !== undefined) {
        const orderingId = orderingIdentity(candidate.handler, candidate.key);
        // Pages come in due order, not in sequence order, so keep the lowest sequence.
        const known = earliest.get(orderingId);
        if (!scan.knows(orderingId) && (known?.sequence ?? Infinity) > candidate.sequence!) {
          earliest.set(orderingId, candidate);
        }
      }
    }
    await Promise.all(
      [...earliest].map(async ([orderingId, candidate]) => {
        const predecessor = await this.#findUnfinishedPredecessor(
          candidate.handler,
          candidate.key!,
          candidate.sequence!,
        );
        scan.noteOpen(orderingId, predecessor ?? candidate.sequence!);
      }),
    );
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
  ): Promise<{ readonly recorded: Set<number>; readonly handedBack: Set<number> }> {
    const picksByLane = new Map<number, string[]>();
    const handedBack = new Set<number>();
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
              // A record that reaches the server late (after its claim failed and reconciled)
              // finds a newer revision and changes nothing.
              revision: atCount(lane.revision),
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
                  revision: { $add: [{ $ifNull: ['$revision', 0] }, 1] },
                },
              },
            ],
          },
        });
      } else if (gone.length === lane.listed.length) {
        operations.push(freeLane(lane.laneId, lane.epoch, lane.revision, gone));
        handedBack.add(lane.laneId);
      }
    }
    if (operations.length === 0) {
      return { recorded: new Set(), handedBack };
    }
    const result = await this.#lanes.bulkWrite(operations, { ordered: false });
    if (result.matchedCount === operations.length) {
      return { recorded: new Set(picksByLane.keys()), handedBack };
    }
    const current = await this.#lanes
      .find(
        { _id: { $in: [...picksByLane.keys()] } },
        { projection: { epoch: 1, owner: 1, holder: 1, revision: 1 } },
      )
      .toArray();
    const recorded = new Set(
      current
        .filter(
          (lane) =>
            lane.epoch === scope.get(lane._id)!.epoch &&
            lane.revision === scope.get(lane._id)!.revision + 1 &&
            lane.owner === request.leaseOwner &&
            lane.holder === this.#instanceId,
        )
        .map((lane) => lane._id),
    );
    return { recorded, handedBack };
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
    const keyed = claimed.filter((document) => document.key !== undefined);
    if (keyed.length === 0) {
      return [...claimed];
    }
    // One query for the whole batch: each branch is one probe of the ordering index.
    const blockers = await this.#deliveries
      .find(
        {
          $or: keyed.map((document) => ({
            handler: document.handler,
            key: document.key!,
            sequence: { $lt: document.sequence! },
            dueAt: { $exists: true },
          })),
        },
        { projection: { handler: 1, key: 1 } },
      )
      .toArray();
    const blocked = new Set(
      blockers.map((blocker) => orderingIdentity(blocker.handler, blocker.key!)),
    );
    const overtaken = new Set(
      keyed
        .filter((document) => blocked.has(orderingIdentity(document.handler, document.key!)))
        .map((document) => document._id),
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

  /** Takes recorded picks out of their lanes' in-flight lists again, under the claim's epochs and record revisions. */
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
          // Under the revision the record left: a pull that reaches the server late, after a
          // reconciliation and a newer claim of the same delivery, matches nothing.
          {
            _id: laneId,
            epoch: scope.get(laneId)!.epoch,
            revision: scope.get(laneId)!.revision + 1,
          },
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
    // The lowest parked delivery of every key, one aggregation per handler in parallel. With the
    // handler fixed and one group field, the server answers it with a distinct scan of the small
    // partial index: one index probe per parked key, not one per parked delivery.
    const lowest = (
      await Promise.all(
        [...new Set(handlers)].map((handler) =>
          this.#deliveries
            .aggregate<{ _id: string; id: string; sequence: number }>([
              { $match: { handler, lane: 'parked' } },
              { $sort: { handler: 1, key: 1, sequence: 1 } },
              {
                $group: { _id: '$key', id: { $first: '$_id' }, sequence: { $first: '$sequence' } },
              },
            ])
            .toArray()
            .then((groups) => groups.map((group) => ({ ...group, handler }))),
        ),
      )
    ).flat();
    const next = await Promise.all(
      lowest.map(async (parked) =>
        (await this.#findUnfinishedPredecessor(parked.handler, parked._id, parked.sequence)) ===
        undefined
          ? [parked.id]
          : [],
      ),
    );
    const ready = next.flat();
    if (ready.length > 0) {
      await this.#deliveries.updateMany(
        { _id: { $in: ready }, lane: 'parked' },
        { $set: { lane: 'ready' } },
      );
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
    // Inside the caller's transaction the completion may still be retried or rolled back, so it
    // stays registered until a later claim finds the delivery gone.
    return session === undefined
      ? this.#settle(leases, () => this.#completeIssued(leases, now, undefined))
      : this.#completeIssued(leases, now, session);
  }

  async #completeIssued(
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
      if (session !== undefined) {
        completed = await this.#deleteCompleted(issued, now, session);
      } else if (this.#completionHistory) {
        completed = await this.transaction((own) => this.#deleteCompleted(issued, now, own));
      } else {
        completed = await this.#deleteFenced(issued, now);
      }
    }
    return leases.map(
      (lease, index) =>
        parsed[index] !== undefined &&
        completed.delete(leaseIdentity({ deliveryId: String(lease.deliveryId), ...parsed[index] })),
    );
  }

  /**
   * Completion without a transaction, used when no history record has to be
   * written with the deletion. The deliveries' claim generations are read
   * first, then the lanes, and a delivery is deleted only if it still has the
   * lease's generation. A takeover moves the generation of every delivery it
   * takes from the previous owner on before it reads anything, and so do a
   * retry, a release and a burial, so:
   * - a takeover before the delivery read shows up in the lane read (new epoch);
   * - a takeover between the two reads shows up in the lane read as well;
   * - a takeover, retry, release or burial after the lane read changed the
   *   generation, so the delete matches nothing.
   * A completion and a takeover (or the end of the claim) therefore never both succeed. Two concurrent
   * completions of the same lease may both report true (the dispatcher never
   * issues them); one call with a lease listed twice reports it once.
   */
  async #deleteFenced(issued: readonly IssuedLease[], now: Date): Promise<Set<string>> {
    const ids = [...new Set(issued.map((lease) => lease.deliveryId))];
    const documents = await this.#deliveries
      .find({ _id: { $in: ids }, status: 'pending' }, { projection: { claims: 1, eventId: 1 } })
      .toArray();
    const lanes = new Map(
      (
        await this.#lanes
          .find(
            { _id: { $in: [...new Set(issued.map((lease) => lease.laneId))] } },
            { projection: { epoch: 1, inFlight: 1 } },
          )
          .toArray()
      ).map((lane) => [lane._id, lane]),
    );
    const fenced = fencedLeases(issued, lanes, documents);
    if (fenced.length === 0) {
      return new Set();
    }
    await this.#markEventsCompleted(fenced, documents, now);
    const deleted = await this.#deliveries.deleteMany({ $or: fenced.map(ofClaim) });
    // Each branch of the delete matches at most one document of exactly that claim, so all of
    // them deleting proves every completion. When fewer did, a claim ended or changed hands after
    // it was read, and nothing tells which of them this call deleted: a missing document may as
    // well have been completed by the claim that took it over. Report none, never a guess.
    return deleted.deletedCount === fenced.length
      ? new Set(fenced.map((lease) => leaseIdentity(lease)))
      : new Set();
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
    const documents = await this.#deliveries
      .find(
        { _id: { $in: [...new Set(issued.map((lease) => lease.deliveryId))] }, status: 'pending' },
        { session },
      )
      .toArray();
    const fenced = fencedLeases(issued, lanes, documents);
    if (fenced.length === 0) {
      return new Set();
    }
    await this.#markEventsCompleted(fenced, documents, now, session);
    await this.#deliveries.deleteMany({ $or: fenced.map(ofClaim) }, { session });
    if (this.#completionHistory) {
      const completedIds = new Set(fenced.map((lease) => lease.deliveryId));
      await this.#completions.insertMany(
        documents
          .filter((document) => completedIds.has(document._id))
          .map((document) => toCompletedDeliveryDocument(document, now)),
        { session },
      );
    }
    return new Set(fenced.map((lease) => leaseIdentity(lease)));
  }

  /**
   * Records `now` as the latest completion of the events of the fenced
   * deliveries (`$max`, so an earlier clock never moves it back). Written
   * before the deletion, it can only be later than the real last completion
   * (when the deletion then fails), which keeps the event longer, never shorter.
   * Inside a transaction it is part of it: the transaction may hold the event
   * already (a transactional handler that appended to its own event), and a
   * write from outside would wait for it forever. Deliveries of one event that
   * complete in concurrent transactions (fan-out to transactional handlers)
   * therefore conflict on the event document, and one of them retries.
   */
  async #markEventsCompleted(
    fenced: readonly IssuedLease[],
    documents: readonly Pick<DeliveryDocument, '_id' | 'eventId'>[],
    now: Date,
    session?: ClientSession,
  ): Promise<void> {
    const completedIds = new Set(fenced.map((lease) => lease.deliveryId));
    const eventIds = [
      ...new Set(
        documents
          .filter((document) => completedIds.has(document._id))
          .map((document) => document.eventId),
      ),
    ];
    await this.#events.updateMany(
      { _id: { $in: eventIds } },
      { $max: { completedAt: now } },
      session === undefined ? {} : { session },
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
    const ofThisClaim = ofClaim({ ...held, deliveryId });
    return this.#settle([lease], () => this.#leaveFlightNow(held, deliveryId, ofThisClaim, update));
  }

  async #leaveFlightNow(
    held: LaneLease,
    deliveryId: string,
    ofThisClaim: Filter<DeliveryDocument>,
    update: UpdateFilter<DeliveryDocument>,
  ): Promise<boolean> {
    return this.transaction(async (session) => {
      // The claim generation is checked before the lane is touched: a later claim of the same
      // delivery in the same lane epoch is listed under the same id and must stay listed.
      if (
        (await this.#deliveries.findOne(ofThisClaim, { session, projection: { _id: 1 } })) === null
      ) {
        return false;
      }
      const fence = await this.#lanes.updateOne(
        { _id: held.laneId, epoch: held.epoch, inFlight: deliveryId },
        { $pull: { inFlight: deliveryId } },
        { session },
      );
      if (fence.matchedCount === 0) {
        return false;
      }
      const changed = await this.#deliveries.updateOne(ofThisClaim, update, { session });
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
    completedBefore: Date | undefined,
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
          {
            _id: { $in: eventIds.filter((eventId) => !stillReferenced.has(eventId)) },
            // An event a delivery of which completed stays as deduplication record until
            // `completedBefore` passes its last completion, whatever this batch purged.
            $or: [
              { completedAt: { $exists: false } },
              ...(completedBefore === undefined ? [] : [{ completedAt: { $lt: completedBefore } }]),
            ],
          },
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
  async #purgeOrphanEvents(cutoff: Date, completedBefore: Date | undefined): Promise<number> {
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
              {
                $match: {
                  $or: [
                    // Never completed (no subscribers, or every delivery dead): by age.
                    { completedAt: { $exists: false }, createdAt: { $lt: cutoff } },
                    // Completed: by the time of its last completion, and only under the completed cutoff.
                    ...(completedBefore === undefined
                      ? []
                      : [{ completedAt: { $lt: completedBefore } }]),
                  ],
                },
              },
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
 * Whether a candidate may be claimed, given what `scan` knows about its key
 * (see `MongoEventStore.#notePredecessors`) and `blockedKeys`: the keys that
 * run, to which every key this claim takes is added, so a claim takes at most
 * one delivery per key.
 */
function isClaimable(
  candidate: DeliveryDocument,
  scan: ClaimScan,
  blockedKeys: Set<string>,
): boolean {
  const { key, sequence } = candidate;
  if (key === undefined) {
    return true;
  }
  const orderingId = orderingIdentity(candidate.handler, key);
  if (scan.waits(orderingId, sequence!)) {
    scan.noteWaiting(orderingId, candidate._id, candidate.laneId);
    return false;
  }
  if (blockedKeys.has(orderingId)) {
    return false;
  }
  blockedKeys.add(orderingId);
  return true;
}

/**
 * Frees a lane, provided it still has `epoch` and lists nothing beyond
 * `finished` (ids of deliveries known not to run): a delivery recorded
 * meanwhile keeps the lane leased.
 */
function freeLane(
  laneId: number,
  epoch: number,
  revision: number,
  finished: readonly string[],
): AnyBulkWriteOperation<LaneDocument> {
  return {
    updateOne: {
      filter: {
        _id: laneId,
        epoch,
        revision: atCount(revision),
        inFlight: { $not: { $elemMatch: { $nin: [...finished] } } },
      },
      update: {
        $set: { owner: null, holder: null, leaseExpiresAt: null, inFlight: [] },
        $inc: { revision: 1 },
      },
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

/** Matches a counter (claim generation, lane revision) equal to `count`; a missing counter is 0. */
function atCount(count: number): number | { $not: { $gt: number } } {
  // Counters are never negative, so "not above 0" is 0 or absent.
  return count === 0 ? { $not: { $gt: 0 } } : count;
}

/** The leases whose lane still has their epoch and lists the delivery, and whose delivery is still in their claim. */
function fencedLeases(
  issued: readonly IssuedLease[],
  lanes: ReadonlyMap<number, Pick<LaneDocument, 'epoch' | 'inFlight'>>,
  documents: readonly Pick<DeliveryDocument, '_id' | 'claims'>[],
): IssuedLease[] {
  const claimsById = new Map(documents.map((document) => [document._id, document.claims ?? 0]));
  const fenced = new Map<string, IssuedLease>();
  for (const lease of issued) {
    const lane = lanes.get(lease.laneId)!;
    if (
      claimsById.get(lease.deliveryId) === lease.claims &&
      lane.epoch === lease.epoch &&
      lane.inFlight.includes(lease.deliveryId)
    ) {
      // A lease listed twice completes once.
      fenced.set(leaseIdentity(lease), lease);
    }
  }
  return [...fenced.values()];
}

/** The delivery document of exactly this claim, still pending. */
function ofClaim(lease: IssuedLease): Filter<DeliveryDocument> {
  return { _id: lease.deliveryId, status: 'pending', claims: atCount(lease.claims) };
}

/** Key of a handed-out claim: the delivery and the exact lease token it was handed out with. */
function leaseKey(deliveryId: string, leaseToken: string): string {
  return `${deliveryId}\u0000${leaseToken}`;
}

function leaseIdentity(lease: IssuedLease): string {
  return `${lease.deliveryId}\u0000${formatLeaseToken(lease.laneId, lease.epoch, lease.claims)}`;
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
