import {
  ConfigurationError,
  createUuidV7,
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
  type IdentifierGenerator,
  type LeaseReference,
  type PurgeRequest,
  type PurgeResult,
} from '@relaybus/core';
import type {
  ClientSession,
  Collection,
  Db,
  Filter,
  MongoClient,
  TransactionOptions,
  UpdateFilter,
} from 'mongodb';
import { ClaimScan, orderingIdentity } from './claim-scan.js';
import {
  toDeliveryDocument,
  toDeliveryRecord,
  toEnvelope,
  toErrorInfo,
  toEventDocument,
  type DeliveryDocument,
  type EventDocument,
  type KeySequenceDocument,
} from './documents.js';
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
  /** Prefix of the three collection names. Default `relaybus_`. */
  readonly collectionPrefix?: string;
  /**
   * Options of the transactions the store opens. Default: snapshot read
   * concern, majority write concern, primary reads.
   */
  readonly transactionOptions?: TransactionOptions;
  /** Rows deleted per purge step. Default 500. */
  readonly purgeBatchSize?: number;
  /** Produces lease tokens. Default UUID v7. */
  readonly generateIdentifier?: IdentifierGenerator;
}

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
};
const NAMESPACE_EXISTS = 48;
const DEFAULT_PURGE_BATCH_SIZE = 500;
const DEFAULT_LIST_LIMIT = 100;
const MINIMUM_CANDIDATE_BATCH_SIZE = 32;
/** Deliveries that waited behind one unfinished predecessor before they are parked. */
const PARK_BACKLOG_THRESHOLD = 32;
const PARK_UPDATE_CHUNK_SIZE = 1_000;

/** Fields that exist only while a delivery is leased. */
const LEASE_FIELDS = { leaseOwner: '', leaseToken: '', leaseExpiresAt: '' } as const;
/** Fields that exist only while a delivery is unfinished. */
const FINISHED_FIELDS = { dueAt: '', lane: '' } as const;

/**
 * Event store on MongoDB 6+ multi-document transactions (replica set or sharded
 * cluster). The transaction handle is the driver's `ClientSession`, so the same
 * session can carry your own writes (native driver or Mongoose).
 *
 * Guarantees:
 * - `append` is atomic with the caller's writes and idempotent on event id and on
 *   (event id, handler). Duplicates are detected by reading, never by catching a
 *   duplicate-key error, which would abort the surrounding transaction.
 * - Per-key sequences are allocated from a counter document inside the append
 *   transaction. Two transactions appending the same key cannot both hold the
 *   counter, so the loser conflicts, restarts after the winner committed, and
 *   its sequence is higher: sequence order is commit order.
 * - `claim` selects candidates, then takes each with a conditional update that
 *   re-checks the exact state it selected, so exactly one claimer wins. A keyed
 *   delivery waits while an earlier delivery of its handler and key is
 *   unfinished, or while any other delivery of them runs under a live lease;
 *   the second rule is re-checked after the update, so a dead letter requeued
 *   during a claim cannot run alongside its successor.
 * - Every lease operation is fenced by delivery id, status and lease token.
 *
 * `transaction` may run `work` more than once (see {@link MongoEventStore.transaction}).
 */
export class MongoEventStore implements EventStore<ClientSession> {
  readonly #client: MongoClient;
  readonly #database: Db;
  readonly #events: Collection<EventDocument>;
  readonly #deliveries: Collection<DeliveryDocument>;
  readonly #keySequences: Collection<KeySequenceDocument>;
  readonly #transactionOptions: TransactionOptions;
  readonly #purgeBatchSize: number;
  readonly #generateIdentifier: IdentifierGenerator;

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
    this.#transactionOptions = options.transactionOptions ?? DEFAULT_TRANSACTION_OPTIONS;
    this.#purgeBatchSize = purgeBatchSize;
    this.#generateIdentifier = options.generateIdentifier ?? (() => createUuidV7());
  }

  /**
   * Verifies the deployment supports transactions and creates the indexes.
   * Safe to run repeatedly and from several processes.
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
    for (const collection of [this.#events, this.#deliveries, this.#keySequences]) {
      await this.#createCollectionIfMissing(collection.collectionName);
    }
    await this.#deliveries.createIndexes([
      // Idempotent appends and lookups by event.
      { key: { eventId: 1, handler: 1 }, name: 'event_handler', unique: true },
      // Claim: due work of a handler in due order. Holds ready deliveries only.
      {
        key: { handler: 1, dueAt: 1, _id: 1 },
        name: 'claim',
        partialFilterExpression: { lane: 'ready' },
      },
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
      // Live leases: a keyed delivery may not start while another of its key is running.
      {
        key: { handler: 1, key: 1, dueAt: 1 },
        name: 'running',
        partialFilterExpression: { status: 'processing', key: { $exists: true } },
      },
      // Statistics: oldest due delivery across handlers.
      { key: { dueAt: 1 }, name: 'due', partialFilterExpression: { dueAt: { $exists: true } } },
      // Purge of completed deliveries.
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
  }

  async #createCollectionIfMissing(name: string): Promise<void> {
    try {
      await this.#database.createCollection(name);
    } catch (error) {
      if (!(
        typeof error === 'object' &&
        error !== null &&
        'code' in error &&
        error.code === NAMESPACE_EXISTS
      )) {
        throw error;
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
          .find({ _id: { $in: eventIds } }, { session, projection: { key: 1, sequence: 1 } })
          .toArray()
      ).map((stored) => [stored._id, stored]),
    );
    const storedDeliveryKeys = new Set(
      (
        await this.#deliveries
          .find({ eventId: { $in: eventIds } }, { session, projection: { eventId: 1, handler: 1 } })
          .toArray()
      ).map((stored) => deliveryPairKey(stored.eventId, stored.handler)),
    );

    const appendedEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    const newEvents: EventDocument[] = [];
    const newDeliveries: DeliveryDocument[] = [];
    const touchedEventIds = new Set<string>();
    const orderingByEventId = new Map<
      string,
      { readonly key: string | null; readonly sequence: number | null }
    >();
    for (const [eventId, stored] of storedEvents) {
      orderingByEventId.set(eventId, { key: stored.key, sequence: stored.sequence });
    }

    for (const { event, deliveries } of entries) {
      let ordering = orderingByEventId.get(event.id);
      if (ordering === undefined) {
        const sequence = event.key === null ? null : await this.#nextSequence(event.key, session);
        ordering = { key: event.key, sequence };
        orderingByEventId.set(event.id, ordering);
        newEvents.push(toEventDocument(event, sequence));
        appendedEventIds.push(event.id);
      } else {
        duplicateEventIds.push(event.id);
      }
      for (const delivery of deliveries) {
        const pairKey = deliveryPairKey(event.id, delivery.handler);
        if (storedDeliveryKeys.has(pairKey)) {
          continue;
        }
        storedDeliveryKeys.add(pairKey);
        newDeliveries.push(toDeliveryDocument(delivery, event, ordering, now));
        if (storedEvents.has(event.id)) {
          touchedEventIds.add(event.id);
        }
      }
    }

    if (newEvents.length > 0) {
      // Upserts, not inserts: a duplicate-key error would abort the whole transaction,
      // whereas a concurrent writer of the same id surfaces as a retryable write conflict.
      await this.#events.bulkWrite(
        newEvents.map((document) => ({
          updateOne: {
            filter: { _id: document._id },
            update: { $setOnInsert: document },
            upsert: true,
          },
        })),
        { session, ordered: true },
      );
    }
    if (touchedEventIds.size > 0) {
      // A real write on the event makes a concurrent purge of it conflict with this transaction.
      await this.#events.updateMany(
        { _id: { $in: [...touchedEventIds] } },
        { $inc: { revision: 1 } },
        { session },
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

  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || request.limit <= 0) {
      return [];
    }
    await this.#unparkNextDeliveries(request.handlers);
    const claimed: DeliveryDocument[] = [];
    const scan = new ClaimScan();
    const candidates = this.#deliveries.find(
      { handler: { $in: [...request.handlers] }, dueAt: { $lte: request.now }, lane: 'ready' },
      {
        sort: { dueAt: 1, _id: 1 },
        batchSize: Math.max(request.limit * 2, MINIMUM_CANDIDATE_BATCH_SIZE),
      },
    );
    try {
      for await (const candidate of candidates) {
        const leased = await this.#claimCandidate(candidate, request, scan);
        if (leased !== undefined) {
          claimed.push(leased);
          if (claimed.length >= request.limit) {
            break;
          }
        }
      }
    } finally {
      await candidates.close();
    }
    await this.#parkDeliveries(scan.waitingBacklogs(PARK_BACKLOG_THRESHOLD));
    return this.#toClaimedDeliveries(claimed, request.leaseExpiresAt);
  }

  async extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, undefined, {
      $set: { leaseExpiresAt, dueAt: leaseExpiresAt, updatedAt: now },
    });
  }

  async complete(lease: LeaseReference, now: Date, session?: ClientSession): Promise<boolean> {
    if (session !== undefined) {
      assertInTransaction(session, 'complete');
    }
    return this.#updateLeased(lease, session, {
      $set: { status: 'completed', completedAt: now, updatedAt: now },
      $unset: { ...LEASE_FIELDS, ...FINISHED_FIELDS },
    });
  }

  async reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#updateLeased(lease, undefined, {
      $set: {
        status: 'pending',
        availableAt,
        dueAt: availableAt,
        lastError: toErrorInfo(error),
        updatedAt: now,
      },
      $unset: LEASE_FIELDS,
    });
  }

  async release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, undefined, {
      $set: { status: 'pending', availableAt: now, dueAt: now, updatedAt: now },
      $inc: { attempts: -1 },
      $unset: LEASE_FIELDS,
    });
  }

  async bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, undefined, {
      $set: { status: 'dead', lastError: toErrorInfo(error), updatedAt: now },
      $unset: { ...LEASE_FIELDS, ...FINISHED_FIELDS },
    });
  }

  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    // Strings only: an object here (say, parsed from an admin request) would be a query operator.
    const filter: Filter<DeliveryDocument> = {
      ...(query.status === undefined ? {} : { status: String(query.status) as DeliveryStatus }),
      ...(query.handler === undefined ? {} : { handler: String(query.handler) }),
      ...(query.eventType === undefined ? {} : { eventType: String(query.eventType) }),
      ...(query.eventId === undefined ? {} : { eventId: String(query.eventId) }),
    };
    const documents = await this.#deliveries
      .find(filter, { sort: { createdAt: 1, _id: 1 }, limit: query.limit ?? DEFAULT_LIST_LIMIT })
      .toArray();
    const events = await this.#readEnvelopes(documents.map((document) => document.eventId));
    return documents.map((document) => toDeliveryRecord(document, events.get(document.eventId)!));
  }

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

  async purge(request: PurgeRequest): Promise<PurgeResult> {
    const filters: Filter<DeliveryDocument>[] = [];
    if (request.completedBefore !== undefined) {
      filters.push({ status: 'completed', completedAt: { $lt: request.completedBefore } });
    }
    if (request.deadBefore !== undefined) {
      filters.push({ status: 'dead', updatedAt: { $lt: request.deadBefore } });
    }
    let deletedDeliveries = 0;
    let deletedEvents = 0;
    for (const filter of filters) {
      const result = await this.#purgeMatching(filter);
      deletedDeliveries += result.deletedDeliveries;
      deletedEvents += result.deletedEvents;
    }
    return { deletedDeliveries, deletedEvents };
  }

  async statistics(now: Date): Promise<EventStoreStatistics> {
    // Four index-only counts: measured at 300k deliveries they take 27 ms in parallel, against
    // 121 ms for one $group, which has to read every document or index entry.
    const statuses: DeliveryStatus[] = ['pending', 'processing', 'completed', 'dead'];
    const [pending, processing, completed, dead] = await Promise.all(
      statuses.map((status) => this.#deliveries.countDocuments({ status })),
    );
    const oldestDue = await this.#deliveries.findOne(
      { dueAt: { $lte: now } },
      { sort: { dueAt: 1 }, projection: { dueAt: 1 } },
    );
    return {
      pending: pending!,
      processing: processing!,
      completed: completed!,
      dead: dead!,
      oldestDueAt: oldestDue?.dueAt ?? null,
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

  async #claimCandidate(
    candidate: DeliveryDocument,
    request: ClaimRequest,
    scan: ClaimScan,
  ): Promise<DeliveryDocument | undefined> {
    const { key, sequence } = candidate;
    if (key === undefined) {
      return this.#lease(candidate, request);
    }
    const orderingId = orderingIdentity(candidate.handler, key);
    const known = scan.verdict(orderingId, sequence!);
    if (known === 'open-predecessor') {
      scan.noteWaiting(orderingId, candidate);
      return undefined;
    }
    if (known === 'live-lease') {
      return undefined;
    }
    const predecessor = await this.#findUnfinishedPredecessor(candidate.handler, key, sequence!);
    if (predecessor !== undefined) {
      scan.noteOpen(orderingId, predecessor);
      scan.noteWaiting(orderingId, candidate);
      return undefined;
    }
    if (await this.#hasLiveLease(candidate.handler, key, request.now)) {
      scan.noteLeaseHeld(orderingId);
      return undefined;
    }
    const leased = await this.#lease(candidate, request);
    if (leased === undefined) {
      return undefined;
    }
    // A dead letter requeued after the checks above can be claimed by another worker
    // before this lease is visible to it, and neither check would see the other. Only
    // the later delivery gives way, so exactly one of the two keeps running.
    const lateBlocker = await this.#findUnfinishedPredecessor(candidate.handler, key, sequence!);
    if (lateBlocker !== undefined) {
      await this.#restore(candidate, leased);
      scan.noteOpen(orderingId, lateBlocker);
      return undefined;
    }
    scan.noteClaimed(orderingId, sequence!);
    return leased;
  }

  /**
   * Takes the delivery with one conditional update that pins the exact state
   * that was selected, so a competing claimer, a finished lease, a reschedule
   * or a parking since then makes it match nothing.
   */
  async #lease(
    candidate: DeliveryDocument,
    request: ClaimRequest,
  ): Promise<DeliveryDocument | undefined> {
    const leased = await this.#deliveries.findOneAndUpdate(
      {
        _id: candidate._id,
        status: candidate.status,
        lane: 'ready',
        attempts: candidate.attempts,
        dueAt: candidate.dueAt!,
      },
      {
        $set: {
          status: 'processing',
          leaseOwner: request.leaseOwner,
          leaseToken: this.#generateIdentifier(),
          leaseExpiresAt: request.leaseExpiresAt,
          dueAt: request.leaseExpiresAt,
          updatedAt: request.now,
        },
        $inc: { attempts: 1 },
      },
      { returnDocument: 'after' },
    );
    return leased ?? undefined;
  }

  /** Puts a delivery that was leased a moment ago back exactly as it was selected. */
  async #restore(selected: DeliveryDocument, leased: DeliveryDocument): Promise<void> {
    const state = {
      status: selected.status,
      attempts: selected.attempts,
      dueAt: selected.dueAt!,
      updatedAt: selected.updatedAt,
    };
    await this.#deliveries.updateOne(
      { _id: leased._id, status: 'processing', leaseToken: leased.leaseToken! },
      selected.status === 'processing'
        ? {
            $set: {
              ...state,
              leaseOwner: selected.leaseOwner!,
              leaseToken: selected.leaseToken!,
              leaseExpiresAt: selected.leaseExpiresAt!,
            },
          }
        : { $set: state, $unset: LEASE_FIELDS },
    );
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

  /** Whether some delivery of the handler and key runs under a lease that has not expired. */
  async #hasLiveLease(handler: string, key: string, now: Date): Promise<boolean> {
    const running = await this.#deliveries.findOne(
      { handler, key, status: 'processing', dueAt: { $gt: now } },
      { projection: { _id: 1 } },
    );
    return running !== null;
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

  async #toClaimedDeliveries(
    leased: readonly DeliveryDocument[],
    leaseExpiresAt: Date,
  ): Promise<ClaimedDelivery[]> {
    const events = await this.#readEnvelopes(leased.map((document) => document.eventId));
    return leased.map((document) => ({
      deliveryId: document._id,
      leaseToken: document.leaseToken!,
      handler: document.handler,
      attempt: document.attempts,
      leaseExpiresAt,
      event: events.get(document.eventId)!,
    }));
  }

  async #readEnvelopes(eventIds: readonly string[]): Promise<Map<string, EventEnvelope>> {
    const documents = await this.#events.find({ _id: { $in: [...new Set(eventIds)] } }).toArray();
    return new Map(documents.map((document) => [document._id, toEnvelope(document)]));
  }

  async #updateLeased(
    lease: LeaseReference,
    session: ClientSession | undefined,
    update: UpdateFilter<DeliveryDocument>,
  ): Promise<boolean> {
    const result = await this.#deliveries.updateOne(
      { _id: lease.deliveryId, status: 'processing', leaseToken: lease.leaseToken },
      update,
      session === undefined ? {} : { session },
    );
    // Matched, not modified: a repeated renewal with identical values changes nothing but still holds the lease.
    return result.matchedCount === 1;
  }

  /**
   * Deletes batches of matching deliveries together with the events no delivery
   * references any more, each batch in one transaction, so a crash can never
   * leave an event without deliveries behind. An append adding a delivery to the
   * same event writes the event document, so the two conflict and one restarts
   * instead of leaving a delivery without event. A delivery requeued since the
   * batch was read conflicts with its deletion and is read again.
   */
  async #purgeMatching(filter: Filter<DeliveryDocument>): Promise<PurgeResult> {
    let deletedDeliveries = 0;
    let deletedEvents = 0;
    for (;;) {
      const step = await this.transaction(async (session) => {
        const batch = await this.#deliveries
          .find(filter, { session, projection: { eventId: 1 }, limit: this.#purgeBatchSize })
          .toArray();
        const eventIds = [...new Set(batch.map((document) => document.eventId))];
        const deliveries = await this.#deliveries.deleteMany(
          { _id: { $in: batch.map((document) => document._id) } },
          { session },
        );
        const stillReferenced = new Set(
          (
            await this.#deliveries
              .find({ eventId: { $in: eventIds } }, { session, projection: { eventId: 1 } })
              .toArray()
          ).map((document) => document.eventId),
        );
        const events = await this.#events.deleteMany(
          { _id: { $in: eventIds.filter((eventId) => !stillReferenced.has(eventId)) } },
          { session },
        );
        return {
          batchSize: batch.length,
          deletedDeliveries: deliveries.deletedCount,
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
}

function deliveryPairKey(eventId: string, handler: string): string {
  return `${eventId}\u0000${handler}`;
}

function assertInTransaction(session: ClientSession, operation: string): void {
  if (!session.inTransaction()) {
    throw new ConfigurationError(
      `MongoEventStore.${operation} needs a ClientSession inside a started transaction (session.withTransaction or startTransaction). Without one, MongoDB commits each write immediately and the write is no longer atomic with yours`,
    );
  }
}
