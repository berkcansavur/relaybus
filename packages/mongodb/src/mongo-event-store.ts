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

/** Fields that exist only while a delivery is leased. */
const LEASE_FIELDS = { leaseOwner: '', leaseToken: '', leaseExpiresAt: '' } as const;

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
 *   re-checks the exact state it selected, so exactly one claimer wins.
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
      // Claim: due work of a handler in due order. Holds unfinished deliveries only.
      {
        key: { handler: 1, dueAt: 1, _id: 1 },
        name: 'claim',
        partialFilterExpression: { dueAt: { $exists: true } },
      },
      // Ordering: unfinished predecessors of a keyed delivery.
      {
        key: { handler: 1, key: 1, sequence: 1 },
        name: 'ordering',
        partialFilterExpression: { dueAt: { $exists: true }, sequence: { $exists: true } },
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
    const claimed: DeliveryDocument[] = [];
    // Keys that already have an in-flight delivery of ours, or the lowest sequence
    // found blocked per key. Every later candidate of the key is blocked as well.
    const claimedKeys = new Set<string>();
    const blockedFromSequence = new Map<string, number>();
    const candidates = this.#deliveries.find(
      { handler: { $in: [...request.handlers] }, dueAt: { $lte: request.now } },
      {
        sort: { dueAt: 1, _id: 1 },
        batchSize: Math.max(request.limit * 2, MINIMUM_CANDIDATE_BATCH_SIZE),
      },
    );
    try {
      for await (const candidate of candidates) {
        const leased = await this.#claimCandidate(
          candidate,
          request,
          claimedKeys,
          blockedFromSequence,
        );
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
    return this.#toClaimedDeliveries(claimed, request.leaseExpiresAt);
  }

  async extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, undefined, {
      $set: { leaseExpiresAt, dueAt: leaseExpiresAt, updatedAt: now },
    });
  }

  async complete(lease: LeaseReference, now: Date, session?: ClientSession): Promise<boolean> {
    return this.#updateLeased(lease, session, {
      $set: { status: 'completed', completedAt: now, updatedAt: now },
      $unset: { ...LEASE_FIELDS, dueAt: '' },
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
      $unset: { ...LEASE_FIELDS, dueAt: '' },
    });
  }

  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    const filter: Filter<DeliveryDocument> = {
      ...(query.status === undefined ? {} : { status: query.status }),
      ...(query.handler === undefined ? {} : { handler: query.handler }),
      ...(query.eventType === undefined ? {} : { eventType: query.eventType }),
      ...(query.eventId === undefined ? {} : { eventId: query.eventId }),
    };
    const documents = await this.#deliveries
      .find(filter, { sort: { createdAt: 1, _id: 1 }, limit: query.limit ?? DEFAULT_LIST_LIMIT })
      .toArray();
    const events = await this.#readEnvelopes(documents.map((document) => document.eventId));
    return documents.map((document) => toDeliveryRecord(document, events.get(document.eventId)!));
  }

  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    const result = await this.#deliveries.updateMany(
      { _id: { $in: [...deliveryIds] }, status: 'dead' },
      { $set: { status: 'pending', attempts: 0, availableAt: now, dueAt: now, updatedAt: now } },
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
    claimedKeys: Set<string>,
    blockedFromSequence: Map<string, number>,
  ): Promise<DeliveryDocument | undefined> {
    const { key, sequence } = candidate;
    const orderingId = key === undefined ? undefined : `${candidate.handler}\u0000${key}`;
    if (orderingId !== undefined) {
      const blockedFrom = blockedFromSequence.get(orderingId);
      if (claimedKeys.has(orderingId) || (blockedFrom !== undefined && sequence! >= blockedFrom)) {
        return undefined;
      }
      if (await this.#hasUnfinishedPredecessor(candidate.handler, key!, sequence!)) {
        blockedFromSequence.set(orderingId, Math.min(blockedFrom ?? sequence!, sequence!));
        return undefined;
      }
    }
    // The filter pins the exact state that was selected, so a competing claimer,
    // a finished lease or a reschedule since then makes this update match nothing.
    const leased = await this.#deliveries.findOneAndUpdate(
      {
        _id: candidate._id,
        status: candidate.status,
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
    if (leased !== null && orderingId !== undefined) {
      claimedKeys.add(orderingId);
    }
    return leased ?? undefined;
  }

  async #hasUnfinishedPredecessor(
    handler: string,
    key: string,
    sequence: number,
  ): Promise<boolean> {
    const predecessor = await this.#deliveries.findOne(
      { handler, key, sequence: { $lt: sequence }, dueAt: { $exists: true } },
      { projection: { _id: 1 } },
    );
    return predecessor !== null;
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
    return result.modifiedCount === 1;
  }

  async #purgeMatching(filter: Filter<DeliveryDocument>): Promise<PurgeResult> {
    let deletedDeliveries = 0;
    let deletedEvents = 0;
    for (;;) {
      const batch = await this.#deliveries
        .find(filter, { projection: { eventId: 1 }, limit: this.#purgeBatchSize })
        .toArray();
      if (batch.length === 0) {
        return { deletedDeliveries, deletedEvents };
      }
      // The filter is repeated so a delivery requeued since the read is kept.
      const deleted = await this.#deliveries.deleteMany({
        $and: [{ _id: { $in: batch.map((document) => document._id) } }, filter],
      });
      deletedDeliveries += deleted.deletedCount;
      deletedEvents += await this.#deleteOrphanedEvents(batch.map((document) => document.eventId));
      if (batch.length < this.#purgeBatchSize) {
        return { deletedDeliveries, deletedEvents };
      }
    }
  }

  /**
   * Deletes the given events that no delivery references. Runs in a transaction:
   * an append adding a delivery to the same event writes the event document, so
   * the two conflict and one restarts instead of leaving a delivery without event.
   */
  async #deleteOrphanedEvents(eventIds: readonly string[]): Promise<number> {
    const candidateIds = [...new Set(eventIds)];
    return this.transaction(async (session) => {
      const referenced = new Set(
        (
          await this.#deliveries
            .find({ eventId: { $in: candidateIds } }, { session, projection: { eventId: 1 } })
            .toArray()
        ).map((document) => document.eventId),
      );
      const orphanedIds = candidateIds.filter((eventId) => !referenced.has(eventId));
      const deleted = await this.#events.deleteMany({ _id: { $in: orphanedIds } }, { session });
      return deleted.deletedCount;
    });
  }
}

function deliveryPairKey(eventId: string, handler: string): string {
  return `${eventId}\u0000${handler}`;
}
