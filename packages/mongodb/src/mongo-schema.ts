import type { Document, IndexDescriptionInfo } from 'mongodb';
import { LANE_COUNT, laneOf } from './lanes.js';
import { createTransactionsUnsupportedError } from './mongo-errors.js';
import type { MongoStoreState } from './mongo-store-state.js';

const NAMESPACE_EXISTS = 48;

const INDEX_NOT_FOUND = 27;

/** Documents converted per write while `setup()` upgrades a 0.1.0 database. */
const UPGRADE_BATCH_SIZE = 1_000;

/** Key of the claim index. A 0.1.0 index named `claim` with another key is dropped by `setup()`. */
const CLAIM_INDEX_KEY = { laneId: 1, dueAt: 1, _id: 1, handler: 1 } as const;

/** Indexes of 0.1.0 that no query uses any more. */
const OBSOLETE_INDEXES = ['running'];

/** Fields that 0.1.0 stored while a delivery was leased. */
const LEASE_FIELDS = { leaseOwner: '', leaseToken: '', leaseExpiresAt: '' } as const;

/** Creates the collections and indexes, seeds the lanes and upgrades a 0.1.0 database. */
export class MongoSchema {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /** Implements {@link MongoEventStore.setup}. */
  async setup(): Promise<void> {
    const hello = await this.#state.client.db('admin').command({ hello: 1 });
    const supportsTransactions =
      typeof hello['setName'] === 'string' || hello['msg'] === 'isdbgrid';
    if (!supportsTransactions) {
      throw createTransactionsUnsupportedError(undefined);
    }
    // Created up front: collections created implicitly inside concurrent transactions conflict.
    const collectionNames = [
      this.#state.events.collectionName,
      this.#state.deliveries.collectionName,
      this.#state.keySequences.collectionName,
      this.#state.lanes.collectionName,
      ...(this.#state.completionHistory ? [this.#state.completions.collectionName] : []),
    ];
    for (const name of collectionNames) {
      await this.#createCollectionIfMissing(name);
    }
    if ((await this.#state.lanes.countDocuments()) < LANE_COUNT) {
      await this.#upgradeFromVersion010();
      await this.#seedLanes();
    }
    await this.#dropObsoleteIndexes();
    await this.#createIndexes();
  }

  /** Creates the indexes of every collection; existing indexes are left as they are. */
  async #createIndexes(): Promise<void> {
    await this.#state.deliveries.createIndexes([
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
    await this.#state.events.createIndexes([
      { key: { createdAt: 1, _id: 1 }, name: 'created' },
      // Purge of events whose deliveries all completed: they are kept until `completedBefore` passes this.
      {
        key: { completedAt: 1, _id: 1 },
        name: 'completed',
        partialFilterExpression: { completedAt: { $exists: true } },
      },
    ]);
    if (this.#state.completionHistory) {
      await this.#state.completions.createIndexes([
        { key: { completedAt: 1, _id: 1 }, name: 'completed' },
        { key: { eventId: 1 }, name: 'event' },
        { key: { createdAt: 1, _id: 1 }, name: 'created' },
      ]);
    }
  }

  async #createCollectionIfMissing(name: string): Promise<void> {
    try {
      await this.#state.database.createCollection(name);
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
    const legacy = this.#state.database.collection(this.#state.deliveries.collectionName);
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
      this.#state.deliveries.find(
        { laneId: { $exists: false } },
        { projection: { eventId: 1, key: 1 }, batchSize: UPGRADE_BATCH_SIZE },
      ),
      async (documents) => {
        await this.#state.deliveries.bulkWrite(
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
    await this.#upgradeEventsFromVersion010();
  }

  /** Tells every 0.1.0 event which handlers it already has deliveries for and when it was created. */
  async #upgradeEventsFromVersion010(): Promise<void> {
    await inBatches(
      this.#state.events.find(
        { handlers: { $exists: false } },
        { projection: { occurredAt: 1 }, batchSize: UPGRADE_BATCH_SIZE },
      ),
      async (events) => {
        const known = new Map(
          (
            await this.#state.deliveries
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
        await this.#state.events.bulkWrite(
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
    await this.#state.lanes.bulkWrite(
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
    for (const index of await this.#state.deliveries.indexes()) {
      if (isObsoleteIndex(index)) {
        await this.#dropIndexIfPresent(index.name!);
      }
    }
  }

  async #dropIndexIfPresent(name: string): Promise<void> {
    try {
      await this.#state.deliveries.dropIndex(name);
    } catch (error) {
      // Another process running setup() dropped it first.
      if (!hasErrorCode(error, INDEX_NOT_FOUND)) {
        throw error;
      }
    }
  }
}

function isObsoleteIndex(index: IndexDescriptionInfo): boolean {
  return (
    OBSOLETE_INDEXES.includes(index.name!) ||
    (index.name === 'claim' &&
      Object.keys(index.key).join() !== Object.keys(CLAIM_INDEX_KEY).join())
  );
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

function hasErrorCode(error: unknown, code: number): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === code;
}
