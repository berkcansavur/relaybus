import type { PurgeRequest, PurgeResult } from '@relaybus/core';
import type { ClientSession, Collection, Document, Filter } from 'mongodb';
import { runInTransaction, type MongoStoreState } from './mongo-store-state.js';

/** The fields purge reads from a delivery or a completion history record. */
interface PurgeableDocument {
  readonly _id: string;
  readonly eventId: string;
  readonly status?: string;
  readonly completedAt?: Date;
  readonly updatedAt?: Date;
}

/** Deletes old completed and dead deliveries and the events nothing references any more. */
export class MongoPurge {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /** Implements {@link MongoEventStore.purge}. */
  async purge(request: PurgeRequest): Promise<PurgeResult> {
    const total = { deletedDeliveries: 0, deletedEvents: 0 };
    const steps = this.#purgeSteps(request);
    const { completedBefore, deadBefore } = request;
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

  /** The purges the request asks for, in the order they run. */
  #purgeSteps(request: PurgeRequest): Array<() => Promise<PurgeResult>> {
    const steps: Array<() => Promise<PurgeResult>> = [];
    const { completedBefore, deadBefore } = request;
    if (completedBefore !== undefined) {
      steps.push(() =>
        this.#purgeMatching(this.#purgeable(this.#state.deliveries.collectionName), {
          filter: { status: 'completed', completedAt: { $lt: completedBefore } },
          completedBefore,
        }),
      );
      if (this.#state.completionHistory) {
        steps.push(() =>
          this.#purgeMatching(this.#purgeable(this.#state.completions.collectionName), {
            filter: { completedAt: { $lt: completedBefore } },
            completedBefore,
          }),
        );
      }
    }
    if (deadBefore !== undefined) {
      steps.push(() =>
        this.#purgeMatching(this.#purgeable(this.#state.deliveries.collectionName), {
          filter: { status: 'dead', updatedAt: { $lt: deadBefore } },
          completedBefore,
        }),
      );
    }
    return steps;
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
    {
      filter,
      completedBefore,
    }: { readonly filter: Filter<PurgeableDocument>; readonly completedBefore: Date | undefined },
  ): Promise<PurgeResult> {
    let deletedDeliveries = 0;
    let deletedEvents = 0;
    for (;;) {
      const step = await runInTransaction(this.#state, (session) =>
        this.#purgeBatch(collection, { filter, completedBefore, session }),
      );
      deletedDeliveries += step.deletedDeliveries;
      deletedEvents += step.deletedEvents;
      if (step.batchSize < this.#state.purgeBatchSize) {
        return { deletedDeliveries, deletedEvents };
      }
    }
  }

  /** One batch of {@link MongoPurge.#purgeMatching}, inside the caller's transaction. */
  async #purgeBatch(
    collection: Collection<PurgeableDocument>,
    {
      filter,
      completedBefore,
      session,
    }: {
      readonly filter: Filter<PurgeableDocument>;
      readonly completedBefore: Date | undefined;
      readonly session: ClientSession;
    },
  ): Promise<{ batchSize: number; deletedDeliveries: number; deletedEvents: number }> {
    const batch = await collection
      .find(filter, { session, projection: { eventId: 1 }, limit: this.#state.purgeBatchSize })
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
    const events = await this.#state.events.deleteMany(
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
  }

  /** A view of a collection that purge deletes from: deliveries or completion history records. */
  #purgeable(name: string): Collection<PurgeableDocument> {
    return this.#state.database.collection<PurgeableDocument>(name, {
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
      ...(await this.#state.deliveries.find(filter, options).toArray()),
      ...(this.#state.completionHistory
        ? await this.#state.completions.find(filter, options).toArray()
        : []),
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
      this.#state.deliveries.collectionName,
      ...(this.#state.completionHistory ? [this.#state.completions.collectionName] : []),
    ];
    let deletedEvents = 0;
    for (;;) {
      const step = await runInTransaction(this.#state, async (session) => {
        const orphans = await this.#state.events
          .aggregate<{ _id: string }>(
            orphanPipeline({
              cutoff,
              completedBefore,
              references,
              limit: this.#state.purgeBatchSize,
            }),
            { session },
          )
          .toArray();
        const deleted = await this.#state.events.deleteMany(
          { _id: { $in: orphans.map((orphan) => orphan._id) } },
          { session },
        );
        return { found: orphans.length, deleted: deleted.deletedCount };
      });
      deletedEvents += step.deleted;
      if (step.found < this.#state.purgeBatchSize) {
        return deletedEvents;
      }
    }
  }
}

/**
 * The aggregation that finds, oldest first, events without a reference in any of the `references`
 * collections: never-completed ones by age (`cutoff`), completed ones by their last completion.
 */
function orphanPipeline(input: {
  readonly cutoff: Date;
  readonly completedBefore: Date | undefined;
  readonly references: readonly string[];
  readonly limit: number;
}): Document[] {
  const { cutoff, completedBefore, references, limit } = input;
  return [
    {
      $match: {
        $or: [
          // Never completed (no subscribers, or every delivery dead): by age.
          { completedAt: { $exists: false }, createdAt: { $lt: cutoff } },
          // Completed: by the time of its last completion, and only under the completed cutoff.
          ...(completedBefore === undefined ? [] : [{ completedAt: { $lt: completedBefore } }]),
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
    { $limit: limit },
    { $project: { _id: 1 } },
  ];
}
