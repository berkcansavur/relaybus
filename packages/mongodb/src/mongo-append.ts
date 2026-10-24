import type { AppendEntry, AppendResult, EventEnvelope } from '@relaybus/core';
import type { ClientSession, WithId } from 'mongodb';
import {
  toDeliveryDocument,
  toEventDocument,
  type DeliveryDocument,
  type EventDocument,
} from './documents.js';
import { assertInTransaction, type MongoStoreState } from './mongo-store-state.js';

/** The events of an append that already existed, with what the append needs of them. */
type StoredEvents = Map<string, WithId<EventDocument>>;

type PlanAppendInput = {
  readonly session: ClientSession;
  readonly entries: readonly AppendEntry[];
  readonly storedEvents: StoredEvents;
  readonly now: Date;
};

/** What an append writes and reports. */
type AppendWritePlan = {
  readonly appendedEventIds: string[];
  readonly duplicateEventIds: string[];
  readonly newEvents: ReadonlyArray<{
    readonly event: EventEnvelope;
    readonly sequence: number | null;
  }>;
  readonly newDeliveries: DeliveryDocument[];
  /** Handlers this call adds, per event. */
  readonly addedHandlers: Map<string, string[]>;
};

type WriteAppendInput = {
  readonly session: ClientSession;
  readonly plan: AppendWritePlan;
  readonly storedEvents: StoredEvents;
  readonly now: Date;
};

/** Appends events and their deliveries inside the caller's transaction. */
export class MongoAppend {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /** Implements {@link MongoEventStore.append}. */
  async append(
    session: ClientSession,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    assertInTransaction(session, 'append');
    const eventIds = [...new Set(entries.map((entry) => entry.event.id))];
    const storedEvents: StoredEvents = new Map(
      (
        await this.#state.events
          .find(
            { _id: { $in: eventIds } },
            { session, projection: { key: 1, sequence: 1, handlers: 1 } },
          )
          .toArray()
      ).map((stored) => [stored._id, stored]),
    );

    const plan = await this.#planAppend({ session, entries, storedEvents, now });
    await this.#writeAppend({ session, plan, storedEvents, now });
    return {
      appendedEventIds: plan.appendedEventIds,
      duplicateEventIds: plan.duplicateEventIds,
      appendedDeliveryCount: plan.newDeliveries.length,
    };
  }

  /**
   * Decides, in entry order, which events are new (taking their key sequence) and which deliveries
   * each event still lacks.
   */
  async #planAppend(input: PlanAppendInput): Promise<AppendWritePlan> {
    const { session, entries, storedEvents, now } = input;
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
    return { appendedEventIds, duplicateEventIds, newEvents, newDeliveries, addedHandlers };
  }

  /** Writes the new events, the handlers added to existing events and the new deliveries, in that order. */
  async #writeAppend(input: WriteAppendInput): Promise<void> {
    const { session, plan, storedEvents, now } = input;
    const { newEvents, newDeliveries, addedHandlers } = plan;
    if (newEvents.length > 0) {
      // Upserts, not inserts: a duplicate-key error would abort the whole transaction,
      // whereas a concurrent writer of the same id surfaces as a retryable write conflict.
      await this.#state.events.bulkWrite(
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
      await this.#state.events.bulkWrite(
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
      await this.#state.deliveries.bulkWrite(
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
  }

  /** Atomically increments the counter of `key`; conflicting appends of the key serialize here. */
  async #nextSequence(key: string, session: ClientSession): Promise<number> {
    const counter = await this.#state.keySequences.findOneAndUpdate(
      { _id: key },
      { $inc: { value: 1 } },
      { upsert: true, returnDocument: 'after', session },
    );
    return counter!.value;
  }
}
