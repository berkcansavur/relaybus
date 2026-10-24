import type { AppendEntry, AppendResult, EventEnvelope } from '@relaybus/core';
import type { ClientSession } from 'mongodb';
import { toDeliveryDocument, toEventDocument, type DeliveryDocument } from './documents.js';
import { assertInTransaction, type MongoStoreState } from './mongo-store-state.js';

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
    const storedEvents = new Map(
      (
        await this.#state.events
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
    return { appendedEventIds, duplicateEventIds, appendedDeliveryCount: newDeliveries.length };
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
