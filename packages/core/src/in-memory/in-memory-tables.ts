import type { EventEnvelope } from '../event.js';
import type { SerializedEnvelope, StoredDelivery, StoredEvent } from './stored-records.js';

/** The rows of the in-memory store: events, deliveries and the sequence counter of each key. */
export class InMemoryTables {
  readonly events = new Map<string, StoredEvent>();
  readonly deliveries = new Map<string, StoredDelivery>();
  readonly #keySequences = new Map<string, number>();

  /** The next sequence of `key`; null for an unkeyed event. */
  nextSequence(key: string | null): number | null {
    if (key === null) {
      return null;
    }
    const next = (this.#keySequences.get(key) ?? 0) + 1;
    this.#keySequences.set(key, next);
    return next;
  }

  /** A fresh copy of a stored event, as a database would read it back. */
  readEnvelope(eventId: string): EventEnvelope {
    const serialized = this.events.get(eventId)!.envelope;
    const copy = JSON.parse(JSON.stringify(serialized)) as SerializedEnvelope;
    return { ...copy, occurredAt: new Date(copy.occurredAt) };
  }
}
