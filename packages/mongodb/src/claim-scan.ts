import type { DeliveryDocument } from './documents.js';

/** What one claim call has learned about the deliveries of a single handler and key. */
interface KeyKnowledge {
  /** Lowest sequence known to be unfinished. Every later delivery of the key must wait. */
  openSequence: number | undefined;
  /** Whether some delivery of the key holds a live lease, so none may start. */
  leaseHeld: boolean;
  /** Pending deliveries that waited behind `openSequence`, candidates for parking. */
  waiting: string[];
}

export type KeyVerdict = 'open-predecessor' | 'live-lease';

/**
 * Bookkeeping of one claim call. A claim scans due deliveries in due order and
 * meets many deliveries of the same key; whatever it learns about a key from
 * the database once (an unfinished predecessor, a live lease, its own claim)
 * answers every later delivery of that key without another query.
 */
export class ClaimScan {
  readonly #keys = new Map<string, KeyKnowledge>();

  /** Why the delivery must wait according to what this scan already knows, if it does. */
  verdict(orderingId: string, sequence: number): KeyVerdict | undefined {
    const known = this.#keys.get(orderingId);
    if (known?.openSequence !== undefined && sequence > known.openSequence) {
      return 'open-predecessor';
    }
    return known?.leaseHeld === true ? 'live-lease' : undefined;
  }

  /** Records an unfinished delivery of the key with this sequence. */
  noteOpen(orderingId: string, sequence: number): void {
    const known = this.#know(orderingId);
    known.openSequence = Math.min(known.openSequence ?? sequence, sequence);
  }

  /** Records that this scan leased the delivery, which is now unfinished and holds a live lease. */
  noteClaimed(orderingId: string, sequence: number): void {
    this.noteOpen(orderingId, sequence);
    this.#know(orderingId).leaseHeld = true;
  }

  /** Records that a live lease of the key blocks every delivery of it. */
  noteLeaseHeld(orderingId: string): void {
    this.#know(orderingId).leaseHeld = true;
  }

  /** Remembers a delivery that waited behind an unfinished predecessor. */
  noteWaiting(orderingId: string, delivery: DeliveryDocument): void {
    if (delivery.status === 'pending') {
      this.#know(orderingId).waiting.push(delivery._id);
    }
  }

  /**
   * Ids of the waiting deliveries of every key that accumulated at least
   * `minimum` of them. Shorter queues stay in the claim index: moving them out
   * and back would cost more than skipping them.
   */
  waitingBacklogs(minimum: number): string[] {
    return [...this.#keys.values()]
      .filter((known) => known.waiting.length >= minimum)
      .flatMap((known) => known.waiting);
  }

  #know(orderingId: string): KeyKnowledge {
    let known = this.#keys.get(orderingId);
    if (known === undefined) {
      known = { openSequence: undefined, leaseHeld: false, waiting: [] };
      this.#keys.set(orderingId, known);
    }
    return known;
  }
}

/** Identity of the ordering lane of a handler and key. */
export function orderingIdentity(handler: string, key: string): string {
  return `${handler}\u0000${key}`;
}
