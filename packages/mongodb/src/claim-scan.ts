/** What one claim call has learned about the deliveries of a single handler and key. */
interface KeyKnowledge {
  /** Lowest sequence known to be unfinished. Every later delivery of the key must wait. */
  openSequence: number | undefined;
  /** Pending deliveries that waited behind `openSequence`, candidates for parking. */
  waiting: string[];
  /** Lane of the key (all deliveries of a key share one). Undefined until a delivery waited. */
  laneId: number | undefined;
}

/**
 * Bookkeeping of one claim call. A claim scans due deliveries in due order and
 * meets many deliveries of the same key; what it learns about a key from the
 * database once (its lowest unfinished delivery) answers every later delivery
 * of that key without another query.
 */
export class ClaimScan {
  readonly #keys = new Map<string, KeyKnowledge>();

  /** Whether an unfinished delivery this scan knows of comes before the delivery with `sequence`. */
  waits(orderingId: string, sequence: number): boolean {
    const known = this.#keys.get(orderingId);
    return known?.openSequence !== undefined && sequence > known.openSequence;
  }

  /** Whether this scan already learned anything about the key. */
  knows(orderingId: string): boolean {
    return this.#keys.has(orderingId);
  }

  /** Records an unfinished delivery of the key with this sequence. */
  noteOpen(orderingId: string, sequence: number): void {
    const known = this.#know(orderingId);
    known.openSequence = Math.min(known.openSequence ?? sequence, sequence);
  }

  /** Remembers a pending delivery of lane `laneId` that waited behind an unfinished predecessor. */
  noteWaiting(orderingId: string, deliveryId: string, laneId: number): void {
    const known = this.#know(orderingId);
    known.waiting.push(deliveryId);
    known.laneId = laneId;
  }

  /**
   * Ids of the waiting deliveries of every key that accumulated at least
   * `minimum` of them, or that lives in one of `handedBack`. Shorter queues
   * of a lane that stays held remain in the claim index: moving them out and
   * back would cost more than skipping them. In a lane handed back they are
   * all its due work, and leaving them ready would make every later claim take
   * the lane for nothing.
   */
  waitingBacklogs(minimum: number, handedBack: ReadonlySet<number>): string[] {
    return [...this.#keys.values()]
      .filter((known) => known.waiting.length >= minimum || handedBack.has(known.laneId!))
      .flatMap((known) => known.waiting);
  }

  #know(orderingId: string): KeyKnowledge {
    let known = this.#keys.get(orderingId);
    if (known === undefined) {
      known = { openSequence: undefined, waiting: [], laneId: undefined };
      this.#keys.set(orderingId, known);
    }
    return known;
  }
}

/** Identity of the ordering of a handler and key. */
export function orderingIdentity(handler: string, key: string): string {
  return `${handler}\u0000${key}`;
}
