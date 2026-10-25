import type { HandlerRegistration } from '../handler-registry.js';

/** A delivery whose handler runs: what aborts it and what settles when it is done. */
export interface InFlightDelivery {
  readonly handler: string;
  readonly controller: AbortController;
  readonly settled: Promise<void>;
}

/** The deliveries a dispatcher runs right now: its concurrency budget and its shutdown list. */
export class InFlightDeliveries {
  readonly #deliveries = new Set<InFlightDelivery>();

  get size(): number {
    return this.#deliveries.size;
  }

  add(delivery: InFlightDelivery): void {
    this.#deliveries.add(delivery);
  }

  delete(delivery: InFlightDelivery): void {
    this.#deliveries.delete(delivery);
  }

  /** How many of the deliveries run a handler of `registration`. */
  countOf(registration: HandlerRegistration): number {
    let count = 0;
    for (const delivery of this.#deliveries) {
      if (registration.claimIds.includes(delivery.handler)) {
        count += 1;
      }
    }
    return count;
  }

  /** Settles once every delivery running now settled, whatever its outcome. */
  allSettled(): Promise<PromiseSettledResult<void>[]> {
    return Promise.allSettled([...this.#deliveries].map((delivery) => delivery.settled));
  }

  abortAll(reason: Error): void {
    for (const delivery of this.#deliveries) {
      delivery.controller.abort(reason);
    }
  }
}
