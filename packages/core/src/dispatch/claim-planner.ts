import type { HandlerRegistration } from '../handler-registry.js';
import type { InFlightDeliveries } from './in-flight-deliveries.js';

/** One claim of a round: the handlers it claims for and how many deliveries it may take. */
export interface ClaimPlan {
  readonly handlers: string[];
  readonly limit: number;
}

/**
 * Splits the free slots of a dispatcher into claims: one per handler with a concurrency limit and
 * room left, then one for every handler without a limit. A round never asks for more than a batch.
 */
export class ClaimPlanner {
  readonly #inFlight: InFlightDeliveries;
  readonly #batchSize: number;

  constructor(inFlight: InFlightDeliveries, batchSize: number) {
    this.#inFlight = inFlight;
    this.#batchSize = batchSize;
  }

  /** One claim for handlers without a concurrency limit, one per limited handler with room. */
  plan(registrations: readonly HandlerRegistration[], freeSlots: number): ClaimPlan[] {
    let free = Math.min(freeSlots, this.#batchSize);
    const plans: ClaimPlan[] = [];
    const unlimited = registrations.filter((registration) => registration.concurrency === null);
    for (const registration of registrations) {
      if (registration.concurrency === null || free === 0) {
        continue;
      }
      const room = registration.concurrency - this.#inFlight.countOf(registration);
      const limit = Math.min(room, free);
      if (limit > 0) {
        plans.push({ handlers: [...registration.claimIds], limit });
        free -= limit;
      }
    }
    if (unlimited.length > 0 && free > 0) {
      plans.push({
        handlers: unlimited.flatMap((registration) => registration.claimIds),
        limit: free,
      });
    }
    return plans;
  }
}
