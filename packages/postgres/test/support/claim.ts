import type { ClaimedDelivery } from '@relaybus/core';
import type { PostgresEventStore } from '../../src/index.js';

export const LEASE_MS = 30_000;

/** Claims for `owner` with a lease of {@link LEASE_MS} from `now`. */
export function claimWithLease(
  store: PostgresEventStore,
  options: {
    readonly handlers: readonly string[];
    readonly limit: number;
    readonly owner: string;
    readonly now: Date;
  },
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers: options.handlers,
    limit: options.limit,
    leaseOwner: options.owner,
    leaseExpiresAt: new Date(options.now.getTime() + LEASE_MS),
    now: options.now,
  });
}
