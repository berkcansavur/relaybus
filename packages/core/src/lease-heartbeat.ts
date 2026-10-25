import type { Clock } from './clock.js';
import { LeaseLostError } from './errors.js';
import type { ClaimedDelivery, EventStore } from './event-store.js';
import { schedule } from './internal/timers.js';
import type { Notifier } from './notifier.js';

/** Lease renewals per lease duration. */
const RENEWALS_PER_LEASE = 3;

/** What the heartbeat needs of the dispatcher. */
type HeartbeatDependencies = {
  readonly store: EventStore;
  readonly clock: Clock;
  readonly notifier: Notifier;
  readonly settings: { readonly leaseDurationMs: number };
};

/** The delivery whose lease is kept and the controller that aborts its handler. */
type HeartbeatTarget = {
  readonly delivery: ClaimedDelivery;
  readonly controller: AbortController;
};

/**
 * Renews the lease every third of its duration. A worker that cannot renew
 * aborts its handler once the last confirmed expiry passes, because another
 * worker may then claim the delivery (self-fencing). Returns a function that stops it.
 */
export function startLeaseHeartbeat(
  { store, clock, notifier, settings }: HeartbeatDependencies,
  { delivery, controller }: HeartbeatTarget,
): () => void {
  let confirmedExpiry = delivery.leaseExpiresAt.getTime();
  let renewing = false;
  const abortLeaseLost = (storeConfirmed = false): void => {
    controller.abort(
      new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, { storeConfirmed }),
    );
  };
  return schedule(clock, {
    run: () => {
      const now = clock.now();
      if (now.getTime() >= confirmedExpiry) {
        abortLeaseLost();
        return;
      }
      if (renewing) {
        return;
      }
      renewing = true;
      const expiry = now.getTime() + settings.leaseDurationMs;
      store
        .extendLease(delivery, new Date(expiry), now)
        .then((extended) => {
          if (extended) {
            confirmedExpiry = expiry;
          } else {
            abortLeaseLost(true);
          }
        })
        .catch((error: unknown) => notifier.failure('extend-lease', error))
        .finally(() => {
          renewing = false;
        });
    },
    delayMs: Math.max(1, Math.floor(settings.leaseDurationMs / RENEWALS_PER_LEASE)),
    repeat: true,
    unref: true,
  });
}
