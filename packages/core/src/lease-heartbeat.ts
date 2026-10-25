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
 * The renewal state of one delivery's lease: the last expiry the store confirmed, and whether a
 * renewal is on its way.
 */
class LeaseRenewal {
  readonly #dependencies: HeartbeatDependencies;
  readonly #target: HeartbeatTarget;
  #confirmedExpiry: number;
  #renewing = false;

  constructor(dependencies: HeartbeatDependencies, target: HeartbeatTarget) {
    this.#dependencies = dependencies;
    this.#target = target;
    this.#confirmedExpiry = target.delivery.leaseExpiresAt.getTime();
  }

  /** One beat: aborts once the confirmed expiry passed, else renews unless a renewal is running. */
  beat(): void {
    const { store, clock, notifier, settings } = this.#dependencies;
    const now = clock.now();
    if (now.getTime() >= this.#confirmedExpiry) {
      this.#abortLeaseLost();
      return;
    }
    if (this.#renewing) {
      return;
    }
    this.#renewing = true;
    const expiry = now.getTime() + settings.leaseDurationMs;
    store
      .extendLease(this.#target.delivery, new Date(expiry), now)
      .then((extended) => {
        if (extended) {
          this.#confirmedExpiry = expiry;
        } else {
          this.#abortLeaseLost(true);
        }
      })
      .catch((error: unknown) => notifier.failure('extend-lease', error))
      .finally(() => {
        this.#renewing = false;
      });
  }

  #abortLeaseLost(storeConfirmed = false): void {
    const { delivery, controller } = this.#target;
    controller.abort(
      new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, { storeConfirmed }),
    );
  }
}

/**
 * Renews the lease every third of its duration. A worker that cannot renew
 * aborts its handler once the last confirmed expiry passes, because another
 * worker may then claim the delivery (self-fencing). Returns a function that stops it.
 */
export function startLeaseHeartbeat(
  dependencies: HeartbeatDependencies,
  target: HeartbeatTarget,
): () => void {
  const renewal = new LeaseRenewal(dependencies, target);
  const { clock, settings } = dependencies;
  return schedule(clock, {
    run: () => renewal.beat(),
    delayMs: Math.max(1, Math.floor(settings.leaseDurationMs / RENEWALS_PER_LEASE)),
    repeat: true,
    unref: true,
  });
}
