import type { ClaimedDelivery } from '../event-store.js';
import { schedule, type CancelTimer } from '../internal/timers.js';
import type { ShutdownDeadline } from '../shutdown-deadline.js';
import { DeliveryLeaseIdentity } from '../values/delivery-lease-identity.js';
import type { DispatcherDependencies, ShutdownLatch } from './dispatch-context.js';

/**
 * A change of a delivery's state (complete, reschedule, bury, release) that the store did not answer.
 * It stays in the lane's in-flight list, and other deliveries of the lane keep renewing the lane
 * lease, so nothing else would ever recover it: the dispatcher retries it until the store applies it
 * (`settle` resolves true) or says the lease is gone (false).
 */
export interface PendingSettlement {
  readonly delivery: ClaimedDelivery;
  readonly settle: () => Promise<boolean>;
  /** Reports the outcome that could not be reported when the store failed. */
  readonly applied: () => void;
}

/** Settlements are pending per claim: an older claim of a delivery must never replace or remove the newer one's. */
function pendingKeyOf(delivery: ClaimedDelivery): string {
  return DeliveryLeaseIdentity.of(delivery.deliveryId, delivery.leaseToken);
}

/** Retries, every poll interval, the settlements the store failed to apply, until it answers. */
export class SettlementRecovery {
  readonly #dependencies: DispatcherDependencies;
  readonly #shutdown: ShutdownLatch;
  readonly #pending = new Map<string, PendingSettlement>();
  #recoveryTimer: CancelTimer | undefined;
  #recovery: Promise<void> | undefined;

  constructor(dependencies: DispatcherDependencies, shutdown: ShutdownLatch) {
    this.#dependencies = dependencies;
    this.#shutdown = shutdown;
  }

  retain(entry: PendingSettlement): void {
    if (this.#shutdown.abandoned) {
      return;
    }
    this.#pending.set(pendingKeyOf(entry.delivery), entry);
    this.#recoveryTimer ??= schedule(this.#dependencies.clock, {
      run: () => void this.#recoverSettlements(),
      delayMs: this.#dependencies.settings.pollIntervalMs,
      repeat: true,
      unref: true,
    });
  }

  /** A new claim of this delivery makes whatever was pending for its earlier claims moot. */
  forgetEarlierClaims(delivery: ClaimedDelivery): void {
    for (const key of [...this.#pending.keys()].filter((pending) =>
      pending.startsWith(DeliveryLeaseIdentity.prefixOf(delivery.deliveryId)),
    )) {
      this.#pending.delete(key);
    }
  }

  /**
   * Gives the settlements still pending one last try and forgets them. A pass of the timer may be inside the
   * store: it ends before the last pass starts. A store that does not answer before the deadline gets no
   * further call, since it may be closed right after.
   */
  async close(deadline: ShutdownDeadline): Promise<void> {
    this.#stopTimer();
    const lastTry = (async () => {
      await this.#recovery;
      await this.#recoverSettlements();
    })();
    await deadline.race(lastTry);
    this.#pending.clear();
  }

  /** Stop gave up: forgets every settlement still to retry, which may reach a closed store. */
  abandon(): void {
    this.#stopTimer();
    this.#pending.clear();
  }

  #stopTimer(): void {
    this.#recoveryTimer?.();
    this.#recoveryTimer = undefined;
  }

  /**
   * Retries every pending settlement once; a pass that is already running is the one returned. A store that
   * still fails leaves the settlement for the next attempt (it was reported when it first failed).
   */
  #recoverSettlements(): Promise<void> {
    this.#recovery ??= this.#recoverAll().finally(() => {
      this.#recovery = undefined;
    });
    return this.#recovery;
  }

  async #recoverAll(): Promise<void> {
    for (const [key, entry] of [...this.#pending]) {
      // Shutdown gave up on this one (the pending map was cleared): the store may be closed by now.
      if (this.#pending.get(key) !== entry) {
        continue;
      }
      try {
        const applied = await entry.settle();
        this.#pending.delete(pendingKeyOf(entry.delivery));
        if (applied) {
          entry.applied();
        } else {
          this.#dependencies.notifier.notify('onLeaseLost', { delivery: entry.delivery });
        }
      } catch {
        // Still down: the next tick tries again.
      }
    }
  }
}
