import type { Clock } from '../clock.js';
import { schedule, type CancelTimer } from '../internal/timers.js';

/**
 * Delays after the latest publish at which the dispatcher is woken. A publish in a transaction of the
 * caller's commits after `publish` returns, at a time the bus cannot see: most commits land before the
 * first wake-up, slow ones before one of the later ones. A commit later than the last waits for the
 * poll, unless the caller calls `bus.wake()` or the store reports it (`watchNewDeliveries`).
 */
const FIRST_WAKE_MS = 25;
const SECOND_WAKE_MS = 100;
const LAST_WAKE_MS = 250;
const WAKE_DELAYS_MS: readonly number[] = [FIRST_WAKE_MS, SECOND_WAKE_MS, LAST_WAKE_MS];

export interface PublishWakeUpsDependencies {
  readonly clock: Clock;
  readonly wake: () => void;
}

/**
 * Wakes the dispatcher {@link WAKE_DELAYS_MS} after the latest publish, with one timer at a time: a
 * publish while the timer runs moves the later wake-ups to its own time, and the timer fires 25 ms
 * after it at the latest.
 */
export class PublishWakeUps {
  readonly #dependencies: PublishWakeUpsDependencies;
  #lastPublishAt = 0;
  #cancel: CancelTimer | undefined;
  /** When the pending timer fires. */
  #dueAt = 0;

  constructor(dependencies: PublishWakeUpsDependencies) {
    this.#dependencies = dependencies;
  }

  /**
   * Wakes the dispatcher for a publish: at once when its transaction has `committed` (one of relaybus'
   * own), else at the wake-ups after it.
   */
  afterPublish(committed: boolean): void {
    if (committed) {
      this.#dependencies.wake();
      return;
    }
    this.#lastPublishAt = this.#dependencies.clock.now().getTime();
    // A later wake-up still pending (100 or 250 ms after an earlier publish) must not delay this one.
    if (this.#cancel === undefined || this.#dueAt > this.#lastPublishAt + FIRST_WAKE_MS) {
      this.cancel();
      this.#cancel = this.#arm(FIRST_WAKE_MS);
    }
  }

  cancel(): void {
    this.#cancel?.();
    this.#cancel = undefined;
  }

  #fire(): void {
    this.#cancel = undefined;
    this.#dependencies.wake();
    const elapsed = this.#dependencies.clock.now().getTime() - this.#lastPublishAt;
    const next = WAKE_DELAYS_MS.find((delay) => delay > elapsed);
    if (next !== undefined) {
      this.#cancel = this.#arm(next - elapsed);
    }
  }

  #arm(delayMs: number): CancelTimer {
    this.#dueAt = this.#dependencies.clock.now().getTime() + delayMs;
    return schedule(this.#dependencies.clock, {
      run: () => this.#fire(),
      delayMs,
      unref: true,
    });
  }
}
