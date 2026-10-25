import type { ClaimedDelivery } from '../event-store.js';
import { schedule, type CancelTimer } from '../internal/timers.js';
import type { ClaimPlanner } from './claim-planner.js';
import type { DispatcherDependencies, ShutdownLatch } from './dispatch-context.js';
import type { InFlightDeliveries, InFlightDelivery } from './in-flight-deliveries.js';

/**
 * Shortest time between the start of a claim and a claim caused by a wake-up (a publish, a notification
 * from the store). The first wake-up after a quiet period claims at once; wake-ups that follow quickly
 * are merged into one claim at the end of this time, so a stream of single publishes costs a claim per
 * few milliseconds instead of a claim per event.
 */
const WAKE_COOLDOWN_MS = 5;

/**
 * Longest a free slot waits for others to free up before it is refilled alone. Fast handlers free a group
 * of slots well within it, so their claims stay large; a slot of a slow handler stays empty for at most
 * this long.
 */
const QUIET_REFILL_MS = 10;

/**
 * Share of a batch (or of the concurrency, when smaller) that must be free before settled deliveries
 * trigger a claim. Measured with the claim-efficiency design test: at 0.5 a backlog drains with about
 * 14 claims per 100 deliveries, at 0.7 with 11 and fewer completion calls too.
 */
const REFILL_FRACTION = 0.7;

/** What one round of claims asked for and got. */
interface ClaimRound {
  readonly requested: number;
  readonly claimed: number;
}

/** What the loop needs from the dispatcher. */
export type ClaimLoopParts = {
  readonly dependencies: DispatcherDependencies;
  readonly inFlight: InFlightDeliveries;
  readonly planner: ClaimPlanner;
  readonly shutdown: ShutdownLatch;
  /** Starts running a claimed delivery. */
  readonly start: (delivery: ClaimedDelivery) => InFlightDelivery;
};

/**
 * Decides when to claim: after a wake-up (merged over a short cooldown), when a group of slots
 * freed up, when a lone slot stayed free for a while, or at the poll interval. Claims for every
 * handler with room and starts what it got.
 */
export class ClaimLoop {
  readonly #parts: ClaimLoopParts;
  #running = false;
  #wakeUp: (() => void) | undefined;
  /** Free slots the claim the loop was woken for needs at least; undefined while nobody woke it. */
  #signalledMinimum: number | undefined;
  /** Free slots a claim needs unless the poll interval or the quiet refill time passed: claims refill in groups. */
  readonly #refillThreshold: number;
  /** Handlers whose last claim got less than it asked for: the store has nothing more for them right now. */
  readonly #exhaustedHandlers = new Set<string>();
  /** Grows with every wake-up: a claim that started before one must not mark a handler exhausted. */
  #wakeGeneration = 0;
  #lastClaimStartedAt = Number.NEGATIVE_INFINITY;
  #cooldownTimer: CancelTimer | undefined;
  #refillScheduled = false;
  #quietRefillTimer: CancelTimer | undefined;

  constructor(parts: ClaimLoopParts) {
    this.#parts = parts;
    const { concurrency, batchSize } = parts.dependencies.settings;
    this.#refillThreshold = Math.ceil(Math.min(concurrency, batchSize) * REFILL_FRACTION);
  }

  get isRunning(): boolean {
    return this.#running;
  }

  /** Marks the loop running; {@link ClaimLoop.run} then claims until {@link ClaimLoop.halt}. */
  open(): void {
    this.#running = true;
  }

  /** Stops claiming: the running pass ends at its next wait. */
  halt(): void {
    this.#running = false;
  }

  /** Cancels the claims waiting for a timer and ends the current wait, so a halted loop returns. */
  interrupt(): void {
    this.#cooldownTimer?.();
    this.#cooldownTimer = undefined;
    this.#quietRefillTimer?.();
    this.#quietRefillTimer = undefined;
    this.#signal(this.#refillThreshold);
  }

  /**
   * Asks for a claim soon, for example right after a publish: at once after a quiet period, otherwise
   * at the end of a short cooldown that merges the wake-ups arriving meanwhile into one claim.
   */
  wake(): void {
    // New work may have arrived for handlers whose last claim found nothing.
    this.#exhaustedHandlers.clear();
    this.#wakeGeneration += 1;
    const wait = this.#lastClaimStartedAt + WAKE_COOLDOWN_MS - Date.now();
    if (wait <= 0) {
      this.#requestClaim();
      return;
    }
    this.#cooldownTimer ??= schedule(this.#parts.dependencies.clock, {
      run: () => {
        this.#cooldownTimer = undefined;
        this.#requestClaim();
      },
      delayMs: wait,
    });
  }

  /**
   * Claims until halted. A claim that filled its batch is followed by the next one at once while enough
   * slots are free. Otherwise the loop sleeps until a wake-up (a publish, a store notification, or enough
   * deliveries settling to refill a group of slots) or the poll interval. Only the poll interval claims
   * for fewer free slots than the refill threshold, so a busy dispatcher never claims slot by slot.
   */
  async run(): Promise<void> {
    let minimumFree = 1;
    while (this.#running) {
      let round: ClaimRound = { requested: 0, claimed: 0 };
      if (this.#freeSlots() >= minimumFree) {
        try {
          round = await this.claimAndStart();
        } catch (error) {
          this.#parts.dependencies.notifier.failure('claim', error);
        }
      }
      if (
        round.requested > 0 &&
        round.claimed === round.requested &&
        this.#freeSlots() >= this.#refillThreshold
      ) {
        minimumFree = this.#refillThreshold;
        continue;
      }
      minimumFree = await this.#waitForWakeUp();
      if (this.#freeSlots() < minimumFree) {
        // Woken for more free slots than there are now: ask again, so the wake-up is not lost.
        this.#requestClaim();
      }
    }
  }

  /**
   * A delivery settled. Its slot is worth refilling when the store may have more for its handler: a keyed
   * delivery unblocks the next one of its key, a delivery that did not succeed may be due again at once
   * (a retry without delay, a release), and a handler whose last claim was full may have a backlog.
   */
  onSettled(delivery: ClaimedDelivery, succeeded: boolean): void {
    if (
      delivery.event.key !== null ||
      !succeeded ||
      !this.#exhaustedHandlers.has(delivery.handler)
    ) {
      this.#requestClaim();
    }
  }

  /** Claims for every handler with room and starts what was claimed. */
  async claimAndStart(): Promise<ClaimRound & { readonly started: InFlightDelivery[] }> {
    const { dependencies, planner, shutdown, start } = this.#parts;
    const { store, registry, clock, settings, observation } = dependencies;
    const started: InFlightDelivery[] = [];
    let requested = 0;
    for (const request of planner.plan(registry.all(), this.#freeSlots())) {
      const now = clock.now();
      const wakeGeneration = this.#wakeGeneration;
      this.#lastClaimStartedAt = Date.now();
      this.#quietRefillTimer?.();
      this.#quietRefillTimer = undefined;
      requested += request.limit;
      const claim = {
        handlers: request.handlers,
        limit: request.limit,
        leaseOwner: settings.owner,
        leaseExpiresAt: new Date(now.getTime() + settings.leaseDurationMs),
        now,
      };
      const claimed = await (observation === undefined
        ? store.claim(claim)
        : observation.claim(store, claim));
      // A wake-up that arrived while the claim ran may have brought what this claim could not see.
      this.#markExhausted(
        request.handlers,
        claimed.length < request.limit && wakeGeneration === this.#wakeGeneration,
      );
      // A claim that answers after a stop gave up is left to lease expiry: the store may be closed.
      for (const delivery of shutdown.abandoned ? [] : claimed) {
        started.push(start(delivery));
      }
    }
    return { requested, claimed: started.length, started };
  }

  #markExhausted(handlers: readonly string[], exhausted: boolean): void {
    for (const handler of handlers) {
      if (exhausted) {
        this.#exhaustedHandlers.add(handler);
      } else {
        this.#exhaustedHandlers.delete(handler);
      }
    }
  }

  /** Ends the current wait of the claim loop, or the next one at once, for a claim once `minimumFree` slots are free. */
  #signal(minimumFree: number): void {
    this.#signalledMinimum = Math.min(this.#signalledMinimum ?? minimumFree, minimumFree);
    this.#wakeUp?.();
  }

  /** Waits for a wake-up or the poll interval. Resolves with the free slots the next claim needs. */
  async #waitForWakeUp(): Promise<number> {
    if (this.#signalledMinimum === undefined) {
      await new Promise<void>((resolve) => {
        const cancel = schedule(this.#parts.dependencies.clock, {
          run: () => {
            this.#signalledMinimum = 1;
            resolve();
          },
          delayMs: this.#parts.dependencies.settings.pollIntervalMs,
        });
        this.#wakeUp = () => {
          cancel();
          resolve();
        };
      });
      this.#wakeUp = undefined;
    }
    const minimumFree = this.#signalledMinimum!;
    this.#signalledMinimum = undefined;
    return minimumFree;
  }

  /**
   * Asks the loop for a claim. Once a group of slots is free the claim waits only for the deliveries
   * settling in the same tick, so they are claimed for together; fewer free slots are refilled when no
   * claim started for a short while. With no free slot the next settlement asks again.
   */
  #requestClaim(): void {
    const free = this.#freeSlots();
    if (free >= this.#refillThreshold) {
      if (!this.#refillScheduled) {
        this.#refillScheduled = true;
        setImmediate(() => {
          this.#refillScheduled = false;
          this.#signal(this.#refillThreshold);
        });
      }
    } else if (free > 0) {
      this.#quietRefillTimer ??= schedule(this.#parts.dependencies.clock, {
        run: () => {
          this.#quietRefillTimer = undefined;
          this.#signal(1);
        },
        delayMs: Math.max(0, this.#lastClaimStartedAt + QUIET_REFILL_MS - Date.now()),
      });
    }
  }

  #freeSlots(): number {
    return this.#parts.dependencies.settings.concurrency - this.#parts.inFlight.size;
  }
}
