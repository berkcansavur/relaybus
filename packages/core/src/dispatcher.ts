import type { Clock } from './clock.js';
import { CompletionBatcher } from './completion-batcher.js';
import {
  DispatcherStoppedError,
  HandlerTimeoutError,
  LeaseLostError,
  AttemptsExhaustedError,
  toErrorInfo,
} from './errors.js';
import { validatePayload, type EventEnvelope } from './event.js';
import type { ClaimedDelivery, EventStore } from './event-store.js';
import { runInIsolatedScope } from './execution-scope.js';
import type { HandlerContext, HandlerRegistration, HandlerRegistry } from './handler-registry.js';
import type { Notifier } from './notifier.js';
import { computeRetryDelay, isRetryableError } from './retry-policy.js';

export interface DispatcherSettings {
  readonly owner: string;
  readonly concurrency: number;
  readonly batchSize: number;
  readonly pollIntervalMs: number;
  readonly leaseDurationMs: number;
  readonly shutdownTimeoutMs: number;
  /** Longest a completion waits to be written together with others. Absent or 0: written one by one. */
  readonly completionBatchWindowMs?: number;
  /** Completions written at once, at most. Used with `completionBatchWindowMs`. */
  readonly completionBatchSize?: number;
}

export interface DispatcherDependencies {
  readonly store: EventStore;
  readonly registry: HandlerRegistry;
  readonly clock: Clock;
  readonly random: () => number;
  readonly notifier: Notifier;
  readonly settings: DispatcherSettings;
  /** Waits between attempts to confirm a completion. Replaceable in tests. */
  readonly sleep?: (milliseconds: number) => Promise<void>;
}

/**
 * A change of a delivery's state (complete, reschedule, bury, release) that the store did not answer.
 * It stays in the lane's in-flight list, and other deliveries of the lane keep renewing the lane
 * lease, so nothing else would ever recover it: the dispatcher retries it until the store applies it
 * (`settle` resolves true) or says the lease is gone (false).
 */
interface PendingSettlement {
  readonly delivery: ClaimedDelivery;
  readonly settle: () => Promise<boolean>;
  /** Reports the outcome that could not be reported when the store failed. */
  readonly applied: () => void;
}

interface InFlightDelivery {
  readonly handler: string;
  readonly controller: AbortController;
  readonly settled: Promise<void>;
}

type HandlerOutcome = 'completed' | 'unconfirmed';

/** Delays between attempts to record a completion before leaving it to lease expiry. */
const FIRST_COMPLETION_RETRY_MS = 50;
const SECOND_COMPLETION_RETRY_MS = 250;
const LAST_COMPLETION_RETRY_MS = 1_000;
const COMPLETION_RETRY_DELAYS_MS = [
  FIRST_COMPLETION_RETRY_MS,
  SECOND_COMPLETION_RETRY_MS,
  LAST_COMPLETION_RETRY_MS,
] as const;
/** Lease renewals per lease duration. */
const RENEWALS_PER_LEASE = 3;

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

const defaultSleep = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

/** One run of a handler: the delivery, its validated event and the transaction it runs in, if any. */
type InvokeInput = {
  readonly registration: HandlerRegistration;
  readonly event: EventEnvelope;
  readonly delivery: ClaimedDelivery;
  readonly controller: AbortController;
  readonly transaction: unknown;
};

/**
 * Claims due deliveries and runs their handlers: bounded concurrency, lease
 * heartbeats with self-fencing, fenced completion, retries with backoff and
 * dead-lettering.
 */
export class Dispatcher {
  readonly #dependencies: DispatcherDependencies;
  readonly #sleep: (milliseconds: number) => Promise<void>;
  readonly #inFlight = new Set<InFlightDelivery>();
  readonly #completions: CompletionBatcher | undefined;
  readonly #pending = new Map<string, PendingSettlement>();
  #recoveryTimer: NodeJS.Timeout | undefined;
  #recovery: Promise<void> | undefined;
  #running = false;
  #loop: Promise<void> | undefined;
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
  #cooldownTimer: NodeJS.Timeout | undefined;
  #refillScheduled = false;
  #quietRefillTimer: NodeJS.Timeout | undefined;
  #unwatch: (() => Promise<void>) | undefined;
  #watching: Promise<void> | undefined;

  constructor(dependencies: DispatcherDependencies) {
    this.#dependencies = dependencies;
    this.#sleep = dependencies.sleep ?? defaultSleep;
    const { store, clock, settings } = dependencies;
    this.#refillThreshold = Math.ceil(
      Math.min(settings.concurrency, settings.batchSize) * REFILL_FRACTION,
    );
    const { completionBatchWindowMs: windowMs = 0, completionBatchSize: maxBatchSize = 1 } =
      settings;
    if (store.completeMany !== undefined && windowMs > 0) {
      this.#completions = new CompletionBatcher(
        { completeMany: (leases, now) => store.completeMany!(leases, now) },
        clock,
        { windowMs, maxBatchSize },
        () => this.#inFlight.size,
      );
    }
  }

  get isRunning(): boolean {
    return this.#running;
  }

  get inFlightCount(): number {
    return this.#inFlight.size;
  }

  /**
   * True while the store reports new deliveries to this dispatcher by itself (see
   * {@link EventStore.watchNewDeliveries}), from every process: a publisher then has no reason to wake it.
   */
  get watchesNewDeliveries(): boolean {
    return this.#unwatch !== undefined;
  }

  start(): void {
    if (this.#running) {
      return;
    }
    this.#running = true;
    this.#completions?.setClosing(false);
    this.#watching = this.#watchNewDeliveries();
    this.#loop = this.#runLoop();
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
    this.#cooldownTimer ??= setTimeout(() => {
      this.#cooldownTimer = undefined;
      this.#requestClaim();
    }, wait);
  }

  /** Ends the current wait of the claim loop, or the next one at once, for a claim once `minimumFree` slots are free. */
  #signal(minimumFree: number): void {
    this.#signalledMinimum = Math.min(this.#signalledMinimum ?? minimumFree, minimumFree);
    this.#wakeUp?.();
  }

  /**
   * Starts listening to the store's reports of new deliveries. Polling stays the safety net: a store
   * that cannot listen, or fails to, only costs latency, so a failure is reported and otherwise ignored.
   */
  async #watchNewDeliveries(): Promise<void> {
    const { store, notifier } = this.#dependencies;
    try {
      const unwatch = await store.watchNewDeliveries?.(() => this.wake());
      if (unwatch !== undefined && !this.#running) {
        await unwatch();
        return;
      }
      this.#unwatch = unwatch;
      if (unwatch !== undefined) {
        // Deliveries committed after the first claim and before listening began were reported to nobody.
        this.wake();
      }
    } catch (error) {
      notifier.failure('watch', error);
    }
  }

  /** Stops listening to the store's reports of new deliveries. */
  async #stopWatching(): Promise<void> {
    await this.#watching;
    const unwatch = this.#unwatch;
    this.#unwatch = undefined;
    try {
      await unwatch?.();
    } catch (error) {
      this.#dependencies.notifier.failure('watch', error);
    }
  }

  /**
   * Stops claiming, waits up to the shutdown timeout for running handlers,
   * then aborts the rest and releases their deliveries for another process.
   */
  async stop(): Promise<void> {
    if (!this.#running) {
      // Batches run by hand (`runOnce`) leave settlements to recover without ever starting the loop.
      await this.#closeRecovery(Date.now() + this.#dependencies.settings.shutdownTimeoutMs);
      return;
    }
    this.#running = false;
    this.#completions?.setClosing(true);
    clearTimeout(this.#cooldownTimer);
    this.#cooldownTimer = undefined;
    clearTimeout(this.#quietRefillTimer);
    this.#quietRefillTimer = undefined;
    this.#signal(this.#refillThreshold);
    const deadline = Date.now() + this.#dependencies.settings.shutdownTimeoutMs;
    await this.#waitUntil(this.#stopWatching(), deadline);
    await this.#waitUntil(this.#loop!, deadline);
    const settled = Promise.allSettled([...this.#inFlight].map((delivery) => delivery.settled));
    if ((await this.#waitUntil(settled, deadline)) === 'timeout') {
      for (const delivery of this.#inFlight) {
        delivery.controller.abort(new DispatcherStoppedError('relaybus dispatcher stopped'));
      }
      await settled;
    }
    await this.#closeRecovery(deadline);
    await this.#waitUntil(this.#relinquish(), deadline);
  }

  /**
   * Gives the settlements still pending one last try and forgets them. A pass of the timer may be inside the
   * store: it ends before the last pass starts. A store that does not answer before the deadline gets no
   * further call, since it may be closed right after.
   */
  async #closeRecovery(deadline: number): Promise<void> {
    clearInterval(this.#recoveryTimer);
    this.#recoveryTimer = undefined;
    const lastTry = (async () => {
      await this.#recovery;
      await this.#recoverSettlements();
    })();
    await this.#waitUntil(lastTry, deadline);
    this.#pending.clear();
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
        this.#pending.delete(pendingKey(entry.delivery));
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

  #retain(entry: PendingSettlement): void {
    this.#pending.set(pendingKey(entry.delivery), entry);
    this.#recoveryTimer ??= setInterval(
      () => void this.#recoverSettlements(),
      this.#dependencies.settings.pollIntervalMs,
    );
    this.#recoveryTimer.unref();
  }

  /** Gives lease groups the owner no longer works on back to the store, so other workers need not wait for expiry. */
  async #relinquish(): Promise<void> {
    const { store, clock, notifier, settings } = this.#dependencies;
    try {
      await store.relinquish?.(settings.owner, clock.now());
    } catch (error) {
      notifier.failure('relinquish', error);
    }
  }

  /** Claims one batch and resolves when every claimed delivery settled. Returns how many ran. */
  async processBatch(): Promise<number> {
    const { started } = await this.#claimAndStart();
    await Promise.allSettled(started.map((delivery) => delivery.settled));
    return started.length;
  }

  async #waitUntil(work: Promise<unknown>, deadline: number): Promise<'done' | 'timeout'> {
    let timer: NodeJS.Timeout | undefined;
    const timedOut = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), Math.max(0, deadline - Date.now()));
    });
    const outcome = await Promise.race([work.then(() => 'done' as const), timedOut]);
    clearTimeout(timer);
    return outcome;
  }

  /**
   * Claims until stopped. A claim that filled its batch is followed by the next one at once while enough
   * slots are free. Otherwise the loop sleeps until a wake-up (a publish, a store notification, or enough
   * deliveries settling to refill a group of slots) or the poll interval. Only the poll interval claims
   * for fewer free slots than the refill threshold, so a busy dispatcher never claims slot by slot.
   */
  async #runLoop(): Promise<void> {
    let minimumFree = 1;
    while (this.#running) {
      let round: ClaimRound = { requested: 0, claimed: 0 };
      if (this.#freeSlots() >= minimumFree) {
        try {
          round = await this.#claimAndStart();
        } catch (error) {
          this.#dependencies.notifier.failure('claim', error);
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

  /** Waits for a wake-up or the poll interval. Resolves with the free slots the next claim needs. */
  async #waitForWakeUp(): Promise<number> {
    if (this.#signalledMinimum === undefined) {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          this.#signalledMinimum = 1;
          resolve();
        }, this.#dependencies.settings.pollIntervalMs);
        this.#wakeUp = () => {
          clearTimeout(timer);
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
   * A delivery settled. Its slot is worth refilling when the store may have more for its handler: a keyed
   * delivery unblocks the next one of its key, a delivery that did not succeed may be due again at once
   * (a retry without delay, a release), and a handler whose last claim was full may have a backlog.
   */
  #onSettled(delivery: ClaimedDelivery, succeeded: boolean): void {
    if (
      delivery.event.key !== null ||
      !succeeded ||
      !this.#exhaustedHandlers.has(delivery.handler)
    ) {
      this.#requestClaim();
    }
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
      this.#quietRefillTimer ??= setTimeout(
        () => {
          this.#quietRefillTimer = undefined;
          this.#signal(1);
        },
        Math.max(0, this.#lastClaimStartedAt + QUIET_REFILL_MS - Date.now()),
      );
    }
  }

  #freeSlots(): number {
    return this.#dependencies.settings.concurrency - this.#inFlight.size;
  }

  /** Claims for every handler with room and starts what was claimed. */
  async #claimAndStart(): Promise<ClaimRound & { readonly started: InFlightDelivery[] }> {
    const { store, registry, clock, settings } = this.#dependencies;
    const started: InFlightDelivery[] = [];
    let requested = 0;
    for (const request of this.#planClaims(registry.all())) {
      const now = clock.now();
      const wakeGeneration = this.#wakeGeneration;
      this.#lastClaimStartedAt = Date.now();
      clearTimeout(this.#quietRefillTimer);
      this.#quietRefillTimer = undefined;
      requested += request.limit;
      const claimed = await store.claim({
        handlers: request.handlers,
        limit: request.limit,
        leaseOwner: settings.owner,
        leaseExpiresAt: new Date(now.getTime() + settings.leaseDurationMs),
        now,
      });
      for (const handler of request.handlers) {
        // A wake-up that arrived while the claim ran may have brought what this claim could not see.
        if (claimed.length < request.limit && wakeGeneration === this.#wakeGeneration) {
          this.#exhaustedHandlers.add(handler);
        } else {
          this.#exhaustedHandlers.delete(handler);
        }
      }
      for (const delivery of claimed) {
        started.push(this.#start(delivery));
      }
    }
    return { requested, claimed: started.length, started };
  }

  /** One claim for handlers without a concurrency limit, one per limited handler with room. */
  #planClaims(
    registrations: readonly HandlerRegistration[],
  ): Array<{ handlers: string[]; limit: number }> {
    let free = Math.min(this.#freeSlots(), this.#dependencies.settings.batchSize);
    const plans: Array<{ handlers: string[]; limit: number }> = [];
    const unlimited = registrations.filter((registration) => registration.concurrency === null);
    for (const registration of registrations) {
      if (registration.concurrency === null || free === 0) {
        continue;
      }
      const room = registration.concurrency - this.#countInFlight(registration.id);
      const limit = Math.min(room, free);
      if (limit > 0) {
        plans.push({ handlers: [registration.id], limit });
        free -= limit;
      }
    }
    if (unlimited.length > 0 && free > 0) {
      plans.push({ handlers: unlimited.map((registration) => registration.id), limit: free });
    }
    return plans;
  }

  #countInFlight(handler: string): number {
    let count = 0;
    for (const delivery of this.#inFlight) {
      if (delivery.handler === handler) {
        count += 1;
      }
    }
    return count;
  }

  #start(delivery: ClaimedDelivery): InFlightDelivery {
    const controller = new AbortController();
    let succeeded = false;
    const entry: InFlightDelivery = {
      handler: delivery.handler,
      controller,
      settled: Promise.resolve()
        .then(async () => {
          succeeded = await this.#execute(delivery, controller);
        })
        // Safety net: #execute handles every expected failure itself.
        .catch((error: unknown) => this.#dependencies.notifier.failure('dispatch', error))
        .finally(() => {
          this.#inFlight.delete(entry);
          this.#onSettled(delivery, succeeded);
        }),
    };
    this.#inFlight.add(entry);
    return entry;
  }

  /** Runs one delivery. Resolves true when its handler succeeded, so the delivery is done for good. */
  async #execute(delivery: ClaimedDelivery, controller: AbortController): Promise<boolean> {
    const { registry, notifier, clock } = this.#dependencies;
    const registration = registry.get(delivery.handler)!;
    // A new claim of this delivery makes whatever was pending for its earlier claims moot.
    for (const key of [...this.#pending.keys()].filter((pending) =>
      pending.startsWith(`${delivery.deliveryId}\u0000`),
    )) {
      this.#pending.delete(key);
    }
    notifier.notify('onDeliveryStarted', { delivery });
    const startedAt = clock.now().getTime();
    if (delivery.attempt > registration.retry.attempts) {
      await this.#bury(
        delivery,
        new AttemptsExhaustedError(
          `Delivery reached attempt ${delivery.attempt} of ${registration.retry.attempts} after its lease expired`,
        ),
      );
      return false;
    }
    let stopHeartbeat = (): void => undefined;
    try {
      stopHeartbeat = registration.transactional
        ? await this.#guardTransactionalLease(registration, delivery, controller)
        : this.#startHeartbeat(delivery, controller);
      const outcome = await this.#runHandler(registration, delivery, controller);
      const durationMs = clock.now().getTime() - startedAt;
      if (outcome === 'completed') {
        notifier.notify('onDeliverySucceeded', { delivery, durationMs });
      } else {
        this.#retain({
          delivery,
          settle: () => this.#complete(delivery),
          applied: () => notifier.notify('onDeliverySucceeded', { delivery, durationMs }),
        });
      }
      return true;
    } catch (error) {
      if (error instanceof LeaseLostError) {
        notifier.notify('onLeaseLost', { delivery });
        if (!error.storeConfirmed) {
          await this.#giveBack(registration, delivery, error);
        }
      } else if (error instanceof DispatcherStoppedError) {
        await this.#release(delivery);
      } else {
        await this.#fail(registration, delivery, error);
      }
      return false;
    } finally {
      stopHeartbeat();
    }
  }

  async #runHandler(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    controller: AbortController,
  ): Promise<HandlerOutcome> {
    const { store, clock } = this.#dependencies;
    const payload = await validatePayload(
      delivery.event.type,
      registration.schema,
      delivery.event.payload,
    );
    const event: EventEnvelope = { ...delivery.event, payload };
    if (registration.transactional) {
      await store.transaction(async (transaction) => {
        await this.#invoke({ registration, event, delivery, controller, transaction });
        if (!(await store.complete(delivery, clock.now(), transaction))) {
          throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, {
            storeConfirmed: true,
          });
        }
      });
      return 'completed';
    }
    await this.#invoke({ registration, event, delivery, controller, transaction: undefined });
    return this.#confirmCompletion(delivery);
  }

  /**
   * Records a completion, retrying transient store errors while the lease is
   * still held. Gives up to lease expiry, which reruns the handler: at-least-once.
   */
  async #confirmCompletion(delivery: ClaimedDelivery): Promise<HandlerOutcome> {
    const { notifier } = this.#dependencies;
    for (let attempt = 0; ; attempt += 1) {
      let completed: boolean;
      try {
        completed = await this.#complete(delivery);
      } catch (error) {
        const delay = COMPLETION_RETRY_DELAYS_MS[attempt];
        if (delay === undefined) {
          notifier.failure('complete', error);
          return 'unconfirmed';
        }
        await this.#sleep(delay);
        continue;
      }
      if (!completed) {
        throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, {
          storeConfirmed: true,
        });
      }
      return 'completed';
    }
  }

  /** Records one completion, grouped with its neighbours when the store can complete many at once. */
  #complete(delivery: ClaimedDelivery): Promise<boolean> {
    const { store, clock } = this.#dependencies;
    return this.#completions === undefined
      ? store.complete(delivery, clock.now())
      : this.#completions.submit(delivery);
  }

  async #invoke(input: InvokeInput): Promise<void> {
    const { registration, event, delivery, controller, transaction } = input;
    const context: HandlerContext = {
      deliveryId: delivery.deliveryId,
      handler: delivery.handler,
      attempt: delivery.attempt,
      maxAttempts: registration.retry.attempts,
      signal: controller.signal,
      transaction,
    };
    // An isolated scope keeps a caller's transaction (for example a test calling
    // runOnce inside bus.transaction) from leaking into the handler.
    const scope = transaction === undefined ? { event } : { event, transaction };
    if (controller.signal.aborted) {
      // The lease was lost (or the bus stopped) while the payload was validated.
      throw controller.signal.reason;
    }
    const work = Promise.resolve().then(() =>
      runInIsolatedScope(scope, () => registration.handle(event, context)),
    );
    work.catch(() => undefined);
    let timer: NodeJS.Timeout | undefined;
    if (registration.timeoutMs !== null) {
      const timeoutMs = registration.timeoutMs;
      timer = setTimeout(() => {
        controller.abort(
          new HandlerTimeoutError(
            `Handler '${registration.id}' did not settle within ${timeoutMs}ms`,
          ),
        );
      }, timeoutMs);
    }
    try {
      await Promise.race([work, rejectOnAbort(controller.signal)]);
    } catch (error) {
      await this.#awaitAbandonedHandler(work, error, transaction !== undefined);
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * After an abort the handler may still be running. Waiting for it keeps a
   * retry from overlapping the abandoned run, and keeps a transactional
   * handler from writing through a transaction that was already released.
   * Transactional handlers are always awaited; others for one lease duration,
   * except at shutdown, which already waited.
   */
  async #awaitAbandonedHandler(
    work: Promise<unknown>,
    reason: unknown,
    transactional: boolean,
  ): Promise<void> {
    const settledWork = work.then(
      () => undefined,
      () => undefined,
    );
    if (transactional) {
      await settledWork;
      return;
    }
    if (reason instanceof DispatcherStoppedError) {
      return;
    }
    const deadline = Date.now() + this.#dependencies.settings.leaseDurationMs;
    await this.#waitUntil(settledWork, deadline);
  }

  /**
   * Transactional handlers get no renewals: a renewal would write the delivery
   * the open transaction completes, which databases with snapshot isolation
   * (MongoDB) turn into a write conflict that reruns the handler. Instead the
   * lease is extended once to cover the handler timeout, and the handler is
   * aborted when that lease ends.
   */
  async #guardTransactionalLease(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    controller: AbortController,
  ): Promise<() => void> {
    const { store, clock, settings } = this.#dependencies;
    const now = clock.now();
    let expiry = delivery.leaseExpiresAt.getTime();
    if (registration.timeoutMs !== null && now.getTime() + registration.timeoutMs >= expiry) {
      expiry = now.getTime() + registration.timeoutMs + settings.leaseDurationMs;
      if (!(await store.extendLease(delivery, new Date(expiry), now))) {
        throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, {
          storeConfirmed: true,
        });
      }
    }
    const timer = setTimeout(
      () => {
        controller.abort(new LeaseLostError(`Lease on delivery ${delivery.deliveryId} expired`));
      },
      Math.max(0, expiry - clock.now().getTime()),
    );
    timer.unref();
    return () => clearTimeout(timer);
  }

  /**
   * Renews the lease every third of its duration. A worker that cannot renew
   * aborts its handler once the last confirmed expiry passes, because another
   * worker may then claim the delivery (self-fencing).
   */
  #startHeartbeat(delivery: ClaimedDelivery, controller: AbortController): () => void {
    const { store, clock, notifier, settings } = this.#dependencies;
    let confirmedExpiry = delivery.leaseExpiresAt.getTime();
    let renewing = false;
    const abortLeaseLost = (storeConfirmed = false): void => {
      controller.abort(
        new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, { storeConfirmed }),
      );
    };
    const timer = setInterval(
      () => {
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
      Math.max(1, Math.floor(settings.leaseDurationMs / RENEWALS_PER_LEASE)),
    );
    timer.unref();
    return () => clearInterval(timer);
  }

  async #fail(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    error: unknown,
  ): Promise<void> {
    const policy = registration.retry;
    if (delivery.attempt >= policy.attempts || !isRetryableError(policy, error)) {
      await this.#bury(delivery, error);
      return;
    }
    await this.#reschedule(registration, delivery, error);
  }

  /** Schedules the next attempt with the retry policy's backoff. */
  async #reschedule(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    error: unknown,
  ): Promise<void> {
    const { store, clock, random, notifier } = this.#dependencies;
    const policy = registration.retry;
    const errorInfo = toErrorInfo(error);
    const now = clock.now();
    const availableAt = new Date(
      now.getTime() + computeRetryDelay(policy, delivery.attempt, random),
    );
    let rescheduled: boolean;
    try {
      rescheduled = await store.reschedule(delivery, availableAt, errorInfo, now);
    } catch (storeError) {
      notifier.failure('reschedule', storeError);
      this.#retain({
        delivery,
        settle: () => store.reschedule(delivery, availableAt, errorInfo, now),
        applied: () =>
          notifier.notify('onDeliveryRetryScheduled', { delivery, error: errorInfo, availableAt }),
      });
      return;
    }
    if (rescheduled) {
      notifier.notify('onDeliveryRetryScheduled', { delivery, error: errorInfo, availableAt });
    } else {
      notifier.notify('onLeaseLost', { delivery });
    }
  }

  async #bury(delivery: ClaimedDelivery, error: unknown): Promise<void> {
    const { store, clock, notifier } = this.#dependencies;
    const errorInfo = toErrorInfo(error);
    let buried: boolean;
    try {
      buried = await store.bury(delivery, errorInfo, clock.now());
    } catch (storeError) {
      notifier.failure('bury', storeError);
      this.#retain({
        delivery,
        settle: () => store.bury(delivery, errorInfo, clock.now()),
        applied: () => notifier.notify('onDeliveryDead', { delivery, error: errorInfo }),
      });
      return;
    }
    if (buried) {
      notifier.notify('onDeliveryDead', { delivery, error: errorInfo });
    } else {
      notifier.notify('onLeaseLost', { delivery });
    }
  }

  /**
   * A worker that only noticed locally that its lease ran out may still own the delivery in the
   * store, where other deliveries of its lane keep renewing the lane: it must not stay in flight
   * for ever. The attempt took longer than its lease, so it counts as a failed attempt (an
   * exhausted delivery is dead-lettered, whatever a custom `retryIf` says): a handler that always
   * outlives its lease cannot run for ever at attempt 1. If the lease is gone for real the store
   * refuses and nothing changes. The lost lease was reported already.
   */
  async #giveBack(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    error: LeaseLostError,
  ): Promise<void> {
    if (delivery.attempt >= registration.retry.attempts) {
      await this.#bury(delivery, error);
    } else {
      await this.#reschedule(registration, delivery, error);
    }
  }

  async #release(delivery: ClaimedDelivery): Promise<void> {
    const { store, clock, notifier } = this.#dependencies;
    let released: boolean;
    try {
      released = await store.release(delivery, clock.now());
    } catch (storeError) {
      notifier.failure('release', storeError);
      this.#retain({
        delivery,
        settle: () => store.release(delivery, clock.now()),
        applied: () => notifier.notify('onDeliveryReleased', { delivery }),
      });
      return;
    }
    if (released) {
      notifier.notify('onDeliveryReleased', { delivery });
    } else {
      notifier.notify('onLeaseLost', { delivery });
    }
  }
}

/** Settlements are pending per claim: an older claim of a delivery must never replace or remove the newer one's. */
function pendingKey(delivery: ClaimedDelivery): string {
  return `${delivery.deliveryId}\u0000${delivery.leaseToken}`;
}

function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise<never>((_, reject) => {
    // The reason is always an Error: the dispatcher aborts with LeaseLostError,
    // HandlerTimeoutError or DispatcherStoppedError.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
}
