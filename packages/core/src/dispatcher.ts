import type { Clock } from './clock.js';
import { CompletionBatcher } from './completion-batcher.js';
import {
  DispatcherStoppedError,
  HandlerTimeoutError,
  LeaseLostError,
  RelayBusError,
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

interface InFlightDelivery {
  readonly handler: string;
  readonly controller: AbortController;
  readonly settled: Promise<void>;
}

type HandlerOutcome = 'completed' | 'unconfirmed';

/** Delays between attempts to record a completion before leaving it to lease expiry. */
const COMPLETION_RETRY_DELAYS_MS = [50, 250, 1_000] as const;

const defaultSleep = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

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
  #running = false;
  #loop: Promise<void> | undefined;
  #wakeUp: (() => void) | undefined;
  #wakeRequested = false;

  constructor(dependencies: DispatcherDependencies) {
    this.#dependencies = dependencies;
    this.#sleep = dependencies.sleep ?? defaultSleep;
    const { store, clock, settings } = dependencies;
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

  start(): void {
    if (this.#running) {
      return;
    }
    this.#running = true;
    this.#loop = this.#runLoop();
  }

  /** Shortens the wait before the next claim, for example right after a publish. */
  wake(): void {
    this.#wakeRequested = true;
    this.#wakeUp?.();
  }

  /**
   * Stops claiming, waits up to the shutdown timeout for running handlers,
   * then aborts the rest and releases their deliveries for another process.
   */
  async stop(): Promise<void> {
    if (!this.#running) {
      return;
    }
    this.#running = false;
    this.wake();
    const deadline = Date.now() + this.#dependencies.settings.shutdownTimeoutMs;
    await this.#waitUntil(this.#loop!, deadline);
    const settled = Promise.allSettled([...this.#inFlight].map((delivery) => delivery.settled));
    if ((await this.#waitUntil(settled, deadline)) === 'timeout') {
      for (const delivery of this.#inFlight) {
        delivery.controller.abort(new DispatcherStoppedError('relaybus dispatcher stopped'));
      }
      await settled;
    }
    await this.#relinquish();
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
    const started = await this.#claimAndStart();
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

  async #runLoop(): Promise<void> {
    while (this.#running) {
      let claimedCount = 0;
      try {
        claimedCount = (await this.#claimAndStart()).length;
      } catch (error) {
        this.#dependencies.notifier.failure('claim', error);
      }
      if (claimedCount > 0 && this.#freeSlots() > 0) {
        continue;
      }
      await this.#waitForWakeUp();
    }
  }

  async #waitForWakeUp(): Promise<void> {
    if (this.#wakeRequested) {
      this.#wakeRequested = false;
      return;
    }
    await new Promise<void>((resolve) => {
      const timer = setTimeout(done, this.#dependencies.settings.pollIntervalMs);
      function done(): void {
        clearTimeout(timer);
        resolve();
      }
      this.#wakeUp = done;
    });
    this.#wakeUp = undefined;
    this.#wakeRequested = false;
  }

  #freeSlots(): number {
    return this.#dependencies.settings.concurrency - this.#inFlight.size;
  }

  async #claimAndStart(): Promise<InFlightDelivery[]> {
    const { store, registry, clock, settings } = this.#dependencies;
    const started: InFlightDelivery[] = [];
    for (const request of this.#planClaims(registry.all())) {
      const now = clock.now();
      const claimed = await store.claim({
        handlers: request.handlers,
        limit: request.limit,
        leaseOwner: settings.owner,
        leaseExpiresAt: new Date(now.getTime() + settings.leaseDurationMs),
        now,
      });
      for (const delivery of claimed) {
        started.push(this.#start(delivery));
      }
    }
    return started;
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
    const entry: InFlightDelivery = {
      handler: delivery.handler,
      controller,
      settled: Promise.resolve()
        .then(() => this.#execute(delivery, controller))
        // Safety net: #execute handles every expected failure itself.
        .catch((error: unknown) => this.#dependencies.notifier.failure('dispatch', error))
        .finally(() => {
          this.#inFlight.delete(entry);
          this.wake();
        }),
    };
    this.#inFlight.add(entry);
    return entry;
  }

  async #execute(delivery: ClaimedDelivery, controller: AbortController): Promise<void> {
    const { registry, notifier, clock } = this.#dependencies;
    const registration = registry.get(delivery.handler)!;
    notifier.notify('onDeliveryStarted', { delivery });
    const startedAt = clock.now().getTime();
    if (delivery.attempt > registration.retry.attempts) {
      await this.#bury(
        delivery,
        new RelayBusError(
          `Delivery reached attempt ${delivery.attempt} of ${registration.retry.attempts} after its lease expired`,
        ),
      );
      return;
    }
    let stopHeartbeat = (): void => undefined;
    try {
      stopHeartbeat = registration.transactional
        ? await this.#guardTransactionalLease(registration, delivery, controller)
        : this.#startHeartbeat(delivery, controller);
      const outcome = await this.#runHandler(registration, delivery, controller);
      if (outcome === 'completed') {
        notifier.notify('onDeliverySucceeded', {
          delivery,
          durationMs: clock.now().getTime() - startedAt,
        });
      }
    } catch (error) {
      if (error instanceof LeaseLostError) {
        notifier.notify('onLeaseLost', { delivery });
      } else if (error instanceof DispatcherStoppedError) {
        await this.#release(delivery);
      } else {
        await this.#fail(registration, delivery, error);
      }
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
        await this.#invoke(registration, event, delivery, controller, transaction);
        if (!(await store.complete(delivery, clock.now(), transaction))) {
          throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`);
        }
      });
      return 'completed';
    }
    await this.#invoke(registration, event, delivery, controller, undefined);
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
        throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`);
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

  async #invoke(
    registration: HandlerRegistration,
    event: EventEnvelope,
    delivery: ClaimedDelivery,
    controller: AbortController,
    transaction: unknown,
  ): Promise<void> {
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
        throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`);
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
    const abortLeaseLost = (): void => {
      controller.abort(new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`));
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
              abortLeaseLost();
            }
          })
          .catch((error: unknown) => notifier.failure('extend-lease', error))
          .finally(() => {
            renewing = false;
          });
      },
      Math.max(1, Math.floor(settings.leaseDurationMs / 3)),
    );
    timer.unref();
    return () => clearInterval(timer);
  }

  async #fail(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    error: unknown,
  ): Promise<void> {
    const { store, clock, random, notifier } = this.#dependencies;
    const policy = registration.retry;
    if (delivery.attempt >= policy.attempts || !isRetryableError(policy, error)) {
      await this.#bury(delivery, error);
      return;
    }
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
      return;
    }
    if (buried) {
      notifier.notify('onDeliveryDead', { delivery, error: errorInfo });
    } else {
      notifier.notify('onLeaseLost', { delivery });
    }
  }

  async #release(delivery: ClaimedDelivery): Promise<void> {
    const { store, clock, notifier } = this.#dependencies;
    let released: boolean;
    try {
      released = await store.release(delivery, clock.now());
    } catch (storeError) {
      notifier.failure('release', storeError);
      return;
    }
    if (released) {
      notifier.notify('onDeliveryReleased', { delivery });
    } else {
      notifier.notify('onLeaseLost', { delivery });
    }
  }
}

function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise<never>((_, reject) => {
    // The reason is always an Error: the dispatcher aborts with LeaseLostError,
    // HandlerTimeoutError or DispatcherStoppedError.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
}
