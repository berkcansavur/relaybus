import type { Clock } from './clock.js';
import { HandlerTimeoutError, LeaseLostError, RelayBusError, toErrorInfo } from './errors.js';
import { validatePayload, type EventEnvelope } from './event.js';
import type { ClaimedDelivery, EventStore } from './event-store.js';
import { runInScope } from './execution-scope.js';
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
}

export interface DispatcherDependencies {
  readonly store: EventStore;
  readonly registry: HandlerRegistry;
  readonly clock: Clock;
  readonly random: () => number;
  readonly notifier: Notifier;
  readonly settings: DispatcherSettings;
}

/** Raised into a handler's signal when the dispatcher stops before the handler finished. */
class DispatcherStoppedError extends RelayBusError {}

interface InFlightDelivery {
  readonly handler: string;
  readonly controller: AbortController;
  readonly settled: Promise<void>;
}

/**
 * Claims due deliveries and runs their handlers: bounded concurrency, lease
 * heartbeats, fenced completion, retries with backoff and dead-lettering.
 */
export class Dispatcher {
  readonly #dependencies: DispatcherDependencies;
  readonly #inFlight = new Set<InFlightDelivery>();
  #running = false;
  #loop: Promise<void> | undefined;
  #wakeUp: (() => void) | undefined;
  #wakeRequested = false;

  constructor(dependencies: DispatcherDependencies) {
    this.#dependencies = dependencies;
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
   * then aborts the rest. Their leases expire and another process retries them.
   */
  async stop(): Promise<void> {
    if (!this.#running) {
      return;
    }
    this.#running = false;
    this.wake();
    await this.#loop;
    const settled = Promise.allSettled([...this.#inFlight].map((delivery) => delivery.settled));
    let timer: NodeJS.Timeout | undefined;
    const timedOut = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), this.#dependencies.settings.shutdownTimeoutMs);
    });
    const outcome = await Promise.race([settled, timedOut]);
    clearTimeout(timer);
    if (outcome === 'timeout') {
      for (const delivery of this.#inFlight) {
        delivery.controller.abort(new DispatcherStoppedError('relaybus dispatcher stopped'));
      }
      await settled;
    }
  }

  /** Claims one batch and resolves when every claimed delivery settled. Returns how many ran. */
  async processBatch(): Promise<number> {
    const started = await this.#claimAndStart();
    await Promise.allSettled(started.map((delivery) => delivery.settled));
    return started.length;
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
    const heartbeat = this.#startHeartbeat(delivery, controller);
    try {
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
      } else if (!(error instanceof DispatcherStoppedError)) {
        await this.#fail(registration, delivery, error);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  async #runHandler(
    registration: HandlerRegistration,
    delivery: ClaimedDelivery,
    controller: AbortController,
  ): Promise<'completed' | 'unconfirmed'> {
    const { store, clock, notifier } = this.#dependencies;
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
    let completed: boolean;
    try {
      completed = await store.complete(delivery, clock.now());
    } catch (error) {
      notifier.failure('complete', error);
      return 'unconfirmed';
    }
    if (!completed) {
      throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`);
    }
    return 'completed';
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
    const scope = transaction === undefined ? { event } : { event, transaction };
    const work = Promise.resolve().then(() =>
      runInScope(scope, () => registration.handle(event, context)),
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
    } finally {
      clearTimeout(timer);
    }
  }

  #startHeartbeat(delivery: ClaimedDelivery, controller: AbortController): NodeJS.Timeout {
    const { store, clock, notifier, settings } = this.#dependencies;
    const timer = setInterval(
      () => {
        const now = clock.now();
        store
          .extendLease(delivery, new Date(now.getTime() + settings.leaseDurationMs), now)
          .then((extended) => {
            if (!extended) {
              controller.abort(
                new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`),
              );
            }
          })
          .catch((error: unknown) => notifier.failure('extend-lease', error));
      },
      Math.max(1, Math.floor(settings.leaseDurationMs / 3)),
    );
    timer.unref();
    return timer;
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
}

function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise<never>((_, reject) => {
    // The reason is always an Error: dispatcher code aborts with LeaseLostError, HandlerTimeoutError or DispatcherStoppedError.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
}
