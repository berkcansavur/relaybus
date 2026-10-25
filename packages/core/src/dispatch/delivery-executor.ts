import type { DeliveryTimers } from '../delivery-timers.js';
import { AttemptsExhaustedError, DispatcherStoppedError, LeaseLostError } from '../errors.js';
import { validatePayload, type EventEnvelope } from '../event.js';
import type { ClaimedDelivery } from '../event-store.js';
import type { HandlerRegistration } from '../handler-registry.js';
import { schedule } from '../internal/timers.js';
import { startLeaseHeartbeat } from '../lease-heartbeat.js';
import type { DispatcherDependencies, ShutdownLatch } from './dispatch-context.js';
import type { DeliveryOutcomes, HandlerOutcome } from './delivery-outcomes.js';
import type { HandlerInvocation } from './handler-invocation.js';
import type { InFlightDeliveries, InFlightDelivery } from './in-flight-deliveries.js';
import type { SettlementRecovery } from './settlement-recovery.js';

/** A claimed delivery and the controller that aborts its handler. */
type DeliveryRun = { readonly delivery: ClaimedDelivery; readonly controller: AbortController };

/** What runs a delivery. */
export type DeliveryExecutorParts = {
  readonly dependencies: DispatcherDependencies;
  readonly inFlight: InFlightDeliveries;
  readonly recovery: SettlementRecovery;
  readonly outcomes: DeliveryOutcomes;
  readonly invocation: HandlerInvocation;
  readonly timers: DeliveryTimers;
  readonly shutdown: ShutdownLatch;
  /** Told when a delivery settled, and whether its handler succeeded. */
  readonly onSettled: (delivery: ClaimedDelivery, succeeded: boolean) => void;
};

/**
 * Runs claimed deliveries: keeps each lease (heartbeat, or one extension for a transactional
 * handler), validates the payload, calls the handler and records the outcome.
 */
export class DeliveryExecutor {
  readonly #parts: DeliveryExecutorParts;
  /** Stops the lease renewal (or transactional lease guard) of each delivery whose handler runs. */
  readonly #leaseKeepers = new Map<ClaimedDelivery, () => void>();

  constructor(parts: DeliveryExecutorParts) {
    this.#parts = parts;
  }

  /** Starts a delivery and tracks it as in flight until it settled. */
  start(delivery: ClaimedDelivery): InFlightDelivery {
    const { dependencies, inFlight, onSettled } = this.#parts;
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
        .catch((error: unknown) => dependencies.notifier.failure('dispatch', error))
        .finally(() => {
          inFlight.delete(entry);
          onSettled(delivery, succeeded);
        }),
    };
    inFlight.add(entry);
    return entry;
  }

  /** Stop gave up: lets every lease run out, so that another process takes the deliveries. */
  abandon(): void {
    for (const stopKeeping of this.#leaseKeepers.values()) {
      stopKeeping();
    }
    this.#leaseKeepers.clear();
  }

  /** Runs one delivery. Resolves true when its handler succeeded, so the delivery is done for good. */
  async #execute(delivery: ClaimedDelivery, controller: AbortController): Promise<boolean> {
    const { dependencies, recovery, outcomes } = this.#parts;
    const { registry, notifier, clock } = dependencies;
    const registration = registry.get(delivery.handler)!;
    recovery.forgetEarlierClaims(delivery);
    notifier.notify('onDeliveryStarted', { delivery });
    const startedAt = clock.now().getTime();
    if (delivery.attempt > registration.retry.attempts) {
      await outcomes.bury(
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
        ? await this.#guardTransactionalLease(registration, { delivery, controller })
        : startLeaseHeartbeat(dependencies, { delivery, controller });
      this.#leaseKeepers.set(delivery, stopHeartbeat);
      const outcome = await this.#runHandler(registration, { delivery, controller });
      this.#reportSuccess(delivery, { outcome, durationMs: clock.now().getTime() - startedAt });
      return true;
    } catch (error) {
      if (this.#parts.shutdown.abandoned) {
        // Stop gave this delivery up and the store may be closed: its lease expires instead.
        return false;
      }
      const recording = this.#recordFailure(registration, { delivery, error });
      if (recording !== undefined) {
        await recording;
      }
      return false;
    } finally {
      stopHeartbeat();
      this.#leaseKeepers.delete(delivery);
    }
  }

  /** Reports a success now, or once a completion the store did not confirm is recovered. */
  #reportSuccess(
    delivery: ClaimedDelivery,
    { outcome, durationMs }: { readonly outcome: HandlerOutcome; readonly durationMs: number },
  ): void {
    const { dependencies, outcomes, recovery } = this.#parts;
    const { notifier } = dependencies;
    if (outcome === 'completed') {
      notifier.notify('onDeliverySucceeded', { delivery, durationMs });
    } else {
      recovery.retain({
        delivery,
        settle: () => outcomes.complete(delivery),
        applied: () => notifier.notify('onDeliverySucceeded', { delivery, durationMs }),
      });
    }
  }

  /** Records a failed run; undefined when there is nothing to record (the store confirmed the lost lease). */
  #recordFailure(
    registration: HandlerRegistration,
    { delivery, error }: { readonly delivery: ClaimedDelivery; readonly error: unknown },
  ): Promise<void> | undefined {
    const { dependencies, outcomes } = this.#parts;
    if (error instanceof LeaseLostError) {
      dependencies.notifier.notify('onLeaseLost', { delivery });
      return error.storeConfirmed
        ? undefined
        : outcomes.giveBack(registration, { delivery, error });
    }
    if (error instanceof DispatcherStoppedError) {
      return outcomes.release(delivery);
    }
    return outcomes.fail(registration, { delivery, error });
  }

  async #runHandler(
    registration: HandlerRegistration,
    { delivery, controller }: DeliveryRun,
  ): Promise<HandlerOutcome> {
    const { dependencies, invocation, outcomes } = this.#parts;
    const { store, clock } = dependencies;
    const payload = await validatePayload(
      delivery.event.type,
      registration.schema,
      delivery.event.payload,
    );
    const event: EventEnvelope = { ...delivery.event, payload };
    if (registration.transactional) {
      await store.transaction(async (transaction) => {
        await invocation.invoke({ registration, event, delivery, controller, transaction });
        if (!(await store.complete(delivery, clock.now(), transaction))) {
          throw new LeaseLostError(`Lease on delivery ${delivery.deliveryId} was lost`, {
            storeConfirmed: true,
          });
        }
      });
      return 'completed';
    }
    await invocation.invoke({ registration, event, delivery, controller, transaction: undefined });
    return outcomes.confirmCompletion(delivery);
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
    { delivery, controller }: DeliveryRun,
  ): Promise<() => void> {
    const { store, clock, settings } = this.#parts.dependencies;
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
    return schedule(clock, {
      run: () => {
        controller.abort(new LeaseLostError(`Lease on delivery ${delivery.deliveryId} expired`));
      },
      delayMs: Math.max(0, expiry - clock.now().getTime()),
      unref: true,
    });
  }
}
