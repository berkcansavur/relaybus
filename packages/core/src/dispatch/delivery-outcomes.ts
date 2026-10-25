import type { CompletionBatcher } from '../completion-batcher.js';
import { LeaseLostError, toErrorInfo } from '../errors.js';
import type { ClaimedDelivery } from '../event-store.js';
import type { HandlerRegistration } from '../handler-registry.js';
import { computeRetryDelay, isRetryableError } from '../retry-policy.js';
import type { DispatcherDependencies, ShutdownLatch } from './dispatch-context.js';
import type { SettlementRecovery } from './settlement-recovery.js';

/** How a handler run ended for the store: recorded, or left to lease expiry. */
export type HandlerOutcome = 'completed' | 'unconfirmed';

/** Delays between attempts to record a completion before leaving it to lease expiry. */
const FIRST_COMPLETION_RETRY_MS = 50;
const SECOND_COMPLETION_RETRY_MS = 250;
const LAST_COMPLETION_RETRY_MS = 1_000;
const COMPLETION_RETRY_DELAYS_MS = [
  FIRST_COMPLETION_RETRY_MS,
  SECOND_COMPLETION_RETRY_MS,
  LAST_COMPLETION_RETRY_MS,
] as const;

/** A delivery and why its run failed. */
type FailedRun<TError = unknown> = { readonly delivery: ClaimedDelivery; readonly error: TError };

/** One state change of a finished delivery, with what to report once the store applied it. */
type StateChange = {
  readonly operation: 'reschedule' | 'bury' | 'release';
  readonly delivery: ClaimedDelivery;
  /** Writes the change now. */
  readonly write: () => Promise<boolean>;
  /** Writes it again when the store failed; recovered later. */
  readonly rewrite: () => Promise<boolean>;
  readonly applied: () => void;
};

/** What the dispatcher needs to record the outcome of a delivery. */
export type DeliveryOutcomesParts = {
  readonly dependencies: DispatcherDependencies;
  readonly recovery: SettlementRecovery;
  readonly shutdown: ShutdownLatch;
  readonly completions: CompletionBatcher | undefined;
  readonly sleep: (milliseconds: number) => Promise<void>;
};

/**
 * Records how deliveries ended: completion (confirmed against transient store errors), retry with
 * backoff, dead-lettering and release. A state change the store fails to apply is handed to
 * {@link SettlementRecovery}; one the store refuses means the lease is lost. The state changes are
 * not async themselves: each awaits the store once, inside `#change`.
 */
export class DeliveryOutcomes {
  readonly #parts: DeliveryOutcomesParts;

  constructor(parts: DeliveryOutcomesParts) {
    this.#parts = parts;
  }

  /**
   * Records a completion, retrying transient store errors while the lease is
   * still held. Gives up to lease expiry, which reruns the handler: at-least-once.
   */
  async confirmCompletion(delivery: ClaimedDelivery): Promise<HandlerOutcome> {
    const { dependencies, shutdown, sleep } = this.#parts;
    for (let attempt = 0; ; attempt += 1) {
      if (shutdown.abandoned) {
        // Stop gave this delivery up: no completion may reach the store any more.
        return 'unconfirmed';
      }
      let completed: boolean;
      try {
        completed = await this.complete(delivery);
      } catch (error) {
        const delay = COMPLETION_RETRY_DELAYS_MS[attempt];
        if (delay === undefined) {
          dependencies.notifier.failure('complete', error);
          return 'unconfirmed';
        }
        await sleep(delay);
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
  complete(delivery: ClaimedDelivery): Promise<boolean> {
    const { dependencies, completions } = this.#parts;
    const { store, clock } = dependencies;
    return completions === undefined
      ? store.complete(delivery, clock.now())
      : completions.submit(delivery);
  }

  async fail(registration: HandlerRegistration, { delivery, error }: FailedRun): Promise<void> {
    const policy = registration.retry;
    if (delivery.attempt >= policy.attempts || !isRetryableError(policy, error)) {
      await this.bury(delivery, error);
      return;
    }
    await this.#reschedule(registration, { delivery, error });
  }

  /**
   * A worker that only noticed locally that its lease ran out may still own the delivery in the
   * store, where other deliveries of its lane keep renewing the lane: it must not stay in flight
   * for ever. The attempt took longer than its lease, so it counts as a failed attempt (an
   * exhausted delivery is dead-lettered, whatever a custom `retryIf` says): a handler that always
   * outlives its lease cannot run for ever at attempt 1. If the lease is gone for real the store
   * refuses and nothing changes. The lost lease was reported already.
   */
  async giveBack(
    registration: HandlerRegistration,
    { delivery, error }: FailedRun<LeaseLostError>,
  ): Promise<void> {
    if (delivery.attempt >= registration.retry.attempts) {
      await this.bury(delivery, error);
    } else {
      await this.#reschedule(registration, { delivery, error });
    }
  }

  bury(delivery: ClaimedDelivery, error: unknown): Promise<void> {
    const { store, clock, notifier } = this.#parts.dependencies;
    const errorInfo = toErrorInfo(error);
    return this.#change({
      operation: 'bury',
      delivery,
      write: () => store.bury(delivery, errorInfo, clock.now()),
      rewrite: () => store.bury(delivery, errorInfo, clock.now()),
      applied: () => notifier.notify('onDeliveryDead', { delivery, error: errorInfo }),
    });
  }

  release(delivery: ClaimedDelivery): Promise<void> {
    const { store, clock, notifier } = this.#parts.dependencies;
    return this.#change({
      operation: 'release',
      delivery,
      write: () => store.release(delivery, clock.now()),
      rewrite: () => store.release(delivery, clock.now()),
      applied: () => notifier.notify('onDeliveryReleased', { delivery }),
    });
  }

  /** Schedules the next attempt with the retry policy's backoff. */
  #reschedule(registration: HandlerRegistration, { delivery, error }: FailedRun): Promise<void> {
    const { store, clock, random, notifier } = this.#parts.dependencies;
    const policy = registration.retry;
    const errorInfo = toErrorInfo(error);
    const now = clock.now();
    const availableAt = new Date(
      now.getTime() + computeRetryDelay(policy, delivery.attempt, random),
    );
    const write = (): Promise<boolean> => store.reschedule(delivery, availableAt, errorInfo, now);
    return this.#change({
      operation: 'reschedule',
      delivery,
      write,
      rewrite: write,
      applied: () =>
        notifier.notify('onDeliveryRetryScheduled', { delivery, error: errorInfo, availableAt }),
    });
  }

  /** Writes a state change; reports it, or the lost lease, or retains it when the store failed. */
  async #change(change: StateChange): Promise<void> {
    const { dependencies, recovery } = this.#parts;
    const { delivery } = change;
    let changed: boolean;
    try {
      changed = await change.write();
    } catch (storeError) {
      dependencies.notifier.failure(change.operation, storeError);
      recovery.retain({ delivery, settle: change.rewrite, applied: change.applied });
      return;
    }
    if (changed) {
      change.applied();
    } else {
      dependencies.notifier.notify('onLeaseLost', { delivery });
    }
  }
}
