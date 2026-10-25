import type { Clock } from '../clock.js';
import type { DeliveryTimers } from '../delivery-timers.js';
import { DispatcherStoppedError, HandlerTimeoutError } from '../errors.js';
import type { EventEnvelope } from '../event.js';
import type { ClaimedDelivery } from '../event-store.js';
import { runInIsolatedScope } from '../execution-scope.js';
import type { HandlerContext, HandlerRegistration } from '../handler-registry.js';
import { schedule, type CancelTimer } from '../internal/timers.js';
import type { DispatcherDependencies } from './dispatch-context.js';

/** One run of a handler: the delivery, its validated event and the transaction it runs in, if any. */
export type InvokeInput = {
  readonly registration: HandlerRegistration;
  readonly event: EventEnvelope;
  readonly delivery: ClaimedDelivery;
  readonly controller: AbortController;
  readonly transaction: unknown;
};

export function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise<never>((_, reject) => {
    // The reason is always an Error: the dispatcher aborts with LeaseLostError,
    // HandlerTimeoutError or DispatcherStoppedError.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
}

/**
 * Calls a handler: in an isolated execution scope, observed when the bus is, bounded by its
 * timeout and abortable. A handler that is abandoned (aborted) is still waited for before the
 * delivery moves on, so a retry never overlaps it.
 */
export class HandlerInvocation {
  readonly #dependencies: DispatcherDependencies;
  readonly #timers: DeliveryTimers;

  constructor(dependencies: DispatcherDependencies, timers: DeliveryTimers) {
    this.#dependencies = dependencies;
    this.#timers = timers;
  }

  async invoke(input: InvokeInput): Promise<void> {
    const { registration, event, delivery, controller, transaction } = input;
    const context = contextOf(input);
    // An isolated scope keeps a caller's transaction (for example a test calling
    // runOnce inside bus.transaction) from leaking into the handler.
    const scope = transaction === undefined ? { event } : { event, transaction };
    if (controller.signal.aborted) {
      // The lease was lost (or the bus stopped) while the payload was validated.
      throw controller.signal.reason;
    }
    const handle = (): unknown =>
      runInIsolatedScope(scope, () => registration.handle(event, context));
    const { observation } = this.#dependencies;
    const work = Promise.resolve().then(
      observation === undefined
        ? handle
        : () => observation.process({ registration, delivery }, handle),
    );
    work.catch(() => undefined);
    const cancelTimeout = this.#startTimeout(registration, controller);
    try {
      const settled = Promise.race([work, rejectOnAbort(controller.signal)]);
      await (observation === undefined
        ? settled
        : observation.observe({ registration, delivery }, settled));
    } catch (error) {
      await this.#awaitAbandonedHandler(work, {
        reason: error,
        transactional: transaction !== undefined,
      });
      throw error;
    } finally {
      cancelTimeout?.();
    }
  }

  /** Aborts the handler when its timeout passes; undefined for a handler without one. */
  #startTimeout(
    registration: HandlerRegistration,
    controller: AbortController,
  ): CancelTimer | undefined {
    if (registration.timeoutMs === null) {
      return undefined;
    }
    const timeoutMs = registration.timeoutMs;
    return this.#timers.start({
      run: () => {
        controller.abort(
          new HandlerTimeoutError(
            `Handler '${registration.id}' did not settle within ${timeoutMs}ms`,
          ),
        );
      },
      delayMs: timeoutMs,
    });
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
    { reason, transactional }: { readonly reason: unknown; readonly transactional: boolean },
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
    await waitUntil(this.#dependencies.clock, { work: settledWork, deadline });
  }
}

/** What the handler learns about its delivery. */
function contextOf(input: InvokeInput): HandlerContext {
  const { registration, delivery, controller, transaction } = input;
  return {
    deliveryId: delivery.deliveryId,
    handler: delivery.handler,
    attempt: delivery.attempt,
    maxAttempts: registration.retry.attempts,
    signal: controller.signal,
    transaction,
  };
}

async function waitUntil(
  clock: Clock,
  { work, deadline }: { readonly work: Promise<unknown>; readonly deadline: number },
): Promise<'done' | 'timeout'> {
  let cancel: CancelTimer | undefined;
  const timedOut = new Promise<'timeout'>((resolve) => {
    cancel = schedule(clock, {
      run: () => resolve('timeout'),
      delayMs: Math.max(0, deadline - Date.now()),
    });
  });
  const outcome = await Promise.race([work.then(() => 'done' as const), timedOut]);
  cancel?.();
  return outcome;
}
