import type { Clock } from './clock.js';
import { toErrorInfo } from './errors.js';
import type { ClaimRequest, ClaimedDelivery, EventStore } from './event-store.js';
import type { HandlerRegistration } from './handler-registry.js';
import type { Notifier } from './notifier.js';
import type { DeliveryKind } from './observer.js';
import type { RelayBusTracer } from './tracing.js';

/** One run of a handler for a claimed delivery. */
export type HandlerRun = {
  readonly registration: HandlerRegistration;
  readonly delivery: ClaimedDelivery;
};

type DispatchObservationInput = {
  readonly notifier: Notifier;
  readonly tracer: RelayBusTracer | undefined;
  readonly clock: Clock;
};

/**
 * Reports the dispatcher's claims and handler runs to observers and wraps local handlers in the
 * tracer's process span. The dispatcher gets one only when something observes or traces, so an
 * unobserved bus pays nothing for it.
 */
export class DispatchObservation {
  readonly #notifier: Notifier;
  readonly #tracer: RelayBusTracer | undefined;
  readonly #clock: Clock;
  readonly #claims: boolean;
  readonly #runs: boolean;

  private constructor(input: DispatchObservationInput) {
    this.#notifier = input.notifier;
    this.#tracer = input.tracer;
    this.#clock = input.clock;
    this.#claims = input.notifier.wants('onClaimFinished');
    this.#runs =
      input.notifier.wants('onHandlerStarted') || input.notifier.wants('onHandlerFinished');
  }

  /** An observation, or undefined when no observer hook and no tracer would see anything. */
  static create(input: DispatchObservationInput): DispatchObservation | undefined {
    const observation = new DispatchObservation(input);
    return observation.#claims || observation.#runs || input.tracer?.process !== undefined
      ? observation
      : undefined;
  }

  /** Claims through `store`, reporting the query's size, lanes and duration. */
  async claim(store: EventStore, request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (!this.#claims) {
      return store.claim(request);
    }
    const startedAt = this.#clock.now().getTime();
    const report = { handlers: request.handlers, requested: request.limit };
    try {
      const claimed = await store.claim(request);
      this.#notifier.notify('onClaimFinished', {
        ...report,
        claimed: claimed.length,
        lanes: new Set(claimed.map((delivery) => delivery.leaseToken)).size,
        durationMs: this.#clock.now().getTime() - startedAt,
        outcome: 'success',
      });
      return claimed;
    } catch (error) {
      this.#notifier.notify('onClaimFinished', {
        ...report,
        claimed: 0,
        lanes: 0,
        durationMs: this.#clock.now().getTime() - startedAt,
        outcome: 'error',
        error: toErrorInfo(error),
      });
      throw error;
    }
  }

  /** Runs a local handler inside the tracer's process span; routes are traced as sends instead. */
  process(run: HandlerRun, handle: () => unknown): unknown {
    const tracer = this.#tracer;
    if (tracer?.process === undefined || run.registration.forwardsToTransport) {
      return handle();
    }
    return tracer.process({ delivery: run.delivery }, async () => handle());
  }

  /** Reports the start of a handler run now and its end when `settled` settles, passing it through. */
  async observe(run: HandlerRun, settled: Promise<unknown>): Promise<void> {
    if (!this.#runs) {
      await settled;
      return;
    }
    const { delivery } = run;
    const kind: DeliveryKind = run.registration.forwardsToTransport ? 'route' : 'handler';
    const startedAt = this.#clock.now().getTime();
    this.#notifier.notify('onHandlerStarted', {
      delivery,
      kind,
      endToEndLatencyMs: startedAt - delivery.event.occurredAt.getTime(),
    });
    try {
      await settled;
    } catch (error) {
      const durationMs = this.#clock.now().getTime() - startedAt;
      const errorInfo = toErrorInfo(error);
      this.#notifier.notify('onHandlerFinished', {
        delivery,
        kind,
        durationMs,
        outcome: 'error',
        error: errorInfo,
      });
      throw error;
    }
    const durationMs = this.#clock.now().getTime() - startedAt;
    this.#notifier.notify('onHandlerFinished', { delivery, kind, durationMs, outcome: 'success' });
  }
}
