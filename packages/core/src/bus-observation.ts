import type { Clock } from './clock.js';
import { InvalidEnvelopeError, toErrorInfo } from './errors.js';
import type { EventStoreStatistics } from './event-store.js';
import type { EventEnvelope, EventHeaders } from './event.js';
import { schedule, type CancelTimer } from './internal/timers.js';
import type { Notifier } from './notifier.js';
import type { OperationOutcome, ReceiveOutcome } from './observer.js';
import { traceContextOf, type PublishTraceInput, type RelayBusTracer } from './tracing.js';
import type { Transport } from './transport.js';

type BusObservationInput = {
  readonly notifier: Notifier;
  readonly tracer: RelayBusTracer | undefined;
  readonly clock: Clock;
};

/** The transport and destination an event was received from; undefined for `bus.receive` calls. */
export type ReceiveOrigin = {
  readonly transport: Transport;
  readonly destination: string | undefined;
};

/** One event received from a broker, and where from. */
type ReceiveInput = {
  readonly envelope: EventEnvelope;
  readonly origin: ReceiveOrigin | undefined;
};

/** One send of a route: the event, the transport and its destination. */
type SendInput = {
  readonly envelope: EventEnvelope;
  readonly transport: Transport;
  readonly destination: string;
};

/** How the backlog is read and how often. */
type BacklogInput = {
  readonly intervalMs: number;
  readonly statistics: () => Promise<EventStoreStatistics>;
};

/** `options` with the trace headers added; headers the caller passed win. */
export function withTraceHeaders<T extends { readonly headers?: EventHeaders }>(
  options: T,
  headers: EventHeaders,
): T {
  return Object.keys(headers).length === 0
    ? options
    : { ...options, headers: { ...headers, ...options.headers } };
}

/**
 * Reports publishes, receives, sends and the backlog to observers and runs them inside the
 * tracer's spans. A bus gets one only when something observes or traces these, so an unobserved
 * bus pays nothing for it.
 */
export class BusObservation {
  readonly #notifier: Notifier;
  readonly #tracer: RelayBusTracer | undefined;
  readonly #clock: Clock;
  readonly #publishes: boolean;
  readonly #receives: boolean;
  readonly #sends: boolean;

  private constructor(input: BusObservationInput) {
    this.#notifier = input.notifier;
    this.#tracer = input.tracer;
    this.#clock = input.clock;
    this.#publishes = input.notifier.wants('onPublishFinished');
    this.#receives = input.notifier.wants('onReceiveFinished');
    this.#sends = input.notifier.wants('onTransportSent');
  }

  /** An observation, or undefined when no observer hook and no tracer would see anything. */
  static create(input: BusObservationInput): BusObservation | undefined {
    const observed =
      input.tracer !== undefined ||
      (['onPublishFinished', 'onReceiveFinished', 'onTransportSent', 'onBacklog'] as const).some(
        (method) => input.notifier.wants(method),
      );
    return observed ? new BusObservation(input) : undefined;
  }

  /** Runs a publish call inside the tracer's publish span, reporting its duration and outcome. */
  async publish<T>(
    input: PublishTraceInput,
    run: (headers: EventHeaders) => Promise<T>,
  ): Promise<T> {
    const startedAt = this.#publishes ? this.#now() : 0;
    const tracer = this.#tracer;
    const traced = tracer?.publish === undefined ? run({}) : tracer.publish(input, run);
    if (!this.#publishes) {
      return traced;
    }
    const report = (outcome: OperationOutcome, error?: unknown): void => {
      this.#notifier.notify('onPublishFinished', {
        eventTypes: input.eventTypes,
        durationMs: this.#now() - startedAt,
        outcome,
        ...(error === undefined ? {} : { error: toErrorInfo(error) }),
      });
    };
    try {
      const result = await traced;
      report('success');
      return result;
    } catch (error) {
      report('error', error);
      throw error;
    }
  }

  /** Runs `receive` for one event, reporting where it came from and what became of it. */
  async receive<T extends ReceiveOutcome>(input: ReceiveInput, run: () => Promise<T>): Promise<T> {
    if (!this.#receives) {
      return run();
    }
    const startedAt = this.#now();
    try {
      const outcome = await run();
      this.#reportReceive(input, { outcome, startedAt, failure: undefined });
      return outcome;
    } catch (error) {
      const outcome = error instanceof InvalidEnvelopeError ? 'invalid' : 'error';
      this.#reportReceive(input, { outcome, startedAt, failure: error });
      throw error;
    }
  }

  #reportReceive(
    { envelope, origin }: ReceiveInput,
    report: { readonly outcome: ReceiveOutcome; readonly startedAt: number; failure: unknown },
  ): void {
    // A malformed event may lack its headers: its trace context is then unknown.
    const { headers } = envelope as Partial<EventEnvelope>;
    this.#notifier.notify('onReceiveFinished', {
      transport: origin?.transport.name,
      system: origin === undefined ? undefined : systemOf(origin.transport),
      destination: origin?.destination,
      eventType: textOrUndefined(envelope.type),
      source: textOrUndefined(envelope.source),
      traceContext: isObject(headers) ? traceContextOf(headers) : undefined,
      outcome: report.outcome,
      durationMs: this.#now() - report.startedAt,
      ...(report.failure === undefined ? {} : { error: toErrorInfo(report.failure) }),
    });
  }

  /** The handler of a route: sends inside the tracer's send span and reports each send. */
  sender(transport: Transport, destination: string): (envelope: EventEnvelope) => Promise<void> {
    if (this.#tracer?.send === undefined && !this.#sends) {
      return (envelope) => transport.publish(envelope, destination);
    }
    return (envelope) => this.#send({ envelope, transport, destination });
  }

  async #send({ envelope, transport, destination }: SendInput): Promise<void> {
    const startedAt = this.#sends ? this.#now() : 0;
    const system = systemOf(transport);
    const run = (headers: EventHeaders): Promise<void> =>
      transport.publish(
        Object.keys(headers).length === 0
          ? envelope
          : { ...envelope, headers: { ...envelope.headers, ...headers } },
        destination,
      );
    const tracer = this.#tracer;
    const sent =
      tracer?.send === undefined
        ? run({})
        : tracer.send({ envelope, transport: transport.name, system, destination }, run);
    if (!this.#sends) {
      return sent;
    }
    const report = (outcome: OperationOutcome, error?: unknown): void => {
      this.#notifier.notify('onTransportSent', {
        envelope,
        transport: transport.name,
        system,
        destination,
        durationMs: this.#now() - startedAt,
        outcome,
        ...(error === undefined ? {} : { error: toErrorInfo(error) }),
      });
    };
    try {
      await sent;
      report('success');
    } catch (error) {
      report('error', error);
      throw error;
    }
  }

  /**
   * Reads the backlog every `intervalMs` and reports it, when an observer wants it. A reading that
   * is still running when the next one is due is not overlapped. Returns the cancel of the timer.
   */
  startBacklog({ intervalMs, statistics }: BacklogInput): CancelTimer | undefined {
    if (!this.#notifier.wants('onBacklog')) {
      return undefined;
    }
    let reading = false;
    return schedule(this.#clock, {
      run: () => {
        if (reading) {
          return;
        }
        reading = true;
        statistics()
          .then((result) => this.#reportBacklog(result))
          .catch((error: unknown) => this.#notifier.failure('backlog', error))
          .finally(() => {
            reading = false;
          });
      },
      delayMs: intervalMs,
      repeat: true,
      unref: true,
    });
  }

  #reportBacklog(statistics: EventStoreStatistics): void {
    const oldestDueAt = statistics.oldestDueAt;
    this.#notifier.notify('onBacklog', {
      pending: statistics.pending,
      oldestPendingAgeMs:
        oldestDueAt === null ? 0 : Math.max(0, this.#now() - oldestDueAt.getTime()),
      statistics,
    });
  }

  #now(): number {
    return this.#clock.now().getTime();
  }
}

/** The messaging system of a transport, else its name. */
function systemOf(transport: Transport): string {
  return transport.system ?? transport.name;
}

function textOrUndefined(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function isObject(value: unknown): value is EventHeaders {
  return typeof value === 'object' && value !== null;
}
