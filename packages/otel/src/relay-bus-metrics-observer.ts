import type { Attributes, Counter, Histogram, Meter } from '@opentelemetry/api';
import type {
  BacklogEvent,
  ClaimFinishedEvent,
  ClaimedDelivery,
  ErrorInfo,
  FailureStage,
  HandlerFinishedEvent,
  HandlerStartedEvent,
  PublishFinishedEvent,
  ReceiveFinishedEvent,
  RelayBusObserver,
  TransportSentEvent,
} from '@relaybus/core';
import { ATTRIBUTES, METRICS, RELAYBUS_SYSTEM, errorType } from './semantic-conventions.js';

const MILLISECONDS_PER_SECOND = 1_000;

/* eslint-disable no-magic-numbers -- histogram bucket boundaries */
/** Bucket boundaries in seconds the messaging semantic conventions advise for durations. */
const DURATION_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];

/** End-to-end latency spans retries and delays: buckets up to an hour. */
const LATENCY_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3_600,
];

const COUNT_BUCKETS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000];
/* eslint-enable no-magic-numbers */

/** The metric instruments, created once per meter. */
type Instruments = {
  readonly operationDuration: Histogram;
  readonly sentMessages: Counter;
  readonly consumedMessages: Counter;
  readonly processDuration: Histogram;
  readonly published: Counter;
  readonly claimedDeliveries: Histogram;
  readonly claimedLanes: Histogram;
  readonly deadLettered: Counter;
  readonly retried: Counter;
  readonly leaseLost: Counter;
  readonly endToEndLatency: Histogram;
  readonly failures: Counter;
};

function seconds(milliseconds: number): number {
  return milliseconds / MILLISECONDS_PER_SECOND;
}

function withError(attributes: Attributes, error: ErrorInfo | undefined): Attributes {
  return error === undefined
    ? attributes
    : { ...attributes, [ATTRIBUTES.errorType]: errorType(error) };
}

/** Attributes of a delivery: the handler is the consumer group, the event type the destination. */
function deliveryAttributes(delivery: ClaimedDelivery): Attributes {
  return {
    [ATTRIBUTES.system]: RELAYBUS_SYSTEM,
    [ATTRIBUTES.destination]: delivery.event.type,
    [ATTRIBUTES.consumerGroup]: delivery.handler,
  };
}

type HistogramOptions = { unit: string; description: string; buckets: number[] };

function createHistograms(
  meter: Meter,
): Pick<
  Instruments,
  'operationDuration' | 'processDuration' | 'claimedDeliveries' | 'claimedLanes' | 'endToEndLatency'
> {
  const histogram = (name: string, options: HistogramOptions): Histogram =>
    meter.createHistogram(name, {
      unit: options.unit,
      description: options.description,
      advice: { explicitBucketBoundaries: options.buckets },
    });
  return {
    operationDuration: histogram(METRICS.operationDuration, {
      unit: 's',
      description: 'Duration of publish, claim, send and receive operations.',
      buckets: DURATION_BUCKETS,
    }),
    processDuration: histogram(METRICS.processDuration, {
      unit: 's',
      description: 'Duration of local handler runs.',
      buckets: DURATION_BUCKETS,
    }),
    claimedDeliveries: histogram(METRICS.claimedDeliveries, {
      unit: '{delivery}',
      description: 'Deliveries one claim query returned.',
      buckets: COUNT_BUCKETS,
    }),
    claimedLanes: histogram(METRICS.claimedLanes, {
      unit: '{lane}',
      description: 'Distinct leases (lanes) among the deliveries of one claim.',
      buckets: COUNT_BUCKETS,
    }),
    endToEndLatency: histogram(METRICS.endToEndLatency, {
      unit: 's',
      description: 'Time from an event occurring to a handler or route starting on it.',
      buckets: LATENCY_BUCKETS,
    }),
  };
}

function createInstruments(meter: Meter): Instruments {
  const counter = (name: string, [unit, description]: [string, string]): Counter =>
    meter.createCounter(name, { unit, description });
  return {
    ...createHistograms(meter),
    sentMessages: counter(METRICS.sentMessages, [
      '{message}',
      'Events a route attempted to send to a broker.',
    ]),
    consumedMessages: counter(METRICS.consumedMessages, [
      '{message}',
      'Events received from a broker, by what receive did with them.',
    ]),
    published: counter(METRICS.published, ['{event}', 'Events passed to publish.']),
    deadLettered: counter(METRICS.deadLettered, ['{delivery}', 'Deliveries moved to dead.']),
    retried: counter(METRICS.retried, ['{delivery}', 'Failed attempts rescheduled for a retry.']),
    leaseLost: counter(METRICS.leaseLost, [
      '{delivery}',
      'Deliveries whose lease was lost while they ran.',
    ]),
    failures: counter(METRICS.failures, [
      '{failure}',
      'Errors relaybus caught and reported, by stage.',
    ]),
  };
}

/**
 * Turns relaybus observer hooks into OpenTelemetry metrics. The backlog gauges report the last
 * reading of `onBacklog`, so they need `backlogInterval` on the bus.
 */
export class RelayBusMetricsObserver implements RelayBusObserver {
  readonly #instruments: Instruments;
  #backlog: BacklogEvent | undefined;

  constructor(meter: Meter) {
    this.#instruments = createInstruments(meter);
    meter
      .createObservableGauge(METRICS.pending, {
        unit: '{delivery}',
        description: 'Deliveries waiting to run, at the last backlog reading.',
      })
      .addCallback((result) => {
        if (this.#backlog !== undefined) {
          result.observe(this.#backlog.pending);
        }
      });
    meter
      .createObservableGauge(METRICS.oldestAge, {
        unit: 's',
        description: 'How long the oldest due delivery had waited, at the last backlog reading.',
      })
      .addCallback((result) => {
        if (this.#backlog !== undefined) {
          result.observe(seconds(this.#backlog.oldestPendingAgeMs));
        }
      });
  }

  onPublishFinished(event: PublishFinishedEvent): void {
    const types = new Set(event.eventTypes);
    const base: Attributes = {
      [ATTRIBUTES.system]: RELAYBUS_SYSTEM,
      [ATTRIBUTES.operationName]: 'publish',
      [ATTRIBUTES.operationType]: 'create',
    };
    const [onlyType] = types;
    this.#instruments.operationDuration.record(
      seconds(event.durationMs),
      withError(
        types.size === 1 ? { ...base, [ATTRIBUTES.destination]: onlyType } : base,
        event.error,
      ),
    );
    for (const type of event.eventTypes) {
      this.#instruments.published.add(
        1,
        withError(
          { [ATTRIBUTES.system]: RELAYBUS_SYSTEM, [ATTRIBUTES.destination]: type },
          event.error,
        ),
      );
    }
  }

  onClaimFinished(event: ClaimFinishedEvent): void {
    const attributes: Attributes = {
      [ATTRIBUTES.system]: RELAYBUS_SYSTEM,
      [ATTRIBUTES.operationName]: 'claim',
      [ATTRIBUTES.operationType]: 'receive',
    };
    this.#instruments.operationDuration.record(
      seconds(event.durationMs),
      withError(attributes, event.error),
    );
    if (event.outcome === 'success') {
      this.#instruments.claimedDeliveries.record(event.claimed, attributes);
      this.#instruments.claimedLanes.record(event.lanes, attributes);
    }
  }

  onHandlerStarted(event: HandlerStartedEvent): void {
    this.#instruments.endToEndLatency.record(seconds(event.endToEndLatencyMs), {
      ...deliveryAttributes(event.delivery),
      [ATTRIBUTES.deliveryKind]: event.kind,
    });
  }

  onHandlerFinished(event: HandlerFinishedEvent): void {
    // A route's run is its send, measured by onTransportSent.
    if (event.kind === 'handler') {
      this.#instruments.processDuration.record(
        seconds(event.durationMs),
        withError(
          {
            ...deliveryAttributes(event.delivery),
            [ATTRIBUTES.operationName]: 'process',
            [ATTRIBUTES.operationType]: 'process',
          },
          event.error,
        ),
      );
    }
  }

  onTransportSent(event: TransportSentEvent): void {
    const attributes = withError(
      {
        [ATTRIBUTES.system]: event.system,
        [ATTRIBUTES.destination]: event.destination,
        [ATTRIBUTES.operationName]: 'send',
        [ATTRIBUTES.operationType]: 'send',
      },
      event.error,
    );
    this.#instruments.sentMessages.add(1, attributes);
    this.#instruments.operationDuration.record(seconds(event.durationMs), attributes);
  }

  onReceiveFinished(event: ReceiveFinishedEvent): void {
    const attributes: Attributes = withError(
      {
        [ATTRIBUTES.system]: event.system ?? RELAYBUS_SYSTEM,
        [ATTRIBUTES.operationName]: 'receive',
        [ATTRIBUTES.operationType]: 'receive',
        ...(event.destination === undefined ? {} : { [ATTRIBUTES.destination]: event.destination }),
      },
      event.error,
    );
    this.#instruments.consumedMessages.add(1, {
      ...attributes,
      [ATTRIBUTES.receiveOutcome]: event.outcome,
    });
    this.#instruments.operationDuration.record(seconds(event.durationMs), attributes);
  }

  onDeliveryRetryScheduled(event: {
    readonly delivery: ClaimedDelivery;
    readonly error: ErrorInfo;
  }): void {
    this.#instruments.retried.add(1, withError(deliveryAttributes(event.delivery), event.error));
  }

  onDeliveryDead(event: { readonly delivery: ClaimedDelivery; readonly error: ErrorInfo }): void {
    this.#instruments.deadLettered.add(
      1,
      withError(deliveryAttributes(event.delivery), event.error),
    );
  }

  onLeaseLost(event: { readonly delivery: ClaimedDelivery }): void {
    this.#instruments.leaseLost.add(1, deliveryAttributes(event.delivery));
  }

  onBacklog(event: BacklogEvent): void {
    this.#backlog = event;
  }

  onFailure(event: { readonly stage: FailureStage; readonly error: ErrorInfo }): void {
    this.#instruments.failures.add(
      1,
      withError({ [ATTRIBUTES.failureStage]: event.stage }, event.error),
    );
  }
}
