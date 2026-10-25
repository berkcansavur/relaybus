import {
  metrics,
  trace,
  type MeterProvider,
  type TextMapPropagator,
  type TracerProvider,
} from '@opentelemetry/api';
import type { RelayBusObserver, RelayBusTracer } from '@relaybus/core';
import { RelayBusMetricsObserver } from './relay-bus-metrics-observer.js';
import { RelayBusOpenTelemetryTracer } from './relay-bus-open-telemetry-tracer.js';
import { INSTRUMENTATION_SCOPE } from './semantic-conventions.js';
import { W3CTraceContextPropagator } from './w3c-trace-context-propagator.js';

export interface RelayBusOpenTelemetryOptions {
  /** Default: the global meter provider (`metrics.getMeterProvider()`). */
  readonly meterProvider?: MeterProvider;
  /** Default: the global tracer provider (`trace.getTracerProvider()`). */
  readonly tracerProvider?: TracerProvider;
  /**
   * Writes and reads the trace context of events. Default: W3C Trace Context (`traceparent`,
   * `tracestate`). Pass `propagation` from `@opentelemetry/api` to use the SDK's global one.
   */
  readonly propagator?: TextMapPropagator;
}

/** What `relayBusOpenTelemetry` returns: pass both to the bus. */
export interface RelayBusOpenTelemetry {
  /** Metrics; add it to `observers`. */
  readonly observer: RelayBusObserver;
  /** Spans and trace propagation; pass it as `tracer`. */
  readonly tracer: RelayBusTracer;
}

/**
 * OpenTelemetry metrics and traces for a relaybus.
 *
 * ```ts
 * const otel = relayBusOpenTelemetry();
 * const bus = new RelayBus({ store, observers: [otel.observer], tracer: otel.tracer, backlogInterval: '30s' });
 * ```
 */
export function relayBusOpenTelemetry(
  options: RelayBusOpenTelemetryOptions = {},
): RelayBusOpenTelemetry {
  const meterProvider = options.meterProvider ?? metrics.getMeterProvider();
  const tracerProvider = options.tracerProvider ?? trace.getTracerProvider();
  return {
    observer: new RelayBusMetricsObserver(meterProvider.getMeter(INSTRUMENTATION_SCOPE)),
    tracer: new RelayBusOpenTelemetryTracer({
      tracer: tracerProvider.getTracer(INSTRUMENTATION_SCOPE),
      propagator: options.propagator ?? new W3CTraceContextPropagator(),
    }),
  };
}
