import { context } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
  type MetricData,
} from '@opentelemetry/sdk-metrics';
import {
  TracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from '@opentelemetry/sdk-trace';
import { relayBusOpenTelemetry, type RelayBusOpenTelemetry } from '../src/index.js';

/** One metric point: its attributes and value (a number, or a histogram's count and sum). */
export interface Point {
  readonly attributes: Record<string, unknown>;
  readonly value: number | { readonly count: number; readonly sum: number | undefined };
}

export interface Telemetry {
  readonly otel: RelayBusOpenTelemetry;
  readonly tracerProvider: TracerProvider;
  /** Spans ended so far, in the order they ended (SDK 3 exports a span asynchronously, so this flushes first). */
  spans(): Promise<ReadableSpan[]>;
  /** Every point of every metric collected now, by metric name. */
  collect(): Promise<Map<string, { metric: MetricData; points: Point[] }>>;
  shutdown(): Promise<void>;
}

/**
 * Makes the active span follow async calls, as the OpenTelemetry Node SDK does. Returns the
 * function that undoes it.
 */
export function enableContextPropagation(): () => void {
  const manager = new AsyncLocalStorageContextManager().enable();
  context.setGlobalContextManager(manager);
  return () => {
    context.disable();
  };
}

/** relaybus OpenTelemetry on in-memory span and metric exporters. */
export function createTelemetry(): Telemetry {
  const spanExporter = new InMemorySpanExporter();
  const tracerProvider = new TracerProvider({
    spanProcessors: [new SimpleSpanProcessor({ exporter: spanExporter })],
  });
  const metricExporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({
    exporter: metricExporter,
    // Exports only when flushed by the test.
    exportIntervalMillis: 3_600_000,
  });
  const meterProvider = new MeterProvider({ readers: [reader] });
  return {
    otel: relayBusOpenTelemetry({ meterProvider, tracerProvider }),
    tracerProvider,
    async spans() {
      await tracerProvider.forceFlush();
      return spanExporter.getFinishedSpans();
    },
    async collect() {
      await reader.forceFlush();
      const latest = metricExporter.getMetrics().at(-1);
      const result = new Map<string, { metric: MetricData; points: Point[] }>();
      for (const metric of latest?.scopeMetrics.flatMap((scope) => scope.metrics) ?? []) {
        result.set(metric.descriptor.name, {
          metric,
          points: metric.dataPoints.map((point) => ({
            attributes: point.attributes,
            value:
              typeof point.value === 'number'
                ? point.value
                : { count: point.value.count, sum: point.value.sum },
          })),
        });
      }
      metricExporter.reset();
      return result;
    },
    async shutdown() {
      await meterProvider.shutdown();
      await tracerProvider.shutdown();
    },
  };
}
