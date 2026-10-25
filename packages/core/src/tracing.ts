import type { EventEnvelope, EventHeaders } from './event.js';
import type { ClaimedDelivery } from './event-store.js';

/** Event header that carries the W3C trace context (`version-traceid-parentid-flags`). */
export const TRACEPARENT_HEADER = 'traceparent';
/** Event header that carries the vendor part of the W3C trace context. */
export const TRACESTATE_HEADER = 'tracestate';

/** A W3C Trace Context as it travels in event headers. */
export interface TraceContext {
  readonly traceparent: string;
  readonly tracestate?: string;
}

/** `<version>-<trace id>-<span id>-<flags>`, lower-case hex, ids not all zero. */
const TRACEPARENT_FIELDS = '(?!0{32})[\\da-f]{32}-(?!0{16})[\\da-f]{16}-[\\da-f]{2}';
/** Version 00 has exactly four fields; later versions (never ff) may append more. */
const TRACEPARENT = new RegExp(
  `^(?:00-${TRACEPARENT_FIELDS}|(?!00|ff)[\\da-f]{2}-${TRACEPARENT_FIELDS}(?:-.*)?)$`,
);

/**
 * The W3C trace context an event carries in its headers, or undefined when it has none or a
 * malformed one. A handler or a consumer span continues the publisher's trace from it.
 *
 * ```ts
 * bus.subscribe(OrderPlaced, (event) => startSpan({ parent: traceContextOf(event.headers) }), ...)
 * ```
 */
export function traceContextOf(headers: EventHeaders): TraceContext | undefined {
  const traceparent = headers[TRACEPARENT_HEADER];
  if (traceparent === undefined || !TRACEPARENT.test(traceparent)) {
    return undefined;
  }
  const tracestate = headers[TRACESTATE_HEADER];
  return tracestate === undefined ? { traceparent } : { traceparent, tracestate };
}

/** A `publish` or `publishAll` call. */
export interface PublishTraceInput {
  /** The type of every event of the call, in order. */
  readonly eventTypes: readonly string[];
}

/** A route sending one event to its transport. */
export interface SendTraceInput {
  readonly envelope: EventEnvelope;
  readonly transport: string;
  /** The messaging system of the transport (`kafka`, `rabbitmq`), else its name. */
  readonly system: string;
  readonly destination: string;
}

/** A local handler running for one claimed delivery. */
export interface ProcessTraceInput {
  readonly delivery: ClaimedDelivery;
}

/**
 * Tracing hooks, for example from `@relaybus/otel`. Each one wraps an operation: it starts a span,
 * calls `run` inside it and ends the span when the returned promise settles, passing its result or
 * error through unchanged. relaybus itself depends on no tracing library.
 */
export interface RelayBusTracer {
  /**
   * Wraps one publish call. `run` takes the trace headers (`traceparent`, `tracestate`) to write
   * into every event of the call, so its consumers continue the trace; headers passed to
   * `publish` explicitly win.
   */
  publish?<T>(input: PublishTraceInput, run: (headers: EventHeaders) => Promise<T>): Promise<T>;
  /**
   * Wraps one send of a route. `run` takes the trace headers to send the event with, in place of
   * the ones it was stored with, so the consumer's trace continues from this send.
   */
  send?<T>(input: SendTraceInput, run: (headers: EventHeaders) => Promise<T>): Promise<T>;
  /**
   * Wraps one run of a local handler; the handler runs inside `run`, so what it publishes or calls
   * joins the span. The event's own trace context is in `traceContextOf(delivery.event.headers)`.
   */
  process?<T>(input: ProcessTraceInput, run: () => Promise<T>): Promise<T>;
}
