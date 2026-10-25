import {
  createTraceState,
  isSpanContextValid,
  trace,
  type Context,
  type SpanContext,
  type TextMapGetter,
  type TextMapPropagator,
  type TextMapSetter,
} from '@opentelemetry/api';
import { TRACEPARENT_HEADER, TRACESTATE_HEADER, traceContextOf } from '@relaybus/core';

const HEX = 16;
const FLAGS_DIGITS = 2;
/** Trace flags are one byte, written as two hex digits. */
const DEFINED_FLAGS = 0xff;

/** A header value as a getter returns it: some carriers repeat headers. */
function firstValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

/**
 * The W3C Trace Context propagator (`traceparent`, `tracestate`), so relaybus propagates traces
 * without `@opentelemetry/core`. Pass another one, such as the global `propagation`, as the
 * `propagator` option of `relayBusOpenTelemetry` to use what the SDK configured.
 */
export class W3CTraceContextPropagator implements TextMapPropagator {
  // eslint-disable-next-line relaybus/max-two-params -- TextMapPropagator.inject signature
  inject(context: Context, carrier: unknown, setter: TextMapSetter): void {
    const spanContext = trace.getSpanContext(context);
    if (spanContext === undefined || !isSpanContextValid(spanContext)) {
      return;
    }
    const flags = (spanContext.traceFlags & DEFINED_FLAGS)
      .toString(HEX)
      .padStart(FLAGS_DIGITS, '0');
    setter.set(
      carrier,
      TRACEPARENT_HEADER,
      `00-${spanContext.traceId}-${spanContext.spanId}-${flags}`,
    );
    const traceState = spanContext.traceState?.serialize();
    if (traceState !== undefined && traceState.length > 0) {
      setter.set(carrier, TRACESTATE_HEADER, traceState);
    }
  }

  // eslint-disable-next-line relaybus/max-two-params -- TextMapPropagator.extract signature
  extract(context: Context, carrier: unknown, getter: TextMapGetter): Context {
    const traceparent = firstValue(getter.get(carrier, TRACEPARENT_HEADER));
    const tracestate = firstValue(getter.get(carrier, TRACESTATE_HEADER));
    const traceContext =
      traceparent === undefined
        ? undefined
        : traceContextOf(tracestate === undefined ? { traceparent } : { traceparent, tracestate });
    if (traceContext === undefined) {
      return context;
    }
    const [, traceId, spanId, flags] = traceContext.traceparent.split('-') as [
      string,
      string,
      string,
      string,
    ];
    const spanContext: SpanContext = {
      traceId,
      spanId,
      traceFlags: Number.parseInt(flags, HEX),
      isRemote: true,
      ...(traceContext.tracestate === undefined
        ? {}
        : { traceState: createTraceState(traceContext.tracestate) }),
    };
    return trace.setSpanContext(context, spanContext);
  }

  fields(): string[] {
    return [TRACEPARENT_HEADER, TRACESTATE_HEADER];
  }
}
