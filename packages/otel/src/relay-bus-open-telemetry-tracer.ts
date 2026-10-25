import {
  SpanKind,
  SpanStatusCode,
  context,
  defaultTextMapGetter,
  defaultTextMapSetter,
  trace,
  type Attributes,
  type Context,
  type Link,
  type Span,
  type TextMapPropagator,
  type Tracer,
} from '@opentelemetry/api';
import {
  toErrorInfo,
  type EventHeaders,
  type ProcessTraceInput,
  type PublishTraceInput,
  type RelayBusTracer,
  type SendTraceInput,
} from '@relaybus/core';
import { ATTRIBUTES, RELAYBUS_SYSTEM, errorType } from './semantic-conventions.js';

type TracerInput = {
  readonly tracer: Tracer;
  readonly propagator: TextMapPropagator;
};

/** Ends `span` when `work` settles, marking it failed with the error's type when it rejects. */
async function endWhenSettled<T>(span: Span, work: Promise<T>): Promise<T> {
  try {
    return await work;
  } catch (error) {
    const info = toErrorInfo(error);
    span.recordException(error instanceof Error ? error : info.message);
    span.setAttribute(ATTRIBUTES.errorType, errorType(info));
    span.setStatus({ code: SpanStatusCode.ERROR, message: info.message });
    throw error;
  } finally {
    span.end();
  }
}

/**
 * relaybus tracing hooks on an OpenTelemetry tracer: `publish <type>` and `send <destination>`
 * producer spans, `process <type>` consumer spans, and the trace context in event headers.
 */
export class RelayBusOpenTelemetryTracer implements RelayBusTracer {
  readonly #tracer: Tracer;
  readonly #propagator: TextMapPropagator;

  constructor({ tracer, propagator }: TracerInput) {
    this.#tracer = tracer;
    this.#propagator = propagator;
  }

  /** A producer span, child of the caller's active span; its context goes into every event. */
  publish<T>(input: PublishTraceInput, run: (headers: EventHeaders) => Promise<T>): Promise<T> {
    const types = [...new Set(input.eventTypes)];
    const attributes: Attributes = {
      [ATTRIBUTES.system]: RELAYBUS_SYSTEM,
      [ATTRIBUTES.operationName]: 'publish',
      [ATTRIBUTES.operationType]: 'create',
      ...(types.length === 1 ? { [ATTRIBUTES.destination]: types[0] } : {}),
      ...(input.eventTypes.length > 1 ? { [ATTRIBUTES.batchCount]: input.eventTypes.length } : {}),
    };
    const name = types.length === 1 ? `publish ${types[0]}` : 'publish';
    return this.#runInSpan(
      { name, kind: SpanKind.PRODUCER, attributes, parent: context.active() },
      run,
    );
  }

  /**
   * A producer span, child of the publish span whose context the stored event carries (and linked
   * to it); the event goes to the broker with this span's context.
   */
  send<T>(input: SendTraceInput, run: (headers: EventHeaders) => Promise<T>): Promise<T> {
    const { envelope } = input;
    const parent = this.#extract(envelope.headers);
    const published = trace.getSpanContext(parent);
    const attributes: Attributes = {
      [ATTRIBUTES.system]: input.system,
      [ATTRIBUTES.destination]: input.destination,
      [ATTRIBUTES.operationName]: 'send',
      [ATTRIBUTES.operationType]: 'send',
      [ATTRIBUTES.messageId]: envelope.id,
      [ATTRIBUTES.eventType]: envelope.type,
      ...(envelope.correlationId === null
        ? {}
        : { [ATTRIBUTES.conversationId]: envelope.correlationId }),
    };
    return this.#runInSpan(
      {
        name: `send ${input.destination}`,
        kind: SpanKind.PRODUCER,
        attributes,
        parent,
        ...(published === undefined ? {} : { links: [{ context: published }] }),
      },
      run,
    );
  }

  /** A consumer span, child of the trace context the event arrived with, active while the handler runs. */
  process<T>(input: ProcessTraceInput, run: () => Promise<T>): Promise<T> {
    const { delivery } = input;
    const parent = this.#extract(delivery.event.headers);
    const span = this.#tracer.startSpan(
      `process ${delivery.event.type}`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          [ATTRIBUTES.system]: RELAYBUS_SYSTEM,
          [ATTRIBUTES.destination]: delivery.event.type,
          [ATTRIBUTES.operationName]: 'process',
          [ATTRIBUTES.operationType]: 'process',
          [ATTRIBUTES.consumerGroup]: delivery.handler,
          [ATTRIBUTES.messageId]: delivery.event.id,
          [ATTRIBUTES.deliveryAttempt]: delivery.attempt,
        },
      },
      parent,
    );
    return endWhenSettled(span, context.with(trace.setSpan(parent, span), run));
  }

  #extract(headers: EventHeaders): Context {
    return this.#propagator.extract(context.active(), headers, defaultTextMapGetter);
  }

  #runInSpan<T>(
    span: {
      readonly name: string;
      readonly kind: SpanKind;
      readonly attributes: Attributes;
      readonly parent: Context;
      readonly links?: Link[];
    },
    run: (headers: EventHeaders) => Promise<T>,
  ): Promise<T> {
    const started = this.#tracer.startSpan(
      span.name,
      {
        kind: span.kind,
        attributes: span.attributes,
        ...(span.links === undefined ? {} : { links: span.links }),
      },
      span.parent,
    );
    const active = trace.setSpan(span.parent, started);
    const headers: Record<string, string> = {};
    this.#propagator.inject(active, headers, defaultTextMapSetter);
    return endWhenSettled(
      started,
      context.with(active, () => run(headers)),
    );
  }
}
