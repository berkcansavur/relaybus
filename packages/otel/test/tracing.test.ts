import {
  ROOT_CONTEXT,
  SpanStatusCode,
  createTraceState,
  defaultTextMapGetter,
  defaultTextMapSetter,
  propagation,
  trace,
  type TextMapGetter,
} from '@opentelemetry/api';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  defineEvent,
  type InMemoryTransaction,
} from '@relaybus/core';
import { FakeClock } from '@relaybus/testing';
import { W3CTraceContextPropagator, relayBusOpenTelemetry } from '../src/index.js';
import { createTelemetry, enableContextPropagation, type Telemetry } from './support.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };
const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';
const SPAN_ID = 'b7ad6b7169203331';

let disableContext: () => void;
let telemetry: Telemetry;

beforeAll(() => {
  disableContext = enableContextPropagation();
});

afterAll(() => {
  disableContext();
});

afterEach(async () => {
  await telemetry.shutdown();
});

function createBus(): RelayBus<InMemoryTransaction> {
  telemetry = createTelemetry();
  return new RelayBus<InMemoryTransaction>({
    store: new InMemoryEventStore(),
    clock: new FakeClock(),
    logger: silent,
    transactionMode: 'optional',
    dispatcher: { enabled: false, completionBatchWindow: 0 },
    retry: { attempts: 1 },
    observers: [telemetry.otel.observer],
    tracer: telemetry.otel.tracer,
  });
}

describe('span outcomes', () => {
  it('marks a failing handler span with the error and its type', async () => {
    const bus = createBus();
    bus.subscribe(
      OrderPlaced,
      async () => {
        throw new Error('payment provider down');
      },
      { id: 'billing' },
    );
    bus.subscribe(
      OrderShipped,
      async () => {
        // A handler may throw anything.
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw 'not an error';
      },
      { id: 'shipping' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.publish(new OrderShipped({ orderId: '1' }));
    await bus.drain();
    const placed = telemetry.spans().find((span) => span.name === 'process order.placed.v1')!;
    expect(placed.status).toEqual({ code: SpanStatusCode.ERROR, message: 'payment provider down' });
    expect(placed.attributes['error.type']).toBe('Error');
    expect(placed.events.map((event) => event.name)).toEqual(['exception']);
    const shipped = telemetry.spans().find((span) => span.name === 'process order.shipped.v1')!;
    expect(shipped.status.code).toBe(SpanStatusCode.ERROR);
    expect(shipped.events[0]!.attributes).toMatchObject({ 'exception.message': 'not an error' });
  });

  it('marks a failed publish and names a publish of several types without a destination', async () => {
    const bus = createBus();
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    bus.subscribe(OrderShipped, async () => undefined, { id: 'shipping' });
    await bus.publishAll([new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })]);
    await expect(bus.publish(new OrderPlaced({ orderId: '1' }), { key: '' })).rejects.toThrow(
      'key must not be empty',
    );
    const [mixed, failed] = telemetry.spans();
    expect(mixed!.name).toBe('publish');
    expect(mixed!.attributes).toEqual({
      'messaging.system': 'relaybus',
      'messaging.operation.name': 'publish',
      'messaging.operation.type': 'create',
      'messaging.batch.message_count': 2,
    });
    expect(failed!.attributes['error.type']).toBe('RELAYBUS_CONFIGURATION');
    expect(failed!.status.code).toBe(SpanStatusCode.ERROR);
  });

  it('starts a root process span for an event without trace context', async () => {
    const bus = createBus();
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.receive({
      id: 'evt-1',
      type: 'order.placed.v1',
      source: 'legacy',
      key: null,
      payload: { orderId: '1' },
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    });
    await bus.drain();
    const [process] = telemetry.spans();
    expect(process!.name).toBe('process order.placed.v1');
    expect(process!.parentSpanContext).toBeUndefined();
  });
});

describe('send spans', () => {
  it('starts a root send span without a link for an event stored without trace context', async () => {
    telemetry = createTelemetry();
    const envelope = {
      id: 'evt-1',
      type: 'order.placed.v1',
      source: 'orders',
      key: null,
      payload: {},
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    const sent = await telemetry.otel.tracer.send!(
      { envelope, transport: 'broker', system: 'kafka', destination: 'orders' },
      async (headers) => headers,
    );
    const [send] = telemetry.spans();
    expect(sent['traceparent']).toContain(send!.spanContext().spanId);
    expect(send!.parentSpanContext).toBeUndefined();
    expect(send!.links).toEqual([]);
    expect(send!.attributes['messaging.message.conversation_id']).toBeUndefined();
  });
});

describe('W3CTraceContextPropagator', () => {
  const propagator = new W3CTraceContextPropagator();
  const spanContext = {
    traceId: TRACE_ID,
    spanId: SPAN_ID,
    traceFlags: 1,
    traceState: createTraceState('congo=t61,rojo=00f067aa0ba902b7'),
  };

  it('writes and reads traceparent and tracestate', () => {
    const carrier: Record<string, string> = {};
    propagator.inject(
      trace.setSpanContext(ROOT_CONTEXT, spanContext),
      carrier,
      defaultTextMapSetter,
    );
    expect(carrier).toEqual({
      traceparent: `00-${TRACE_ID}-${SPAN_ID}-01`,
      tracestate: 'congo=t61,rojo=00f067aa0ba902b7',
    });
    const extracted = trace.getSpanContext(
      propagator.extract(ROOT_CONTEXT, carrier, defaultTextMapGetter),
    );
    expect(extracted).toMatchObject({ traceId: TRACE_ID, spanId: SPAN_ID, traceFlags: 1 });
    expect(extracted!.isRemote).toBe(true);
    expect(extracted!.traceState!.get('congo')).toBe('t61');
    expect(propagator.fields()).toEqual(['traceparent', 'tracestate']);
  });

  it('writes nothing for an invalid span context and no tracestate when it is empty', () => {
    const none: Record<string, string> = {};
    propagator.inject(ROOT_CONTEXT, none, defaultTextMapSetter);
    expect(none).toEqual({});
    const plain: Record<string, string> = {};
    propagator.inject(
      trace.setSpanContext(ROOT_CONTEXT, { traceId: TRACE_ID, spanId: SPAN_ID, traceFlags: 0 }),
      plain,
      defaultTextMapSetter,
    );
    expect(plain).toEqual({ traceparent: `00-${TRACE_ID}-${SPAN_ID}-00` });
    const emptyState: Record<string, string> = {};
    propagator.inject(
      trace.setSpanContext(ROOT_CONTEXT, { ...spanContext, traceState: createTraceState('') }),
      emptyState,
      defaultTextMapSetter,
    );
    expect(emptyState).toEqual({ traceparent: `00-${TRACE_ID}-${SPAN_ID}-01` });
  });

  it('reads repeated headers by their first value and ignores malformed ones', () => {
    const repeated: TextMapGetter<Record<string, string[]>> = {
      get: (carrier, key) => carrier[key],
      keys: (carrier) => Object.keys(carrier),
    };
    const context = propagator.extract(
      ROOT_CONTEXT,
      { traceparent: [`00-${TRACE_ID}-${SPAN_ID}-01`, 'ignored'] },
      repeated,
    );
    expect(trace.getSpanContext(context)?.traceState).toBeUndefined();
    expect(trace.getSpanContext(context)?.spanId).toBe(SPAN_ID);
    for (const carrier of [{}, { traceparent: 'garbage' }]) {
      expect(propagator.extract(ROOT_CONTEXT, carrier, defaultTextMapGetter)).toBe(ROOT_CONTEXT);
    }
  });
});

describe('relayBusOpenTelemetry', () => {
  it('works on the global providers and a given propagator', async () => {
    telemetry = createTelemetry();
    const otel = relayBusOpenTelemetry({ propagator: propagation });
    const bus = new RelayBus<InMemoryTransaction>({
      store: new InMemoryEventStore(),
      logger: silent,
      transactionMode: 'optional',
      observers: [otel.observer],
      tracer: otel.tracer,
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    const envelope = await bus.publish(new OrderPlaced({ orderId: '1' }));
    // No SDK registered: the global providers are no-ops and nothing is propagated.
    expect(envelope.headers).toEqual({});
    expect(await bus.drain()).toBe(1);
  });
});
