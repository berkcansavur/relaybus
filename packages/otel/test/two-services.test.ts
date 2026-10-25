import { SpanKind } from '@opentelemetry/api';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  defineEvent,
  type InMemoryTransaction,
  type RelayBusObserver,
  type RelayBusTracer,
} from '@relaybus/core';
import { FakeClock, InMemoryTransport } from '@relaybus/testing';
import { createTelemetry, enableContextPropagation, type Telemetry } from './support.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class InvoiceCreated extends defineEvent<{ orderId: string }>('invoice.created.v1') {}

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

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

interface Services {
  readonly orders: RelayBus<InMemoryTransaction>;
  readonly billing: RelayBus<InMemoryTransaction>;
  readonly clock: FakeClock;
  /** Carries what the orders service sent to the billing service, like a broker. */
  forward(): Promise<void>;
}

/** An orders service routing OrderPlaced to a broker, and a billing service consuming it. */
async function createServices(
  observability: {
    readonly observers: readonly RelayBusObserver[];
    readonly tracer: RelayBusTracer;
  },
  handle: (bus: RelayBus<InMemoryTransaction>) => Promise<void>,
): Promise<Services> {
  const clock = new FakeClock();
  const outbound = Object.assign(new InMemoryTransport('broker'), { system: 'rabbitmq' });
  const inbound = Object.assign(new InMemoryTransport('broker'), { system: 'rabbitmq' });
  const common = {
    clock,
    logger: silent,
    transactionMode: 'optional' as const,
    dispatcher: { enabled: false, completionBatchWindow: 0 },
    ...observability,
  };
  const orders = new RelayBus<InMemoryTransaction>({
    ...common,
    store: new InMemoryEventStore(),
    source: 'orders',
    transports: [outbound],
    routes: [{ event: OrderPlaced, transport: 'broker', destination: 'orders' }],
  });
  const billing = new RelayBus<InMemoryTransaction>({
    ...common,
    store: new InMemoryEventStore(),
    source: 'billing',
    transports: [inbound],
    subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
  });
  billing.subscribe(OrderPlaced, () => handle(billing), { id: 'billing.on-order-placed' });
  billing.subscribe(InvoiceCreated, async () => undefined, { id: 'billing.on-invoice-created' });
  await billing.start();
  return {
    orders,
    billing,
    clock,
    async forward() {
      for (const { destination, envelope } of outbound.published.splice(0)) {
        await inbound.deliver(destination, envelope);
      }
    },
  };
}

function only(spans: ReadableSpan[], name: string): ReadableSpan {
  const matching = spans.filter((span) => span.name === name);
  expect(matching.map((span) => span.name)).toEqual([name]);
  return matching[0]!;
}

describe('one trace from publish in one service to process in another', () => {
  it('chains publish, send and process spans and continues into what the handler publishes', async () => {
    telemetry = createTelemetry();
    const services = await createServices(
      { observers: [telemetry.otel.observer], tracer: telemetry.otel.tracer },
      async (bus) => {
        await bus.publish(new InvoiceCreated({ orderId: 'o-1' }));
      },
    );
    const request = telemetry.tracerProvider.getTracer('test').startSpan('POST /orders');
    const { context, trace } = await import('@opentelemetry/api');
    await context.with(trace.setSpan(context.active(), request), () =>
      services.orders.publish(new OrderPlaced({ orderId: 'o-1' }), { key: 'o-1' }),
    );
    request.end();
    await services.orders.drain();
    await services.forward();
    await services.billing.drain();
    await services.billing.stop();

    const spans = telemetry.spans();
    const publish = only(spans, 'publish order.placed.v1');
    const send = only(spans, 'send orders');
    const process = only(spans, 'process order.placed.v1');
    const nested = only(spans, 'publish invoice.created.v1');
    const traceId = request.spanContext().traceId;
    expect(spans.every((span) => span.spanContext().traceId === traceId)).toBe(true);
    expect(publish.parentSpanContext?.spanId).toBe(request.spanContext().spanId);
    expect(send.parentSpanContext?.spanId).toBe(publish.spanContext().spanId);
    expect(send.links.map((link) => link.context.spanId)).toEqual([publish.spanContext().spanId]);
    expect(process.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
    expect(nested.parentSpanContext?.spanId).toBe(process.spanContext().spanId);
    expect([publish.kind, send.kind, process.kind]).toEqual([
      SpanKind.PRODUCER,
      SpanKind.PRODUCER,
      SpanKind.CONSUMER,
    ]);
    expect(publish.attributes).toEqual({
      'messaging.system': 'relaybus',
      'messaging.operation.name': 'publish',
      'messaging.operation.type': 'create',
      'messaging.destination.name': 'order.placed.v1',
    });
    expect(send.attributes).toMatchObject({
      'messaging.system': 'rabbitmq',
      'messaging.destination.name': 'orders',
      'messaging.operation.name': 'send',
      'messaging.operation.type': 'send',
      'relaybus.event.type': 'order.placed.v1',
    });
    expect(process.attributes).toMatchObject({
      'messaging.system': 'relaybus',
      'messaging.destination.name': 'order.placed.v1',
      'messaging.operation.name': 'process',
      'messaging.consumer.group.name': 'billing.on-order-placed',
      'relaybus.delivery.attempt': 1,
    });
  });
});
