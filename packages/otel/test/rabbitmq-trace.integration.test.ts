import { context, trace } from '@opentelemetry/api';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { InMemoryEventStore, RelayBus, defineEvent } from '@relaybus/core';
import { RabbitMqTransport } from '@relaybus/rabbitmq';
import { startBroker, unique, type TestBroker } from '../../rabbitmq/test/support/broker.js';
import { createTelemetry, enableContextPropagation, type Telemetry } from './support.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: TestBroker;
let disableContext: () => void;

beforeAll(async () => {
  broker = await startBroker();
  disableContext = enableContextPropagation();
});

afterAll(async () => {
  disableContext();
  await broker.stop();
});

/** A service with its own outbox, telemetry and RabbitMQ connection. */
function createService(
  source: string,
  topology: { exchange: string; telemetry: Telemetry; consumerGroup?: string },
): RelayBus {
  const consumes = topology.consumerGroup !== undefined;
  return new RelayBus({
    store: new InMemoryEventStore(),
    source,
    transactionMode: 'optional',
    transports: [
      new RabbitMqTransport({
        name: 'rabbit',
        url: broker.getAmqpUrl(),
        exchange: topology.exchange,
        ...(consumes ? { consumerGroup: topology.consumerGroup } : {}),
        logger: silent,
      }),
    ],
    routes: consumes
      ? []
      : [{ event: OrderPlaced, transport: 'rabbit', destination: 'orders.placed' }],
    subscriptions: consumes ? [{ transport: 'rabbit', destinations: ['orders.*'] }] : [],
    dispatcher: { pollInterval: 20 },
    observers: [topology.telemetry.otel.observer],
    tracer: topology.telemetry.otel.tracer,
    logger: silent,
  });
}

describe('OpenTelemetry across two services over RabbitMQ', () => {
  it('records one trace from publish in the orders service to process in the billing service', async () => {
    const exchange = unique('events');
    const ordersTelemetry = createTelemetry();
    const billingTelemetry = createTelemetry();
    const billing = createService('billing', {
      exchange,
      telemetry: billingTelemetry,
      consumerGroup: unique('billing'),
    });
    const orders = createService('orders', { exchange, telemetry: ordersTelemetry });
    let handled: () => void;
    const done = new Promise<void>((resolve) => {
      handled = resolve;
    });
    billing.subscribe(OrderPlaced, async () => handled(), { id: 'billing.on-order-placed' });
    try {
      await billing.start();
      await orders.start();
      const request = ordersTelemetry.tracerProvider.getTracer('test').startSpan('POST /orders');
      await context.with(trace.setSpan(context.active(), request), () =>
        orders.publish(new OrderPlaced({ orderId: 'o-1' }), { key: 'o-1' }),
      );
      request.end();
      await done;
    } finally {
      await orders.stop();
      await billing.stop();
    }

    const sent = ordersTelemetry.spans();
    const processed = billingTelemetry.spans();
    const publish = sent.find((span) => span.name === 'publish order.placed.v1')!;
    const send = sent.find((span) => span.name === 'send orders.placed')!;
    const process = processed.find((span) => span.name === 'process order.placed.v1')!;
    const root = rootSpan(sent);
    const traceId = root.spanContext().traceId;
    expect([publish, send, process].map((span) => span.spanContext().traceId)).toEqual([
      traceId,
      traceId,
      traceId,
    ]);
    expect(publish.parentSpanContext?.spanId).toBe(root.spanContext().spanId);
    expect(send.parentSpanContext?.spanId).toBe(publish.spanContext().spanId);
    expect(process.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
    expect(process.parentSpanContext?.isRemote).toBe(true);
    expect(send.attributes).toMatchObject({
      'messaging.system': 'rabbitmq',
      'messaging.destination.name': 'orders.placed',
    });

    const consumed = (await billingTelemetry.collect()).get('messaging.client.consumed.messages');
    expect(consumed!.points).toEqual([
      {
        attributes: {
          'messaging.system': 'rabbitmq',
          'messaging.destination.name': 'orders.placed',
          'messaging.operation.name': 'receive',
          'messaging.operation.type': 'receive',
          'relaybus.receive.outcome': 'accepted',
        },
        value: 1,
      },
    ]);
    await ordersTelemetry.shutdown();
    await billingTelemetry.shutdown();
  });
});

function rootSpan(spans: ReturnType<Telemetry['spans']>): ReturnType<Telemetry['spans']>[number] {
  return spans.find((span) => span.name === 'POST /orders')!;
}
