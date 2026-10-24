import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { InMemoryEventStore, RelayBus, defineEvent } from '@relaybus/core';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import {
  eventually,
  remoteEnvelope,
  startBroker,
  unique,
  type TestBroker,
} from './support/broker.js';
import { TcpProxy } from './support/tcp-proxy.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: TestBroker;
let proxy: TcpProxy;
const buses: RelayBus[] = [];

beforeAll(async () => {
  broker = await startBroker();
  proxy = new TcpProxy(() => broker.getMappedPort(5672));
  await proxy.listen();
});

afterAll(async () => {
  await proxy.close();
  await broker.stop();
});

afterEach(async () => {
  proxy.refuse(false);
  await Promise.all(buses.splice(0).map((bus) => bus.stop()));
});

interface Service {
  readonly bus: RelayBus;
  readonly transport: RabbitMqTransport;
  readonly onReceived: ReturnType<typeof vi.fn>;
}

function createService(
  source: string,
  url: string,
  topology: { exchange: string; consumerGroup?: string; routes?: boolean },
): Service {
  const transport = new RabbitMqTransport({
    name: 'rabbit',
    url,
    exchange: topology.exchange,
    ...(topology.consumerGroup === undefined ? {} : { consumerGroup: topology.consumerGroup }),
    reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
    logger: silent,
  });
  const onReceived = vi.fn();
  const bus = new RelayBus({
    store: new InMemoryEventStore(),
    source,
    transactionMode: 'optional',
    transports: [transport],
    routes: topology.routes
      ? [
          {
            event: OrderPlaced,
            transport: 'rabbit',
            destination: 'orders.placed',
            retry: { initialDelay: '50ms', maxDelay: '100ms', jitter: 'none' },
          },
        ]
      : [],
    subscriptions:
      topology.consumerGroup === undefined
        ? []
        : [{ transport: 'rabbit', destinations: ['orders.*'] }],
    dispatcher: { pollInterval: 20 },
    observers: [{ onReceived }],
    logger: silent,
  });
  buses.push(bus);
  return { bus, transport, onReceived };
}

describe('RelayBus over RabbitMQ', () => {
  it('carries an event from the outbox of one service to the handler of another', async () => {
    const exchange = unique('events');
    const consumer = createService('billing', broker.getAmqpUrl(), {
      exchange,
      consumerGroup: unique('billing'),
    });
    const handled: unknown[] = [];
    consumer.bus.subscribe(
      OrderPlaced,
      async (event) => {
        handled.push(event.payload);
      },
      { id: 'billing.on-order-placed' },
    );
    await consumer.bus.start();

    const producer = createService('orders', broker.getAmqpUrl(), { exchange, routes: true });
    await producer.bus.start();
    await producer.bus.publish(new OrderPlaced({ orderId: 'o-1' }), { key: 'o-1' });

    await eventually(() => handled.length === 1);
    expect(handled).toEqual([{ orderId: 'o-1' }]);
    expect(consumer.onReceived).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'accepted',
        envelope: expect.objectContaining({ source: 'orders', key: 'o-1' }),
      }),
    );
  });

  it('stores a redelivered event once and runs its handler once', async () => {
    const exchange = unique('events');
    const consumer = createService('billing', broker.getAmqpUrl(), {
      exchange,
      consumerGroup: unique('billing'),
    });
    const handler = vi.fn(async () => undefined);
    consumer.bus.subscribe(OrderPlaced, handler, { id: 'billing.on-order-placed' });
    await consumer.bus.start();

    const producer = new RabbitMqTransport({ url: broker.getAmqpUrl(), exchange, logger: silent });
    await producer.connect();
    const envelope = remoteEnvelope({ payload: { orderId: 'dup' } });
    await producer.publish(envelope, 'orders.placed');
    await producer.publish(envelope, 'orders.placed');
    await producer.publish(envelope, 'orders.placed');
    await producer.close();

    await eventually(() => consumer.onReceived.mock.calls.length === 3);
    await eventually(() => handler.mock.calls.length === 1);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(handler).toHaveBeenCalledTimes(1);
    const statuses = consumer.onReceived.mock.calls.map(
      ([event]) => (event as { status: string }).status,
    );
    expect(statuses.sort()).toEqual(['accepted', 'duplicate', 'duplicate']);
  });

  it('delays events through a broker outage and loses none of them', async () => {
    const exchange = unique('events');
    const consumer = createService('billing', broker.getAmqpUrl(), {
      exchange,
      consumerGroup: unique('billing'),
    });
    const handled: string[] = [];
    consumer.bus.subscribe(
      OrderPlaced,
      async (event) => {
        handled.push(event.payload.orderId);
      },
      { id: 'billing.on-order-placed' },
    );
    await consumer.bus.start();

    const producerUrl = `amqp://guest:guest@127.0.0.1:${proxy.port}?heartbeat=5`;
    const producer = createService('orders', producerUrl, { exchange, routes: true });
    await producer.bus.start();
    await producer.bus.publish(new OrderPlaced({ orderId: 'before' }));
    await eventually(() => handled.includes('before'));

    proxy.refuse(true);
    proxy.sever();
    await eventually(() => !producer.transport.isConnected(), 30_000, 'the outage to be noticed');
    for (const orderId of ['during-1', 'during-2', 'during-3']) {
      await producer.bus.publish(new OrderPlaced({ orderId }));
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(handled).toEqual(['before']);

    proxy.refuse(false);
    await eventually(() => handled.length === 4, 60_000, 'the outbox to catch up');
    expect(handled.sort()).toEqual(['before', 'during-1', 'during-2', 'during-3']);
  });
});
