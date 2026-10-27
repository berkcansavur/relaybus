import amqplib from 'amqplib';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  catalogWireTypes,
  defineCatalog,
  defineEvent,
  produce,
  route,
  type EventEnvelope,
} from '@relaybus/core';
import { RabbitMqTransport, jsonEnvelope, type RabbitMqJsonEnvelopeMapping } from '../src/index.js';
import { eventually, startBroker, unique, type TestBroker } from './support/broker.js';

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: TestBroker;
let amqpUrl: string;
const buses: RelayBus[] = [];

beforeAll(async () => {
  broker = await startBroker();
  amqpUrl = broker.getAmqpUrl();
});

afterAll(async () => {
  await broker.stop();
});

afterEach(async () => {
  await Promise.all(buses.splice(0).map((bus) => bus.stop()));
});

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderCancelled extends defineEvent<{ orderId: string }>('order.cancelled.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

/** The contract of an in-house format: its type names are not relaybus event types. */
const OrderEvents = defineCatalog(
  { OrderPlaced, OrderCancelled, OrderShipped },
  {
    name: 'OrderEvents',
    wireTypes: { OrderPlaced: 'Order Placed', OrderCancelled: 'Order Cancelled' },
  },
);

const MAPPING: RabbitMqJsonEnvelopeMapping = {
  type: { body: 'eventName' },
  id: 'message-id-property',
  payload: { body: 'data' },
  key: { body: 'key' },
};

const SHIPPED_WIRE_NAME = 'Order Shipped';

/** A service on `exchange`: it produces the catalog by a route, or consumes it and has no route. */
async function startService(
  exchange: string,
  { name, consumes }: { readonly name: string; readonly consumes: boolean },
): Promise<{ readonly bus: RelayBus; readonly handled: EventEnvelope[] }> {
  const handled: EventEnvelope[] = [];
  const codec = jsonEnvelope(
    consumes
      ? {
          ...MAPPING,
          wireTypes: { ...catalogWireTypes(OrderEvents), 'order.shipped.v1': SHIPPED_WIRE_NAME },
        }
      : MAPPING,
  );
  const bus = new RelayBus({
    store: new InMemoryEventStore(),
    source: name,
    transactionMode: 'optional',
    transports: [
      new RabbitMqTransport({
        name: 'rabbit',
        url: amqpUrl,
        exchange,
        consumerGroup: name,
        codec,
        reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
        logger: silent,
      }),
    ],
    routes: consumes
      ? []
      : [
          route(produce(OrderEvents), {
            transport: 'rabbit',
            destination: 'orders.events',
            wireTypes: { OrderShipped: SHIPPED_WIRE_NAME },
          }),
        ],
    subscriptions: consumes ? [{ transport: 'rabbit', destinations: ['orders.*'] }] : [],
    dispatcher: { pollInterval: 20 },
    logger: silent,
  });
  if (consumes) {
    const record = (event: EventEnvelope): void => void handled.push(event);
    bus.subscribeCatalog(
      OrderEvents,
      { onOrderPlaced: record, onOrderCancelled: record, onOrderShipped: record },
      { idPrefix: name },
    );
  }
  buses.push(bus);
  await bus.start();
  return { bus, handled };
}

describe('catalog routes on a real RabbitMQ broker', () => {
  it('sends several catalog events to one routing key under their wire names', async () => {
    const exchange = unique('events');
    const connection = await amqplib.connect(amqpUrl);
    const channel = await connection.createChannel();
    const queue = unique('raw');
    await channel.assertExchange(exchange, 'topic', { durable: true });
    await channel.assertQueue(queue, { exclusive: true });
    await channel.bindQueue(queue, exchange, 'orders.#');
    const { bus } = await startService(exchange, { name: unique('orders'), consumes: false });

    await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' });
    await bus.publish(new OrderShipped({ orderId: '1' }), { key: 'o-1' });
    await bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' });

    const bodies: { eventName: string; key: string }[] = [];
    const routingKeys: string[] = [];
    await eventually(async () => {
      const message = await channel.get(queue, { noAck: true });
      if (message !== false) {
        bodies.push(JSON.parse(message.content.toString('utf8')) as (typeof bodies)[number]);
        routingKeys.push(message.fields.routingKey);
      }
      return bodies.length === 3;
    });
    await connection.close();

    expect(bodies.map(({ eventName }) => eventName)).toEqual([
      'Order Placed',
      'Order Shipped',
      'Order Cancelled',
    ]);
    expect(new Set(routingKeys)).toEqual(new Set(['orders.events']));
    expect(new Set(bus.handlers().map(({ id }) => id))).toEqual(
      new Set(['relaybus.transport:rabbit:orders.events']),
    );
  });

  it('reaches another relaybus service, which reads the wire names back as event types', async () => {
    const exchange = unique('events');
    const consumer = await startService(exchange, { name: unique('billing'), consumes: true });
    const producer = await startService(exchange, { name: unique('orders'), consumes: false });

    const published = [
      await producer.bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' }),
      await producer.bus.publish(new OrderShipped({ orderId: '1' }), { key: 'o-1' }),
      await producer.bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' }),
    ];

    await eventually(() => consumer.handled.length === 3);
    // One handler per event type: they run independently, so only the set of events is compared.
    const summary = (events: readonly EventEnvelope[]) =>
      events
        .map(({ type, id, key }) => ({ type, id, key }))
        .sort((a, b) => a.id.localeCompare(b.id));
    expect(summary(consumer.handled)).toEqual(summary(published));
  });
});
