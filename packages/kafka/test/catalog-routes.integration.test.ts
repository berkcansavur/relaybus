import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  catalogWireTypes,
  defineCatalog,
  defineEvent,
  produce,
  route,
  type EventEnvelope,
  type InMemoryTransaction,
} from '@relaybus/core';
import { KafkaTransport, jsonEnvelope, type KafkaJsonEnvelopeMapping } from '../src/index.js';
import { createLogger } from './fakes.js';
import { KAFKA_CLIENTS } from './support/kafka-clients.js';
import {
  brokerAddresses,
  createTopics,
  readRaw,
  startKafkaBroker,
  stopKafkaBroker,
  uniqueName,
  waitFor,
} from './support/kafka-broker.js';

beforeAll(async () => {
  await startKafkaBroker();
});

afterAll(async () => {
  await stopKafkaBroker();
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

const MAPPING: KafkaJsonEnvelopeMapping = {
  type: { body: 'eventName' },
  id: { body: 'messageId' },
  payload: { body: 'data' },
};

const SHIPPED_WIRE_NAME = 'Order Shipped';

describe.each(KAFKA_CLIENTS)('catalog routes on $name', (client) => {
  function createBus(options: {
    readonly topic: string;
    readonly name: string;
    readonly consumes: boolean;
  }) {
    const handled: EventEnvelope[] = [];
    const transport = new KafkaTransport({
      kafka: client.createClient(brokerAddresses()),
      groupId: uniqueName('group'),
      logger: createLogger(),
      ...client.transportOptions,
      processing: { maxAttempts: 3, initialBackoff: 50, maxBackoff: 200 },
      // The producing side declares its wire names by route; the consuming side has no route and
      // reads them from the catalog.
      codec: jsonEnvelope(
        options.consumes
          ? {
              ...MAPPING,
              wireTypes: {
                ...catalogWireTypes(OrderEvents),
                'order.shipped.v1': SHIPPED_WIRE_NAME,
              },
            }
          : MAPPING,
      ),
    });
    const bus = new RelayBus<InMemoryTransaction>({
      store: new InMemoryEventStore(),
      source: options.name,
      transactionMode: 'optional',
      transports: [transport],
      routes: options.consumes
        ? []
        : [
            route(produce(OrderEvents), {
              transport: 'kafka',
              destination: options.topic,
              wireTypes: { OrderShipped: SHIPPED_WIRE_NAME },
            }),
          ],
      subscriptions: options.consumes
        ? [{ transport: 'kafka', destinations: [options.topic] }]
        : [],
      dispatcher: { pollInterval: 25 },
      logger: createLogger(),
    });
    if (options.consumes) {
      bus.subscribeCatalog(
        OrderEvents,
        {
          onOrderPlaced: (event: EventEnvelope) => void handled.push(event),
          onOrderCancelled: (event: EventEnvelope) => void handled.push(event),
          onOrderShipped: (event: EventEnvelope) => void handled.push(event),
        },
        { idPrefix: options.name },
      );
    }
    return { bus, handled };
  }

  it('sends several catalog events to one topic under their wire names, in order per key', async () => {
    const topic = uniqueName('catalog-route');
    await createTopics(topic);
    const { bus } = createBus({ topic, name: 'orders', consumes: false });
    await bus.start();

    for (const [event, key] of [
      [new OrderPlaced({ orderId: '1' }), 'o-1'],
      [new OrderPlaced({ orderId: '2' }), 'o-2'],
      [new OrderShipped({ orderId: '1' }), 'o-1'],
      [new OrderCancelled({ orderId: '2' }), 'o-2'],
    ] as const) {
      await bus.publish(event, { key });
    }

    const records = await readRaw(topic, 4);
    await bus.stop();
    const sent = records.map((record) => {
      const body = JSON.parse(record.value!) as { eventName: string; data: { orderId: string } };
      return `${record.key} ${body.eventName} ${body.data.orderId}`;
    });
    expect(sent.filter((line) => line.startsWith('o-1'))).toEqual([
      'o-1 Order Placed 1',
      'o-1 Order Shipped 1',
    ]);
    expect(sent.filter((line) => line.startsWith('o-2'))).toEqual([
      'o-2 Order Placed 2',
      'o-2 Order Cancelled 2',
    ]);
    expect(bus.handlers().map(({ id }) => id)).toEqual(
      Array(3).fill(`relaybus.transport:kafka:${topic}`),
    );
  });

  it('reaches another relaybus service, which reads the wire names back as event types', async () => {
    const topic = uniqueName('catalog-route-roundtrip');
    await createTopics(topic, `${topic}.dead`);
    const consumer = createBus({ topic, name: 'billing', consumes: true });
    const producer = createBus({ topic, name: 'orders', consumes: false });
    await consumer.bus.start();
    await producer.bus.start();

    const published = [
      await producer.bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' }),
      await producer.bus.publish(new OrderShipped({ orderId: '1' }), { key: 'o-1' }),
      await producer.bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' }),
    ];

    await waitFor(() => consumer.handled.length === 3);
    // One handler per event type: they run independently, so only the set of events is compared.
    const summary = (events: readonly EventEnvelope[]) =>
      events
        .map(({ type, id, key }) => ({ type, id, key }))
        .sort((a, b) => a.id.localeCompare(b.id));
    expect(summary(consumer.handled)).toEqual(summary(published));
    await producer.bus.stop();
    await consumer.bus.stop();
  });
});
