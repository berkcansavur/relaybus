import amqplib from 'amqplib';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { InMemoryEventStore, RelayBus, type EventEnvelope } from '@relaybus/core';
import {
  RabbitMqTransport,
  decodeCloudEvent,
  fromHeader,
  plainJsonDecoder,
  type RabbitMqTransportOptions,
} from '../src/index.js';
import {
  eventually,
  publishRaw,
  remoteEnvelope,
  startBroker,
  takeMessage,
  type TestBroker,
  unique,
} from './support/broker.js';

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

interface ForeignMessage {
  readonly routingKey: string;
  readonly body: string;
  readonly properties?: amqplib.Options.Publish;
}

/** Publishes with one plain amqplib confirm channel, the way a service without relaybus would. */
async function publishForeign(
  exchange: string,
  messages: readonly ForeignMessage[],
): Promise<void> {
  const connection = await amqplib.connect(amqpUrl);
  try {
    const channel = await connection.createConfirmChannel();
    await channel.assertExchange(exchange, 'topic', { durable: true });
    for (const message of messages) {
      channel.publish(exchange, message.routingKey, Buffer.from(message.body), {
        persistent: true,
        ...message.properties,
      });
    }
    await channel.waitForConfirms();
  } finally {
    await connection.close();
  }
}

interface ConsumingService {
  readonly bus: RelayBus;
  readonly store: InMemoryEventStore;
  readonly queue: string;
  readonly handled: EventEnvelope[];
  readonly statuses: string[];
}

/** A relaybus service consuming `orders.*` of `exchange`, with one handler for `order.placed.v1`. */
async function startConsumingService(
  exchange: string,
  options: Partial<RabbitMqTransportOptions>,
  prepare: (store: InMemoryEventStore) => void = () => undefined,
): Promise<ConsumingService> {
  const consumerGroup = unique('billing');
  const store = new InMemoryEventStore();
  const handled: EventEnvelope[] = [];
  const statuses: string[] = [];
  const bus = new RelayBus({
    store,
    source: 'billing',
    transactionMode: 'optional',
    transports: [
      new RabbitMqTransport({
        name: 'rabbit',
        url: amqpUrl,
        exchange,
        consumerGroup,
        reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
        logger: silent,
        ...options,
      }),
    ],
    subscriptions: [{ transport: 'rabbit', destinations: ['orders.*'] }],
    dispatcher: { pollInterval: 20 },
    observers: [{ onReceived: ({ status }) => statuses.push(status) }],
    logger: silent,
  });
  bus.subscribe(
    'order.placed.v1',
    async (event) => {
      handled.push(event);
    },
    { id: 'billing.on-order-placed' },
  );
  buses.push(bus);
  prepare(store);
  await bus.start();
  return { bus, store, queue: `relaybus.${consumerGroup}`, handled, statuses };
}

describe('relaybus producer to a consumer that does not use relaybus', () => {
  it('lets a plain amqplib consumer read the JSON payload, the type and the id', async () => {
    const exchange = unique('events');
    const rawQueue = unique('raw');
    const connection = await amqplib.connect(amqpUrl);
    const channel = await connection.createChannel();
    await channel.assertExchange(exchange, 'topic', { durable: true });
    await channel.assertQueue(rawQueue, { exclusive: true });
    await channel.bindQueue(rawQueue, exchange, 'orders.#');
    const producer = new RabbitMqTransport({ url: amqpUrl, exchange, logger: silent });
    await producer.connect();
    const envelope = remoteEnvelope({ payload: { orderId: '77', total: 12.5 } });
    await producer.publish(envelope, 'orders.placed');
    await producer.close();

    let message: amqplib.GetMessage | false = false;
    await eventually(async () => {
      message = await channel.get(rawQueue, { noAck: true });
      return message !== false;
    });
    const received = message as unknown as amqplib.GetMessage;
    expect(JSON.parse(received.content.toString('utf8'))).toEqual({ orderId: '77', total: 12.5 });
    expect(received.properties).toMatchObject({
      contentType: 'application/json',
      messageId: envelope.id,
      type: 'order.placed.v1',
      appId: 'orders-service',
      deliveryMode: 2,
    });
    expect(received.properties.headers).toMatchObject({
      'cloudEvents:specversion': '1.0',
      'cloudEvents:id': envelope.id,
      'cloudEvents:type': 'order.placed.v1',
      'cloudEvents:source': 'orders-service',
      'cloudEvents:time': '2026-01-01T10:20:30.456Z',
      'cloudEvents:subject': 'order-1',
      'relaybus:traceparent': '00-abc-def-01',
    });
    await connection.close();
  });
});

describe('producer that does not use relaybus to a relaybus consumer', () => {
  it('handles plain JSON messages once each, in order per key, when ordered', async () => {
    const exchange = unique('events');
    const service = await startConsumingService(exchange, {
      ordered: true,
      decode: plainJsonDecoder({
        type: (message) => message.properties.type as string,
        key: fromHeader('customer'),
      }),
    });
    const customers = ['customer-a', 'customer-b', 'customer-c'];
    const messages = Array.from({ length: 30 }, (_, index) => ({
      routingKey: 'orders.placed',
      body: JSON.stringify({ sequence: index }),
      properties: {
        messageId: `msg-${index}`,
        type: 'order.placed.v1',
        appId: 'legacy-java',
        headers: { customer: customers[index % customers.length]! },
      },
    }));
    await publishForeign(exchange, messages);

    await eventually(() => service.handled.length === messages.length);
    for (const customer of customers) {
      const sequences = service.handled
        .filter((event) => event.key === customer)
        .map((event) => (event.payload as { sequence: number }).sequence);
      expect(sequences).toEqual([...sequences].sort((left, right) => left - right));
      expect(sequences).toHaveLength(10);
    }
    expect(new Set(service.handled.map((event) => event.id)).size).toBe(messages.length);
    expect(service.handled.find((event) => event.id === 'msg-0')).toMatchObject({
      type: 'order.placed.v1',
      source: 'legacy-java',
      key: 'customer-a',
      headers: { customer: 'customer-a' },
    });
  });

  it('stores a message redelivered through the retry queue once, because its messageId is stable', async () => {
    const exchange = unique('events');
    let failedAfterCommit = false;
    const service = await startConsumingService(
      exchange,
      {
        decode: plainJsonDecoder({ type: 'order.placed.v1' }),
        handlerRetry: { attempts: 1 },
        redelivery: { initialDelay: '50ms', maxDelay: '100ms', jitter: 'none' },
      },
      (store) => {
        const transaction = store.transaction.bind(store);
        // The first receive commits and then fails, as if the process died before the ack.
        vi.spyOn(store, 'transaction').mockImplementation(async (work) => {
          const result = await transaction(work);
          if (!failedAfterCommit) {
            failedAfterCommit = true;
            throw new Error('connection lost after commit');
          }
          return result;
        });
      },
    );
    await publishRaw(
      amqpUrl,
      exchange,
      'orders.placed',
      Buffer.from('{"orderId":"1"}'),
      {},
      {
        messageId: 'legacy-1',
      },
    );

    // The failed first receive reports nothing; the redelivery finds the stored event.
    await eventually(() => service.statuses.length >= 1 && service.handled.length >= 1);
    await service.bus.drain();
    expect(failedAfterCommit).toBe(true);
    expect(service.statuses).toEqual(['duplicate']);
    expect(service.handled.map((event) => event.id)).toEqual(['legacy-1']);
  });

  it('gives the decoder the original routing key on a redelivery through the retry queue', async () => {
    const exchange = unique('events');
    let failed = false;
    const service = await startConsumingService(
      exchange,
      {
        decode: plainJsonDecoder({
          type: (message) =>
            message.routingKey === 'orders.placed' ? 'order.placed.v1' : undefined,
        }),
        handlerRetry: { attempts: 1 },
        redelivery: { initialDelay: '50ms', maxDelay: '100ms', jitter: 'none' },
      },
      (store) => {
        const transaction = store.transaction.bind(store);
        // The first receive fails before anything is stored, so the retry copy is what gets stored.
        vi.spyOn(store, 'transaction').mockImplementation(async (work) => {
          if (!failed) {
            failed = true;
            throw new Error('database unavailable');
          }
          return transaction(work);
        });
      },
    );
    await publishForeign(exchange, [
      { routingKey: 'orders.placed', body: '{"orderId":"1"}', properties: { messageId: 'rk-1' } },
    ]);

    await eventually(() => service.handled.length === 1);
    expect(failed).toBe(true);
    expect(service.handled[0]).toMatchObject({
      id: 'rk-1',
      type: 'order.placed.v1',
      source: 'orders.placed',
    });
    expect(service.handled[0]!.headers).toEqual({});
  });

  it('dedupes two sends of one event by their messageId', async () => {
    const exchange = unique('events');
    const service = await startConsumingService(exchange, {
      ordered: true,
      decode: plainJsonDecoder({ type: 'order.placed.v1' }),
    });
    const send = { routingKey: 'orders.placed', body: '{"orderId":"1"}' };
    await publishForeign(exchange, [
      { ...send, properties: { messageId: 'm-1' } },
      { ...send, properties: { messageId: 'm-1' } },
      { ...send, properties: { messageId: 'm-2' } },
    ]);

    await eventually(() => service.statuses.length === 3);
    await service.bus.drain();
    expect(service.statuses).toEqual(['accepted', 'duplicate', 'accepted']);
    expect(service.handled.map((event) => event.id).sort()).toEqual(['m-1', 'm-2']);
  });

  it('handles a structured-mode CloudEvent from a CloudEvents SDK producer', async () => {
    const exchange = unique('events');
    const service = await startConsumingService(exchange, { decode: decodeCloudEvent });
    await publishForeign(exchange, [
      {
        routingKey: 'orders.placed',
        body: JSON.stringify({
          specversion: '1.0',
          id: 'ce-5',
          type: 'order.placed.v1',
          source: '//python/orders',
          time: '2026-05-01T10:00:00Z',
          subject: 'order-5',
          data: { orderId: '5' },
          traceparent: '00-abc-def-01',
        }),
        properties: { contentType: 'application/cloudevents+json; charset=utf-8' },
      },
    ]);

    await eventually(() => service.handled.length === 1);
    expect(service.handled[0]).toMatchObject({
      id: 'ce-5',
      type: 'order.placed.v1',
      source: '//python/orders',
      key: 'order-5',
      payload: { orderId: '5' },
      headers: { traceparent: '00-abc-def-01' },
      occurredAt: new Date('2026-05-01T10:00:00Z'),
    });
  });

  it('dead-letters garbage and keeps consuming the messages after it', async () => {
    const exchange = unique('events');
    const service = await startConsumingService(exchange, {
      ordered: true,
      decode: plainJsonDecoder({ type: fromHeader('event-type') }),
    });
    const typed = { 'event-type': 'order.placed.v1' };
    await publishForeign(exchange, [
      {
        routingKey: 'orders.placed',
        body: 'not json',
        properties: { messageId: 'a', headers: typed },
      },
      { routingKey: 'orders.placed', body: '{"orderId":"b"}', properties: { headers: typed } },
      { routingKey: 'orders.placed', body: '{"orderId":"c"}', properties: { messageId: 'c' } },
      {
        routingKey: 'orders.placed',
        body: '{"orderId":"d"}',
        properties: { messageId: 'd', headers: typed },
      },
    ]);

    await eventually(() => service.handled.length === 1);
    expect(service.handled[0]!.payload).toEqual({ orderId: 'd' });
    const deadLetters: string[] = [];
    await eventually(async () => {
      const message = await takeMessage(amqpUrl, `${service.queue}.dead`);
      if (message !== undefined) {
        expect(message.headers['x-relaybus-dead-letter-reason']).toBe('malformed message');
        deadLetters.push(message.body);
      }
      return deadLetters.length === 3;
    });
    expect(deadLetters).toEqual(['not json', '{"orderId":"b"}', '{"orderId":"c"}']);
  });
});
