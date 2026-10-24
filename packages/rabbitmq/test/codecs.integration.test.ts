import amqplib from 'amqplib';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { InMemoryEventStore, RelayBus, defineEvent, type EventEnvelope } from '@relaybus/core';
import {
  RabbitMqTransport,
  cloudEventsBinary,
  cloudEventsStructured,
  jsonEnvelope,
  type RabbitMqCodec,
  type RabbitMqJsonEnvelopeMapping,
} from '../src/index.js';
import {
  eventually,
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

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const BODY_ENVELOPE: RabbitMqJsonEnvelopeMapping = {
  type: { body: 'eventName' },
  id: { body: 'messageId' },
  payload: { body: 'data' },
  key: { body: 'meta.key' },
  source: { body: 'meta.source' },
  occurredAt: { body: 'meta.occurredAt' },
  correlationId: { body: 'meta.correlationId' },
  causationId: { body: 'meta.causationId' },
  headers: { body: 'meta.headers' },
};

const MASS_TRANSIT: RabbitMqJsonEnvelopeMapping = {
  type: { body: 'messageType.0' },
  id: { body: 'messageId' },
  payload: { body: 'message' },
  correlationId: { body: 'correlationId' },
  occurredAt: { body: 'sentTime' },
  headers: { body: 'headers' },
  contentType: 'application/vnd.masstransit+json',
};

const PLAIN_JSON: RabbitMqJsonEnvelopeMapping = {
  type: { header: 'event-type' },
  id: 'message-id-property',
  payload: 'whole-body',
};

/** Reads one message from a queue bound to `orders.#` with plain amqplib. */
async function readRawMessage(
  exchange: string,
  publish: () => Promise<void>,
): Promise<amqplib.GetMessage> {
  const connection = await amqplib.connect(amqpUrl);
  try {
    const channel = await connection.createChannel();
    const queue = unique('raw');
    await channel.assertExchange(exchange, 'topic', { durable: true });
    await channel.assertQueue(queue, { exclusive: true });
    await channel.bindQueue(queue, exchange, 'orders.#');
    await publish();
    let message: amqplib.GetMessage | false = false;
    await eventually(async () => {
      message = await channel.get(queue, { noAck: true });
      return message !== false;
    });
    return message as unknown as amqplib.GetMessage;
  } finally {
    await connection.close();
  }
}

/** Publishes the way a service without relaybus would. */
async function publishForeign(
  exchange: string,
  message: { readonly body: Buffer; readonly properties: amqplib.Options.Publish },
): Promise<void> {
  const connection = await amqplib.connect(amqpUrl);
  try {
    const channel = await connection.createConfirmChannel();
    await channel.assertExchange(exchange, 'topic', { durable: true });
    channel.publish(exchange, 'orders.placed', message.body, message.properties);
    await channel.waitForConfirms();
  } finally {
    await connection.close();
  }
}

/** A relaybus service consuming `orders.*` of `exchange` and publishing OrderPlaced there. */
async function startService(
  exchange: string,
  { codec, consumerGroup }: { readonly codec: RabbitMqCodec; readonly consumerGroup: string },
): Promise<{ readonly bus: RelayBus; readonly handled: EventEnvelope[]; readonly queue: string }> {
  const handled: EventEnvelope[] = [];
  const bus = new RelayBus({
    store: new InMemoryEventStore(),
    source: consumerGroup,
    transactionMode: 'optional',
    transports: [
      new RabbitMqTransport({
        name: 'rabbit',
        url: amqpUrl,
        exchange,
        consumerGroup,
        codec,
        reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
        logger: silent,
      }),
    ],
    routes: [{ event: OrderPlaced, transport: 'rabbit', destination: 'orders.placed' }],
    subscriptions: [{ transport: 'rabbit', destinations: ['orders.*'] }],
    dispatcher: { pollInterval: 20 },
    logger: silent,
  });
  bus.subscribe(
    OrderPlaced,
    async (event) => {
      handled.push(event);
    },
    { id: `${consumerGroup}.on-order-placed` },
  );
  buses.push(bus);
  await bus.start();
  return { bus, handled, queue: `relaybus.${consumerGroup}` };
}

describe.each<[string, RabbitMqCodec]>([
  ['cloudEventsBinary', cloudEventsBinary],
  ['cloudEventsStructured', cloudEventsStructured],
  ['jsonEnvelope with a body envelope', jsonEnvelope(BODY_ENVELOPE)],
  ['jsonEnvelope with MassTransit', jsonEnvelope(MASS_TRANSIT)],
  ['jsonEnvelope with the bare payload', jsonEnvelope(PLAIN_JSON)],
])('%s on a real broker', (_label, codec) => {
  it('lets a plain amqplib consumer read what relaybus wrote', async () => {
    const exchange = unique('events');
    const envelope = remoteEnvelope();
    const message = await readRawMessage(exchange, async () => {
      const producer = new RabbitMqTransport({ url: amqpUrl, exchange, codec, logger: silent });
      await producer.connect();
      await producer.publish(envelope, 'orders.placed');
      await producer.close();
    });
    const expected = codec.encode(envelope);
    expect(message.content.equals(expected.content)).toBe(true);
    expect(message.properties).toMatchObject({
      contentType: expected.options.contentType,
      messageId: envelope.id,
      headers: expected.options.headers,
    });
  });

  it('hands relaybus what a plain amqplib producer wrote in the format', async () => {
    const exchange = unique('events');
    const service = await startService(exchange, { codec, consumerGroup: unique('billing') });
    const envelope = remoteEnvelope();
    const { content, options } = codec.encode(envelope);
    await publishForeign(exchange, { body: content, properties: options });

    await eventually(() => service.handled.length === 1);
    expect(service.handled[0]).toMatchObject({
      id: envelope.id,
      type: 'order.placed.v1',
      payload: { orderId: '1' },
    });
  });
});

describe('relaybus to relaybus with a non-default codec', () => {
  it('delivers every field of the event through a body envelope, dead-lettering garbage', async () => {
    const exchange = unique('events');
    const codec = jsonEnvelope(BODY_ENVELOPE);
    const consumer = await startService(exchange, { codec, consumerGroup: unique('billing') });
    const producer = new RelayBus({
      store: new InMemoryEventStore(),
      source: 'orders',
      transactionMode: 'optional',
      transports: [
        new RabbitMqTransport({ name: 'rabbit', url: amqpUrl, exchange, codec, logger: silent }),
      ],
      routes: [{ event: OrderPlaced, transport: 'rabbit', destination: 'orders.placed' }],
      dispatcher: { pollInterval: 20 },
      logger: silent,
    });
    buses.push(producer);
    await producer.start();

    const published = await producer.publish(new OrderPlaced({ orderId: '42' }), {
      key: 'order-42',
      correlationId: 'flow-42',
      headers: { traceparent: '00-abc-def-01' },
    });
    await eventually(() => consumer.handled.length === 1);
    expect(consumer.handled[0]).toEqual(published);

    await publishForeign(exchange, {
      body: Buffer.from('{"eventName":"order.placed.v1"}'),
      properties: { messageId: 'garbage' },
    });
    let deadLetter: Awaited<ReturnType<typeof takeMessage>>;
    await eventually(async () => {
      deadLetter = await takeMessage(amqpUrl, `${consumer.queue}.dead`);
      return deadLetter !== undefined;
    });
    expect(deadLetter!.headers).toMatchObject({
      'x-relaybus-dead-letter-error-name': 'MalformedMessageError',
    });
  });
});
