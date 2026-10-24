import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  EventValidationError,
  InMemoryEventStore,
  RelayBus,
  defineEvent,
  type ErrorInfo,
  type EventEnvelope,
  type InMemoryTransaction,
  type StandardSchemaV1,
} from '@relaybus/core';
import {
  RabbitMqTransport,
  cloudEventsBinary,
  cloudEventsStructured,
  jsonEnvelope,
  type AmqpMessage,
  type EncodedMessage,
  type RabbitMqCodec,
} from '../src/index.js';
import { FakeConnection, createEnvelope, createLogger } from './fakes.js';

/** A Standard Schema object, as zod, valibot or arktype expose one: `{ orderId: string }`. */
const orderSchema: StandardSchemaV1<unknown, { orderId: string }> = {
  '~standard': {
    version: 1,
    vendor: 'test',
    validate: (value) =>
      typeof value === 'object' &&
      value !== null &&
      typeof (value as { orderId?: unknown }).orderId === 'string'
        ? { value: value as { orderId: string } }
        : { issues: [{ message: 'orderId must be a string', path: ['orderId'] }] },
  },
};

class OrderPlaced extends defineEvent('order.placed.v1', { schema: orderSchema }) {}

const buses: RelayBus<InMemoryTransaction>[] = [];

afterEach(async () => {
  await Promise.all(buses.splice(0).map((bus) => bus.stop()));
});

async function startService(codec: RabbitMqCodec) {
  const connections: FakeConnection[] = [];
  const handled: EventEnvelope[] = [];
  const dead: ErrorInfo[] = [];
  const bus = new RelayBus<InMemoryTransaction>({
    store: new InMemoryEventStore(),
    source: 'billing',
    transactionMode: 'optional',
    transports: [
      new RabbitMqTransport({
        name: 'rabbit',
        connect: async () => {
          const connection = new FakeConnection();
          connections.push(connection);
          return connection;
        },
        consumerGroup: 'billing',
        codec,
        logger: createLogger(),
      }),
    ],
    routes: [{ event: OrderPlaced, transport: 'rabbit', destination: 'orders.out' }],
    subscriptions: [{ transport: 'rabbit', destinations: ['orders.*'] }],
    dispatcher: { pollInterval: 5 },
    observers: [{ onDeliveryDead: ({ error }) => dead.push(error) }],
    logger: createLogger(),
  });
  bus.subscribe(
    OrderPlaced,
    async (event) => {
      handled.push(event);
    },
    { id: 'billing.on-order-placed' },
  );
  buses.push(bus);
  await bus.start();
  const connection = connections[0]!;
  return {
    bus,
    handled,
    dead,
    consumer: connection.consumerChannel,
    publisher: connection.publisher,
  };
}

/** A published message as the consumer receives it. */
function toDelivery({ content, options }: EncodedMessage): AmqpMessage {
  return { content, fields: { routingKey: 'orders.placed' }, properties: { ...options } };
}

async function waitFor(condition: () => boolean): Promise<void> {
  await vi.waitFor(() => {
    expect(condition()).toBe(true);
  });
}

describe.each<[string, RabbitMqCodec]>([
  ['cloudEventsBinary', cloudEventsBinary],
  ['cloudEventsStructured', cloudEventsStructured],
  [
    'jsonEnvelope with the payload under data',
    jsonEnvelope({
      type: { body: 'eventName' },
      id: { body: 'messageId' },
      payload: { body: 'data' },
    }),
  ],
  [
    'jsonEnvelope with the bare payload',
    jsonEnvelope({
      type: { header: 'event-type' },
      id: 'message-id-property',
      payload: 'whole-body',
    }),
  ],
])('schema validation of payloads decoded by %s', (_label, codec) => {
  it('dead-letters an invalid payload with the validation error and keeps consuming', async () => {
    const service = await startService(codec);
    const invalid = createEnvelope({ id: 'invalid-1', payload: { orderId: 42 } });
    const valid = createEnvelope({ id: 'valid-1', payload: { orderId: '7' } });
    service.consumer.deliver!(toDelivery(codec.encode(invalid)));
    service.consumer.deliver!(toDelivery(codec.encode(valid)));

    await waitFor(() => service.handled.length === 1 && service.dead.length === 1);
    expect(service.handled.map((event) => [event.id, event.payload])).toEqual([
      ['valid-1', { orderId: '7' }],
    ]);
    expect(service.dead[0]).toMatchObject({
      name: 'EventValidationError',
      code: 'RELAYBUS_EVENT_VALIDATION',
      message: expect.stringContaining('orderId must be a string'),
    });
  });
});

describe('schema validation on publish', () => {
  it('rejects an invalid payload before any codec sees it', async () => {
    const codec = jsonEnvelope({
      type: { body: 'eventName' },
      id: { body: 'messageId' },
      payload: { body: 'data' },
    });
    const encode = vi.spyOn(codec, 'encode');
    const service = await startService(codec);
    await expect(
      service.bus.publish(new OrderPlaced({ orderId: 42 } as unknown as { orderId: string })),
    ).rejects.toThrow(EventValidationError);
    await service.bus.drain();
    expect(encode).not.toHaveBeenCalled();
    expect(service.publisher.published).toEqual([]);
  });
});
