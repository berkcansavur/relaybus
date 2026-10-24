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
  KafkaTransport,
  cloudEventsBinary,
  cloudEventsStructured,
  jsonEnvelope,
  type KafkaCodec,
  type KafkaEncodedRecord,
} from '../src/index.js';
import { FakeKafka, createEnvelope, createLogger } from './fakes.js';

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

async function startService(codec: KafkaCodec) {
  const kafka = new FakeKafka();
  const handled: EventEnvelope[] = [];
  const dead: ErrorInfo[] = [];
  const bus = new RelayBus<InMemoryTransaction>({
    store: new InMemoryEventStore(),
    source: 'billing',
    transactionMode: 'optional',
    transports: [new KafkaTransport({ kafka, groupId: 'billing', codec, logger: createLogger() })],
    routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders.out' }],
    subscriptions: [{ transport: 'kafka', destinations: ['orders'] }],
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
  return { bus, handled, dead, consumer: kafka.consumer_, producer: kafka.producer_ };
}

/** A produced record as the consumer of `orders` receives it. */
function toIncoming(record: KafkaEncodedRecord, offset: string) {
  return {
    key: record.key === null ? null : Buffer.from(record.key),
    value: Buffer.from(record.value as string),
    headers: Object.fromEntries(
      Object.entries(record.headers).map(([name, value]) => [name, Buffer.from(value as string)]),
    ),
    timestamp: record.timestamp ?? '1767225600000',
    offset,
  };
}

async function waitFor(condition: () => boolean): Promise<void> {
  await vi.waitFor(() => {
    expect(condition()).toBe(true);
  });
}

describe.each<[string, KafkaCodec]>([
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
      id: { header: 'message-id' },
      payload: 'whole-body',
    }),
  ],
])('schema validation of payloads decoded by %s', (_label, codec) => {
  it('dead-letters an invalid payload with the validation error and keeps consuming', async () => {
    const service = await startService(codec);
    const invalid = createEnvelope({ id: 'invalid-1', payload: { orderId: 42 } });
    const valid = createEnvelope({ id: 'valid-1', payload: { orderId: '7' } });
    await service.consumer.deliver(toIncoming(codec.encode(invalid), '7'));
    await service.consumer.deliver(toIncoming(codec.encode(valid), '8'));

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
    expect(service.producer.sent).toEqual([]);
  });
});
