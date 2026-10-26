import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  defineEvent,
  type EventEnvelope,
  type InMemoryTransaction,
} from '@relaybus/core';
import {
  KafkaTransport,
  cloudEventsBinary,
  cloudEventsStructured,
  jsonEnvelope,
  type KafkaCodec,
  type KafkaJsonEnvelopeMapping,
  type KafkaTransportOptions,
} from '../src/index.js';
import { createEnvelope, createLogger } from './fakes.js';
import { KAFKA_CLIENTS } from './support/kafka-clients.js';
import {
  brokerAddresses,
  createTopics,
  produceRaw,
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

const BODY_ENVELOPE: KafkaJsonEnvelopeMapping = {
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

const PLAIN_JSON: KafkaJsonEnvelopeMapping = {
  type: { header: 'event-type' },
  id: { header: 'message-id' },
  payload: 'whole-body',
};

describe.each(KAFKA_CLIENTS)('relaybus on $name', (client) => {
  function createTransport(options: Partial<KafkaTransportOptions> = {}): KafkaTransport {
    return new KafkaTransport({
      kafka: client.createClient(brokerAddresses()),
      groupId: uniqueName('group'),
      logger: createLogger(),
      ...client.transportOptions,
      processing: { maxAttempts: 3, initialBackoff: 50, maxBackoff: 200 },
      ...options,
    });
  }

  /** A relaybus service on `topic`: it publishes OrderPlaced there and handles what arrives. */
  async function startService(
    topic: string,
    { codec, name }: { readonly codec: KafkaCodec; readonly name: string },
  ): Promise<{ readonly bus: RelayBus<InMemoryTransaction>; readonly handled: EventEnvelope[] }> {
    const handled: EventEnvelope[] = [];
    const bus = new RelayBus<InMemoryTransaction>({
      store: new InMemoryEventStore(),
      source: name,
      transactionMode: 'optional',
      transports: [createTransport({ name: 'kafka', codec })],
      routes: [{ event: OrderPlaced, transport: 'kafka', destination: topic }],
      subscriptions: [{ transport: 'kafka', destinations: [topic] }],
      dispatcher: { pollInterval: 25 },
      logger: createLogger(),
    });
    bus.subscribe(
      OrderPlaced,
      async (event) => {
        handled.push(event);
      },
      { id: `${name}.on-order-placed` },
    );
    await bus.start();
    return { bus, handled };
  }

  describe.each<[string, KafkaCodec, Record<string, string>]>([
    [
      'cloudEventsStructured',
      cloudEventsStructured,
      { 'content-type': 'application/cloudevents+json', traceparent: '00-abc-def-01' },
    ],
    [
      'jsonEnvelope with a body envelope',
      jsonEnvelope(BODY_ENVELOPE),
      { 'content-type': 'application/json' },
    ],
    [
      'jsonEnvelope with the bare payload',
      jsonEnvelope(PLAIN_JSON),
      {
        'content-type': 'application/json',
        'event-type': 'order.placed.v1',
        'message-id': '0190a000-0000-7000-8000-0000000000aa',
        traceparent: '00-abc-def-01',
      },
    ],
  ])('%s on a real broker', (_label, codec, expectedHeaders) => {
    it('lets a plain kafkajs consumer read what relaybus wrote', async () => {
      const topic = uniqueName('codec-out');
      await createTopics(topic);
      const producer = createTransport({ codec });
      await producer.publish(createEnvelope(), topic);
      await producer.close();

      const [record] = await readRaw(topic, 1);
      const expected = codec.encode(createEnvelope());
      expect(record!.key).toBe('order-1');
      expect(JSON.parse(record!.value!)).toEqual(JSON.parse(expected.value as string));
      expect(record!.headers).toEqual(expectedHeaders);
    });

    it('hands relaybus what a plain kafkajs producer wrote in the format', async () => {
      const topic = uniqueName('codec-in');
      await createTopics(topic);
      const service = await startService(topic, { codec, name: 'billing' });
      const encoded = codec.encode(createEnvelope());
      await produceRaw(topic, [
        {
          key: 'order-1',
          value: encoded.value,
          headers: encoded.headers as Record<string, string>,
        },
      ]);

      await waitFor(() => service.handled.length === 1);
      expect(service.handled[0]).toMatchObject({
        id: '0190a000-0000-7000-8000-0000000000aa',
        type: 'order.placed.v1',
        key: 'order-1',
        payload: { orderId: '1' },
      });
      await service.bus.stop();
    });
  });

  describe('cloudEventsBinary on a real broker', () => {
    it('is what a plain kafkajs producer and consumer exchange with relaybus by default', async () => {
      const topic = uniqueName('codec-binary');
      await createTopics(topic);
      const service = await startService(topic, { codec: cloudEventsBinary, name: 'billing' });
      await produceRaw(topic, [
        {
          key: 'order-1',
          value: '{"orderId":"1"}',
          headers: {
            ce_specversion: '1.0',
            ce_id: 'raw-1',
            ce_type: 'order.placed.v1',
            ce_source: 'go-orders',
            ce_time: '2026-01-01T00:00:00Z',
            'content-type': 'application/json',
          },
        },
      ]);
      await waitFor(() => service.handled.length === 1);
      expect(service.handled[0]).toMatchObject({
        id: 'raw-1',
        source: 'go-orders',
        key: 'order-1',
      });

      await service.bus.publish(new OrderPlaced({ orderId: '2' }), { key: 'order-2' });
      const records = await readRaw(topic, 2);
      expect(records[1]!.headers).toMatchObject({
        ce_specversion: '1.0',
        ce_type: 'order.placed.v1',
      });
      expect(JSON.parse(records[1]!.value!)).toEqual({ orderId: '2' });
      await service.bus.stop();
    });
  });

  describe('relaybus to relaybus with a non-default codec', () => {
    it('delivers every field of the event through a body envelope', async () => {
      const topic = uniqueName('codec-relaybus');
      await createTopics(topic, `${topic}.dead`);
      const codec = jsonEnvelope(BODY_ENVELOPE);
      const consumer = await startService(topic, { codec, name: 'billing' });
      const producerToTopic = new RelayBus<InMemoryTransaction>({
        store: new InMemoryEventStore(),
        source: 'orders',
        transactionMode: 'optional',
        transports: [createTransport({ name: 'kafka', codec })],
        routes: [{ event: OrderPlaced, transport: 'kafka', destination: topic }],
        dispatcher: { pollInterval: 25 },
        logger: createLogger(),
      });
      await producerToTopic.start();

      const published = await producerToTopic.publish(new OrderPlaced({ orderId: '42' }), {
        key: 'order-42',
        correlationId: 'flow-42',
        headers: { traceparent: '00-abc-def-01' },
      });
      await waitFor(() => consumer.handled.length === 1);
      expect(consumer.handled[0]).toEqual(published);

      await produceRaw(topic, [{ key: 'garbage', value: '{"eventName":"order.placed.v1"}' }]);
      const [deadLetter] = await readRaw(`${topic}.dead`, 1);
      expect(deadLetter!.headers['relaybus-invalid-reason']).toBe(
        "The message has no id at 'messageId'",
      );
      await producerToTopic.stop();
      await consumer.bus.stop();
    });
  });

  describe('a legacy flat JSON envelope without record keys or headers', () => {
    const legacyFlat = jsonEnvelope({
      type: { body: 'type' },
      id: { body: 'id', whenMissing: 'record-position' },
      payload: { body: 'data' },
      source: { body: 'provider' },
      occurredAt: { body: 'timestamp', format: 'epoch-millis' },
      correlationId: { body: 'requestId' },
      headers: 'top-level-fields',
      extra: { body: { createdDate: (event) => event.occurredAt.toISOString() } },
      bodyOrder: ['id', 'requestId', 'provider', 'timestamp', 'createdDate', 'type', 'data'],
      recordKey: 'none',
      contentTypeHeader: false,
    });

    it('writes keyless, headerless flat records and reads them and foreign ones back', async () => {
      const topic = uniqueName('legacy-flat');
      await createTopics(topic);
      const service = await startService(topic, { codec: legacyFlat, name: 'billing-api' });
      const published = await service.bus.publish(new OrderPlaced({ orderId: '7' }), {
        key: 'order-7',
        correlationId: 'req-7',
      });
      const [record] = await readRaw(topic, 1);
      expect(record!.key).toBeNull();
      expect(record!.headers).toEqual({});
      expect(JSON.parse(record!.value!)).toEqual({
        id: published.id,
        requestId: 'req-7',
        provider: 'billing-api',
        timestamp: published.occurredAt.getTime(),
        createdDate: published.occurredAt.toISOString(),
        type: 'order.placed.v1',
        data: { orderId: '7' },
      });

      await produceRaw(topic, [
        {
          value:
            '{"provider":"orders-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.placed.v1","data":{"orderId":"8"},"userId":"u-1","attempt":2}',
        },
      ]);
      await waitFor(() => service.handled.length === 2);
      expect(service.handled[0]!.id).toBe(published.id);
      expect(service.handled[1]).toMatchObject({
        id: `kafka:${topic}:0:1`,
        source: 'orders-api',
        key: null,
        payload: { orderId: '8' },
        headers: { userId: 'u-1' },
        occurredAt: new Date('2026-10-25T14:00:00.123Z'),
      });
      await service.bus.stop();
    });
  });
});
