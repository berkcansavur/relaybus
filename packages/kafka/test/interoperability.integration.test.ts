import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  type EventEnvelope,
  type InMemoryTransaction,
} from '@relaybus/core';
import {
  KafkaTransport,
  decodeCloudEvent,
  fromHeader,
  plainJsonDecoder,
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

  interface ConsumingService {
    readonly bus: RelayBus<InMemoryTransaction>;
    readonly store: InMemoryEventStore;
    readonly handled: EventEnvelope[];
    readonly statuses: string[];
  }

  /** A relaybus service consuming `topic` through `decode`, with one handler for `order.placed.v1`. */
  async function startConsumingService(
    topic: string,
    options: Partial<KafkaTransportOptions>,
    prepare: (store: InMemoryEventStore) => void = () => undefined,
  ): Promise<ConsumingService> {
    const store = new InMemoryEventStore();
    const handled: EventEnvelope[] = [];
    const statuses: string[] = [];
    const bus = new RelayBus<InMemoryTransaction>({
      store,
      source: 'billing',
      transactionMode: 'optional',
      transports: [createTransport({ name: 'kafka', ...options })],
      subscriptions: [{ transport: 'kafka', destinations: [topic] }],
      dispatcher: { pollInterval: 25 },
      observers: [{ onReceived: ({ status }) => statuses.push(status) }],
      logger: createLogger(),
    });
    bus.subscribe(
      'order.placed.v1',
      async (event) => {
        handled.push(event);
      },
      { id: 'billing.on-order-placed' },
    );
    prepare(store);
    await bus.start();
    return { bus, store, handled, statuses };
  }

  describe('relaybus producer to a consumer that does not use relaybus', () => {
    it('lets a plain kafkajs consumer read the JSON payload and the type and id headers', async () => {
      const topic = uniqueName('to-foreign');
      await createTopics(topic);
      const producer = createTransport();
      const envelope = createEnvelope({ payload: { orderId: '77', total: 12.5 } });
      await producer.publish(envelope, topic);

      const [record] = await readRaw(topic, 1);
      expect(JSON.parse(record!.value!)).toEqual({ orderId: '77', total: 12.5 });
      expect(record!.key).toBe('order-1');
      expect(record!.headers).toMatchObject({
        ce_specversion: '1.0',
        ce_type: 'order.placed.v1',
        ce_id: envelope.id,
        ce_source: 'orders',
        ce_time: '2026-01-01T00:00:00.000Z',
        'content-type': 'application/json',
      });
      await producer.close();
    });
  });

  describe('producer that does not use relaybus to a relaybus consumer', () => {
    it('handles plain JSON records once each, in order per key', async () => {
      const topic = uniqueName('from-foreign');
      await createTopics(topic);
      const service = await startConsumingService(topic, {
        decode: plainJsonDecoder({ type: fromHeader('event-type') }),
      });
      const keys = ['customer-a', 'customer-b', 'customer-c'];
      const messages = Array.from({ length: 30 }, (_, index) => ({
        key: keys[index % keys.length]!,
        value: JSON.stringify({ sequence: index }),
        headers: { 'event-type': 'order.placed.v1', origin: 'legacy-java' },
      }));
      await produceRaw(topic, messages);

      await waitFor(() => service.handled.length === messages.length);
      for (const key of keys) {
        const sequences = service.handled
          .filter((event) => event.key === key)
          .map((event) => (event.payload as { sequence: number }).sequence);
        expect(sequences).toEqual([...sequences].sort((left, right) => left - right));
        expect(sequences).toHaveLength(10);
      }
      expect(new Set(service.handled.map((event) => event.id)).size).toBe(messages.length);
      // Order holds per key only: the record at offset 0 is not necessarily the first one handled.
      expect(service.handled.find((event) => event.id === `kafka:${topic}:0:0`)).toMatchObject({
        id: `kafka:${topic}:0:0`,
        type: 'order.placed.v1',
        source: topic,
        headers: { 'event-type': 'order.placed.v1', origin: 'legacy-java' },
      });
      await service.bus.stop();
    });

    it('stores a redelivered plain JSON record once, because its derived id is stable', async () => {
      const topic = uniqueName('from-foreign-redelivered');
      await createTopics(topic);
      let failedAfterCommit = false;
      const service = await startConsumingService(
        topic,
        {
          decode: plainJsonDecoder({ type: 'order.placed.v1' }),
          processing: { maxAttempts: 1, initialBackoff: 10, maxBackoff: 10 },
        },
        (store) => {
          const transaction = store.transaction.bind(store);
          // The first receive commits and then fails, as if the process died before the offset commit.
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
      await produceRaw(topic, [{ key: 'order-1', value: '{"orderId":"1"}' }]);

      // The failed first receive reports nothing; the redelivery finds the stored event.
      await waitFor(() => service.statuses.length >= 1 && service.handled.length >= 1, 90_000);
      await service.bus.drain();
      expect(failedAfterCommit).toBe(true);
      expect(service.statuses).toEqual(['duplicate']);
      expect(service.handled.map((event) => event.id)).toEqual([`kafka:${topic}:0:0`]);
      await service.bus.stop();
    }, 120_000);

    it('dedupes two sends of one event by a producer-supplied id header', async () => {
      const topic = uniqueName('from-foreign-id');
      await createTopics(topic);
      const service = await startConsumingService(topic, {
        decode: plainJsonDecoder({ type: 'order.placed.v1', id: fromHeader('message-id') }),
      });
      const send = { value: '{"orderId":"1"}', headers: { 'message-id': 'msg-1' } };
      await produceRaw(topic, [send, send, { value: '{"orderId":"2"}' }]);

      await waitFor(() => service.statuses.length === 3);
      await service.bus.drain();
      expect(service.statuses).toEqual(['accepted', 'duplicate', 'accepted']);
      expect(service.handled.map((event) => event.id).sort()).toEqual(
        [`kafka:${topic}:0:2`, 'msg-1'].sort(),
      );
      await service.bus.stop();
    });

    it('handles a structured-mode CloudEvent from a CloudEvents SDK producer', async () => {
      const topic = uniqueName('from-structured');
      await createTopics(topic);
      const service = await startConsumingService(topic, { decode: decodeCloudEvent });
      await produceRaw(topic, [
        {
          key: 'order-5',
          value: JSON.stringify({
            specversion: '1.0',
            id: 'ce-5',
            type: 'order.placed.v1',
            source: '//go/orders',
            time: '2026-05-01T10:00:00Z',
            datacontenttype: 'application/json',
            data: { orderId: '5' },
            traceparent: '00-abc-def-01',
          }),
          headers: { 'content-type': 'application/cloudevents+json; charset=UTF-8' },
        },
      ]);

      await waitFor(() => service.handled.length === 1);
      expect(service.handled[0]).toMatchObject({
        id: 'ce-5',
        type: 'order.placed.v1',
        source: '//go/orders',
        key: 'order-5',
        payload: { orderId: '5' },
        headers: { traceparent: '00-abc-def-01' },
        occurredAt: new Date('2026-05-01T10:00:00Z'),
      });
      await service.bus.stop();
    });

    it('dead-letters garbage and keeps consuming the records after it', async () => {
      const topic = uniqueName('from-foreign-garbage');
      await createTopics(topic, `${topic}.dead`);
      const service = await startConsumingService(topic, {
        decode: plainJsonDecoder({ type: fromHeader('event-type') }),
      });
      await produceRaw(topic, [
        { key: 'a', value: 'this is not json', headers: { 'event-type': 'order.placed.v1' } },
        { key: 'b', value: null, headers: { 'event-type': 'order.placed.v1' } },
        { key: 'c', value: '{"orderId":"c"}' },
        { key: 'd', value: '{"orderId":"d"}', headers: { 'event-type': 'not a type' } },
        { key: 'e', value: '{"orderId":"e"}', headers: { 'event-type': 'order.placed.v1' } },
      ]);

      await waitFor(() => service.handled.length === 1);
      expect(service.handled[0]!.payload).toEqual({ orderId: 'e' });
      const deadLetters = await readRaw(`${topic}.dead`, 4);
      expect(deadLetters.map((record) => record.key)).toEqual(['a', 'b', 'c', 'd']);
      expect(deadLetters.map((record) => record.headers['relaybus-invalid-reason'])).toEqual([
        'Message value is not valid JSON',
        'Message has no value',
        'Record has no event type: the type option found none',
        expect.stringMatching(/^The event type is not a valid event type/),
      ]);
      await service.bus.stop();
    });
  });
});
