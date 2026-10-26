import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { type Kafka, type Consumer } from 'kafkajs';
import {
  InMemoryEventStore,
  NonRetryableError,
  RelayBus,
  defineEvent,
  type EventEnvelope,
  type InMemoryTransaction,
} from '@relaybus/core';
import {
  KafkaTransport,
  type KafkaConsumerConfig,
  type KafkaConsumerLike,
  type KafkaRunConfig,
  type KafkaTransportOptions,
} from '../src/index.js';
import { createEnvelope, createLogger } from './fakes.js';
import {
  KAFKA_CLIENTS,
  withoutOwnProducerSettings,
  type KafkaClientUnderTest,
} from './support/kafka-clients.js';
import {
  brokerAddresses,
  createClient,
  createTopics,
  produceRaw,
  readRaw,
  startKafkaBroker,
  stopKafkaBroker,
  uniqueName,
  waitFor,
} from './support/kafka-broker.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

function transportFactory(client: KafkaClientUnderTest) {
  return (options: Partial<KafkaTransportOptions> = {}) => {
    const logger = createLogger();
    const transport = new KafkaTransport({
      kafka: client.createClient(brokerAddresses()),
      groupId: uniqueName('group'),
      logger,
      ...client.transportOptions,
      processing: { maxAttempts: 3, initialBackoff: 50, maxBackoff: 200 },
      ...options,
    });
    return { transport, logger };
  };
}

beforeAll(async () => {
  await startKafkaBroker();
});

afterAll(async () => {
  await stopKafkaBroker();
});

describe.each(KAFKA_CLIENTS)('KafkaTransport on $name against a real broker', (client) => {
  const createTransport = transportFactory(client);

  it('round-trips an envelope exactly and writes CloudEvents binary mode on the wire', async () => {
    const topic = uniqueName('orders');
    await createTopics(topic);
    const { transport: producerSide } = createTransport();
    const { transport: consumerSide } = createTransport();
    const received: EventEnvelope[] = [];
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    expect(consumerSide.isConsuming()).toBe(true);

    const envelope = createEnvelope({ headers: { ce_id: 'user-supplied', traceparent: 'tp' } });
    await producerSide.publish(envelope, topic);
    await waitFor(() => received.length === 1);
    expect(received[0]).toEqual(envelope);
    expect(received[0]!.occurredAt).toBeInstanceOf(Date);

    const [wire] = await readRaw(topic, 1);
    expect(wire!.key).toBe('order-1');
    expect(wire!.value).toBe('{"orderId":"1"}');
    expect(wire!.headers).toMatchObject({
      ce_specversion: '1.0',
      ce_id: envelope.id,
      ce_type: 'order.placed.v1',
      ce_source: 'orders',
      ce_time: '2026-01-01T00:00:00.000Z',
      ce_subject: 'order-1',
      ce_correlationid: 'flow-1',
      ce_causationid: 'cause-1',
      'content-type': 'application/json',
      traceparent: 'tp',
      'x-relaybus-h-ce_id': 'user-supplied',
    });

    await producerSide.close();
    await consumerSide.close();
    expect(consumerSide.isConsuming()).toBe(false);
  });

  it('keeps the order of events that share a key', async () => {
    const topic = uniqueName('ordered');
    await createTopics(topic);
    const { transport: producerSide } = createTransport();
    const { transport: consumerSide } = createTransport();
    const sequence: string[] = [];
    await consumerSide.subscribe([topic], async (envelope) => {
      sequence.push((envelope.payload as { n: string }).n);
    });
    const numbers = Array.from({ length: 20 }, (_, index) => String(index));
    for (const n of numbers) {
      await producerSide.publish(createEnvelope({ id: `id-${n}`, payload: { n } }), topic);
    }
    await waitFor(() => sequence.length === numbers.length);
    expect(sequence).toEqual(numbers);
    await producerSide.close();
    await consumerSide.close();
  });

  it('delivers events published before the group first joined (fromBeginning)', async () => {
    const topic = uniqueName('early');
    await createTopics(topic);
    const { transport: producerSide } = createTransport();
    await producerSide.publish(createEnvelope(), topic);
    const { transport: consumerSide } = createTransport();
    const received: EventEnvelope[] = [];
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    await waitFor(() => received.length === 1);
    await producerSide.close();
    await consumerSide.close();
  });

  it('redelivers a record whose handler failed beyond the in-process attempts, losing nothing', async () => {
    const topic = uniqueName('redelivery');
    await createTopics(topic);
    const { transport: producerSide } = createTransport();
    const { transport: consumerSide, logger } = createTransport({
      processing: { maxAttempts: 2, initialBackoff: 20, maxBackoff: 20 },
    });
    let calls = 0;
    const stored: string[] = [];
    await consumerSide.subscribe([topic], async (envelope) => {
      calls += 1;
      if (calls <= 5) {
        throw new Error('database unavailable');
      }
      stored.push(envelope.id);
    });
    await producerSide.publish(createEnvelope({ id: 'first' }), topic);
    await producerSide.publish(createEnvelope({ id: 'second' }), topic);
    await waitFor(() => stored.length === 2, 90_000);
    expect(stored).toEqual(['first', 'second']);
    expect(calls).toBeGreaterThan(5);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('will be redelivered'));
    await waitFor(() => consumerSide.isConsuming());
    await producerSide.close();
    await consumerSide.close();
  }, 120_000);

  it('skips malformed records with a logged error and keeps consuming', async () => {
    const topic = uniqueName('malformed');
    await createTopics(topic);
    const { transport: consumerSide, logger } = createTransport({ onInvalidMessage: 'skip' });
    const received: EventEnvelope[] = [];
    await produceRaw(topic, [
      { value: 'not even json' },
      { value: '{}', headers: { ce_specversion: '1.0' } },
    ]);
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    const { transport: producerSide } = createTransport();
    await producerSide.publish(createEnvelope({ id: 'valid' }), topic);
    await waitFor(() => received.length === 1);
    expect(received[0]!.id).toBe('valid');
    expect(logger.error).toHaveBeenCalledTimes(2);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Skipping malformed message'),
    );
    await producerSide.close();
    await consumerSide.close();
  });

  it('dead-letters malformed records to a topic with diagnostic headers', async () => {
    const topic = uniqueName('malformed-dlq');
    const deadLetterTopic = `${topic}.dlq`;
    await createTopics(topic, deadLetterTopic);
    const { transport: consumerSide } = createTransport({ onInvalidMessage: { deadLetterTopic } });
    const received: EventEnvelope[] = [];
    await produceRaw(topic, [{ key: 'k1', value: 'garbage', headers: { origin: 'legacy' } }]);
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    const [deadLetter] = await readRaw(deadLetterTopic, 1);
    expect(received).toEqual([]);
    expect(deadLetter).toMatchObject({
      key: 'k1',
      value: 'garbage',
      headers: {
        origin: 'legacy',
        'relaybus-source-topic': topic,
        'relaybus-source-partition': '0',
        'relaybus-source-offset': '0',
      },
    });
    expect(deadLetter!.headers['relaybus-invalid-reason']).toContain('ce_specversion');
    await consumerSide.close();
  });

  it('dead-letters malformed records to <topic>.dead by default and keeps consuming', async () => {
    const topic = uniqueName('malformed-default');
    await createTopics(topic, `${topic}.dead`);
    const { transport: consumerSide } = createTransport();
    const received: EventEnvelope[] = [];
    await produceRaw(topic, [{ key: 'k1', value: 'garbage', headers: { origin: 'legacy' } }]);
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    const [deadLetter] = await readRaw(`${topic}.dead`, 1);
    expect(deadLetter).toMatchObject({
      key: 'k1',
      value: 'garbage',
      headers: { origin: 'legacy' },
    });
    const { transport: producerSide } = createTransport();
    await producerSide.publish(createEnvelope({ id: 'valid' }), topic);
    await waitFor(() => received.length === 1);
    await producerSide.close();
    await consumerSide.close();
  });

  it('dead-letters an event the handler rejects with a NonRetryableError', async () => {
    const topic = uniqueName('non-retryable');
    await createTopics(topic, `${topic}.dead`);
    const { transport: producerSide } = createTransport();
    const { transport: consumerSide } = createTransport();
    await consumerSide.subscribe([topic], async () => {
      throw new NonRetryableError('schema mismatch');
    });
    await producerSide.publish(createEnvelope({ id: 'poison' }), topic);
    const [deadLetter] = await readRaw(`${topic}.dead`, 1);
    expect(deadLetter!.headers['relaybus-invalid-reason']).toBe('schema mismatch');
    expect(deadLetter!.headers['ce_id']).toBe('poison');
    await producerSide.close();
    await consumerSide.close();
  });

  it('decodes a record from a plain CloudEvents producer that sends no ce_time', async () => {
    const topic = uniqueName('plain-cloudevents');
    await createTopics(topic);
    const { transport: consumerSide } = createTransport();
    const received: EventEnvelope[] = [];
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    const before = Date.now();
    await produceRaw(topic, [
      {
        value: '{"orderId":"9"}',
        headers: {
          ce_specversion: '1.0',
          ce_id: 'plain-1',
          ce_type: 'order.placed.v1',
          ce_source: '//legacy',
          'content-type': 'application/json',
        },
      },
    ]);
    await waitFor(() => received.length === 1);
    expect(received[0]!.id).toBe('plain-1');
    expect(received[0]!.occurredAt.getTime()).toBeGreaterThanOrEqual(before - 5_000);
    await consumerSide.close();
  });

  it.each([
    ['TypeError', () => new TypeError('x is not a function')],
    ['RangeError', () => new RangeError('out of range')],
    ['ReferenceError', () => new ReferenceError('y is not defined')],
    ['SyntaxError', () => new SyntaxError('bad json')],
  ])(
    'keeps consuming and delivers the event after a handler threw a %s repeatedly',
    async (_name, createError) => {
      const topic = uniqueName('handler-bug');
      await createTopics(topic);
      const { transport: producerSide } = createTransport();
      const { transport: consumerSide } = createTransport({
        processing: { maxAttempts: 2, initialBackoff: 20, maxBackoff: 20 },
      });
      let calls = 0;
      const stored: string[] = [];
      await consumerSide.subscribe([topic], async (envelope) => {
        calls += 1;
        if (calls <= 5) {
          throw createError();
        }
        stored.push(envelope.id);
      });
      await producerSide.publish(createEnvelope({ id: 'survivor' }), topic);
      await waitFor(() => stored.length === 1, 90_000);
      expect(stored).toEqual(['survivor']);
      await waitFor(() => consumerSide.isConsuming());
      await producerSide.close();
      await consumerSide.close();
    },
    120_000,
  );

  it.runIf(client.name === 'kafkajs')(
    'restarts the consumer run loop after the client reported an unrecoverable crash',
    async () => {
      const topic = uniqueName('crash-restart');
      await createTopics(topic);
      const kafkajs = createClient();
      let crashed = false;
      // Makes kafkajs see a raw TypeError from eachMessage, which it marks as unrecoverable.
      const crashingClient = {
        producer: (config: Parameters<Kafka['producer']>[0]) => kafkajs.producer(config),
        consumer: (config: Parameters<Kafka['consumer']>[0]) => {
          const consumer = kafkajs.consumer(config);
          return Object.assign(Object.create(consumer) as Consumer, {
            run: (runConfig: NonNullable<Parameters<Consumer['run']>[0]>) =>
              consumer.run({
                eachMessage: async (payload) => {
                  if (!crashed) {
                    crashed = true;
                    throw new TypeError('raw bug escaped the transport');
                  }
                  await runConfig.eachMessage!(payload);
                },
              }),
          });
        },
      };
      const { transport: producerSide } = createTransport();
      const { transport: consumerSide, logger } = createTransport({
        kafka: crashingClient as unknown as Kafka,
        restart: { initialBackoff: 100, maxBackoff: 500 },
      });
      const received: string[] = [];
      await consumerSide.subscribe([topic], async (envelope) => {
        received.push(envelope.id);
      });
      await producerSide.publish(createEnvelope({ id: 'after-crash' }), topic);
      await waitFor(() => received.length === 1, 90_000);
      expect(received).toEqual(['after-crash']);
      expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('restart=false'));
      expect(logger.log).toHaveBeenCalledWith(expect.stringContaining('Consumer restarted'));
      await waitFor(() => consumerSide.isConsuming());
      expect(consumerSide.isReady()).toBe(false);
      await producerSide.close();
      await consumerSide.close();
    },
    120_000,
  );

  // Confluent never gives up on a consumer: an error escaping eachMessage seeks back to the record.
  it.runIf(client.name === 'confluent')(
    'redelivers a record after a raw error escaped eachMessage, without a crash or restart',
    async () => {
      const topic = uniqueName('raw-error');
      await createTopics(topic);
      const kafka = client.createClient(brokerAddresses());
      let thrown = false;
      const throwingClient = {
        producer: kafka.producer.bind(kafka),
        consumer: (config: KafkaConsumerConfig): KafkaConsumerLike => {
          const consumer = kafka.consumer(config);
          return {
            connect: () => consumer.connect(),
            subscribe: (subscription) => consumer.subscribe(subscription),
            disconnect: () => consumer.disconnect(),
            run: (runConfig: KafkaRunConfig) =>
              consumer.run({
                eachMessage: async (payload) => {
                  if (!thrown) {
                    thrown = true;
                    throw new TypeError('raw bug escaped the transport');
                  }
                  await runConfig.eachMessage(payload);
                },
              }),
          };
        },
      };
      const { transport: producerSide } = createTransport();
      const { transport: consumerSide, logger } = createTransport({ kafka: throwingClient });
      const received: string[] = [];
      await consumerSide.subscribe([topic], async (envelope) => {
        received.push(envelope.id);
      });
      await producerSide.publish(createEnvelope({ id: 'after-error' }), topic);
      await waitFor(() => received.length === 1, 90_000);
      expect(received).toEqual(['after-error']);
      expect(thrown).toBe(true);
      expect(logger.error).not.toHaveBeenCalled();
      expect(consumerSide.isConsuming()).toBe(true);
      await producerSide.close();
      await consumerSide.close();
    },
    120_000,
  );

  it('fails start-up when the broker is unreachable instead of looking healthy', async () => {
    const { transport } = createTransport({
      kafka: client.createClient(['127.0.0.1:1'], { unreachable: true }),
    });
    await expect(transport.subscribe(['any'], async () => undefined)).rejects.toThrow();
    expect(transport.isConsuming()).toBe(false);
    await transport.close().catch(() => undefined);
  });

  it('publishes through an injected, already-connected producer and leaves it open on close', async () => {
    const topic = uniqueName('injected');
    await createTopics(topic);
    const appProducer = client.createClient(brokerAddresses()).producer(client.appProducerConfig);
    await appProducer.connect();
    const { transport: consumerSide } = createTransport();
    const received: EventEnvelope[] = [];
    await consumerSide.subscribe([topic], async (envelope) => {
      received.push(envelope);
    });
    // Publish-only: no client of its own, so no second connection.
    const clientOptions = withoutOwnProducerSettings(client);
    const publisher = new KafkaTransport({
      ...clientOptions,
      producer: appProducer,
      logger: createLogger(),
    });
    await publisher.connect();
    expect(publisher.isReady()).toBe(true);
    await publisher.publish(createEnvelope({ id: 'through-app-producer' }), topic);
    await waitFor(() => received.length === 1);
    expect(received[0]!.id).toBe('through-app-producer');
    await publisher.close();

    // Still connected: the application keeps using it after the transport closed.
    await appProducer.send({
      topic,
      messages: [{ key: null, value: 'after close', headers: {} }],
    });
    const records = await readRaw(topic, 2);
    expect(records.map((record) => record.value)).toContain('after close');
    await appProducer.disconnect();
    await consumerSide.close();
  });

  it('dead-letters through an injected producer while consuming with its own client', async () => {
    const topic = uniqueName('injected-dlq');
    await createTopics(topic, `${topic}.dead`);
    const appProducer = client.createClient(brokerAddresses()).producer(client.appProducerConfig);
    await appProducer.connect();
    const clientOptions = withoutOwnProducerSettings(client);
    const consumerSide = new KafkaTransport({
      ...clientOptions,
      kafka: client.createClient(brokerAddresses()),
      groupId: uniqueName('group'),
      producer: appProducer,
      logger: createLogger(),
    });
    await produceRaw(topic, [{ key: 'k1', value: 'garbage' }]);
    await consumerSide.subscribe([topic], async () => undefined);
    const [deadLetter] = await readRaw(`${topic}.dead`, 1);
    expect(deadLetter).toMatchObject({ key: 'k1', value: 'garbage' });
    await consumerSide.close();
    await appProducer.send({ topic, messages: [{ key: null, value: 'still open', headers: {} }] });
    await appProducer.disconnect();
  });

  it('closes gracefully: stops consuming, rejects publishes, leaves the group', async () => {
    const topic = uniqueName('close');
    await createTopics(topic);
    const { transport } = createTransport();
    await transport.subscribe([topic], async () => undefined);
    await transport.connect();
    await transport.close();
    expect(transport.isConsuming()).toBe(false);
    await expect(transport.publish(createEnvelope(), topic)).rejects.toThrow(/closed/);
    await transport.close();
  });
});

describe.each(KAFKA_CLIENTS)('RelayBus across Kafka on $name', (client) => {
  const createTransport = transportFactory(client);

  interface Service {
    readonly bus: RelayBus<InMemoryTransaction>;
    readonly store: InMemoryEventStore;
    readonly transport: KafkaTransport;
  }

  function createService(
    name: string,
    topic: string,
    options: { routes?: boolean; subscriptions?: boolean },
  ): Service {
    const store = new InMemoryEventStore();
    const { transport } = createTransport({ name: 'kafka' });
    const bus = new RelayBus<InMemoryTransaction>({
      store,
      source: name,
      transactionMode: 'optional',
      transports: [transport],
      routes:
        options.routes === true
          ? [{ event: OrderPlaced, transport: 'kafka', destination: topic }]
          : [],
      subscriptions:
        options.subscriptions === true ? [{ transport: 'kafka', destinations: [topic] }] : [],
      dispatcher: { pollInterval: 25 },
      logger: createLogger(),
    });
    return { bus, store, transport };
  }

  it('delivers an event from service A through Kafka to a handler in service B', async () => {
    const topic = uniqueName('orders');
    await createTopics(topic);
    const serviceB = createService('billing', topic, { subscriptions: true });
    const handled: EventEnvelope[] = [];
    serviceB.bus.subscribe(
      OrderPlaced,
      async (event) => {
        handled.push(event);
      },
      { id: 'billing-handler' },
    );
    await serviceB.bus.start();
    const serviceA = createService('orders', topic, { routes: true });
    await serviceA.bus.start();

    const published = await serviceA.bus.publish(new OrderPlaced({ orderId: '42' }), {
      key: 'order-42',
    });
    await waitFor(() => handled.length === 1);
    expect(handled[0]).toEqual(published);
    expect(handled[0]).toMatchObject({
      source: 'orders',
      key: 'order-42',
      payload: { orderId: '42' },
    });
    expect(serviceB.transport.isConsuming()).toBe(true);

    await serviceA.bus.stop();
    await serviceB.bus.stop();
    expect(serviceB.transport.isConsuming()).toBe(false);
  });

  it('stores a duplicated Kafka message once and runs the handler once', async () => {
    const topic = uniqueName('orders-duplicate');
    await createTopics(topic);
    const serviceB = createService('billing', topic, { subscriptions: true });
    const handler = vi.fn(async (_event: EventEnvelope) => undefined);
    serviceB.bus.subscribe(OrderPlaced, handler, { id: 'billing-handler' });
    await serviceB.bus.start();

    const { transport: producerSide } = createTransport();
    const envelope = createEnvelope({ id: 'duplicate-1' });
    await producerSide.publish(envelope, topic);
    await producerSide.publish(envelope, topic);
    await producerSide.publish(createEnvelope({ id: 'marker' }), topic);
    await waitFor(() => handler.mock.calls.length >= 2);
    await serviceB.bus.drain();
    const handledIds = handler.mock.calls.map(([event]) => event.id);
    expect(handledIds.sort()).toEqual(['duplicate-1', 'marker']);

    await producerSide.close();
    await serviceB.bus.stop();
  });

  it('keeps the event when the inbound store fails, and stores it after recovery', async () => {
    const topic = uniqueName('orders-store-outage');
    await createTopics(topic);
    const serviceB = createService('billing', topic, { subscriptions: true });
    const handled: string[] = [];
    serviceB.bus.subscribe(
      OrderPlaced,
      async (event) => {
        handled.push(event.id);
      },
      { id: 'billing-handler' },
    );
    const originalTransaction = serviceB.store.transaction.bind(serviceB.store);
    let outage = true;
    const transactionSpy = vi
      .spyOn(serviceB.store, 'transaction')
      .mockImplementation(async (work) => {
        if (outage) {
          throw new Error('database unavailable');
        }
        return originalTransaction(work);
      });
    await serviceB.bus.start();
    const { transport: producerSide } = createTransport();
    await producerSide.publish(createEnvelope({ id: 'survivor' }), topic);
    await waitFor(() => transactionSpy.mock.calls.length >= 4);
    expect(handled).toEqual([]);
    outage = false;
    await waitFor(() => handled.length === 1, 90_000);
    expect(handled).toEqual(['survivor']);

    await producerSide.close();
    await serviceB.bus.stop();
  }, 120_000);
});
