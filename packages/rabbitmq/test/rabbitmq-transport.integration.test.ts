import amqplib from 'amqplib';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { InvalidEnvelopeError, type EventEnvelope } from '@relaybus/core';
import {
  AmqpLimitExceededError,
  UnroutableMessageError,
  UnserializablePayloadError,
} from '../src/errors.js';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import type { RabbitMqTransportOptions } from '../src/options.js';
import {
  eventually,
  inspectQueue,
  publishRaw,
  remoteEnvelope,
  startBroker,
  takeMessage,
  unique,
  type TestBroker,
} from './support/broker.js';
import { TcpProxy } from './support/tcp-proxy.js';
import { RabbitMqUnavailableError } from '../src/errors.js';

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: TestBroker;
let amqpUrl: string;
let proxy: TcpProxy;
const transports: RabbitMqTransport[] = [];

beforeAll(async () => {
  broker = await startBroker();
  amqpUrl = broker.getAmqpUrl();
  proxy = new TcpProxy(() => broker.getMappedPort(5672));
  await proxy.listen();
});

afterAll(async () => {
  await proxy.close();
  await broker.stop();
});

afterEach(async () => {
  proxy.refuse(false);
  await Promise.all(transports.splice(0).map((transport) => transport.close()));
});

/** Queue declarations as the broker reports them, read with the CLI inside the container. */
async function listQueueArguments(
  prefix: string,
): Promise<{ name: string; arguments: Record<string, unknown> }[]> {
  const { output } = await broker.exec([
    'rabbitmqctl',
    'list_queues',
    'name',
    'arguments',
    '--formatter',
    'json',
  ]);
  const queues = JSON.parse(output.slice(output.indexOf('['))) as {
    name: string;
    arguments: [string, string, unknown][];
  }[];
  return queues
    .filter((queue) => queue.name.startsWith(prefix))
    .map((queue) => ({
      name: queue.name,
      arguments: Object.fromEntries(queue.arguments.map(([key, , value]) => [key, value])),
    }));
}

function proxiedUrl(): string {
  return `amqp://guest:guest@127.0.0.1:${proxy.port}?heartbeat=5`;
}

function createTransport(options: Partial<RabbitMqTransportOptions> = {}): RabbitMqTransport {
  const transport = new RabbitMqTransport({
    url: amqpUrl,
    logger: silent,
    reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
    ...options,
  });
  transports.push(transport);
  return transport;
}

describe('RabbitMqTransport against a real broker', () => {
  it('delivers a confirmed publish to a subscriber with the envelope intact, then acknowledges it', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('billing');
    const received: EventEnvelope[] = [];
    const consumer = createTransport({ exchange, consumerGroup });
    await consumer.connect();
    await consumer.subscribe(['orders.*'], async (envelope) => {
      received.push(envelope);
    });
    const producer = createTransport({ exchange });
    await producer.connect();

    const envelope = remoteEnvelope();
    await producer.publish(envelope, 'orders.placed');

    await eventually(() => received.length === 1);
    expect(received[0]).toEqual(envelope);
    await eventually(
      async () => (await inspectQueue(amqpUrl, `relaybus.${consumerGroup}`)).messageCount === 0,
    );
  });

  it('shares one queue between consumers of the same group and gives each group every message', async () => {
    const exchange = unique('events');
    const group = unique('shared');
    const perConsumer = [0, 0];
    const otherGroup: string[] = [];
    for (const index of [0, 1]) {
      const consumer = createTransport({ exchange, consumerGroup: group });
      await consumer.connect();
      await consumer.subscribe(['orders.placed'], async () => {
        perConsumer[index]! += 1;
      });
    }
    const independent = createTransport({ exchange, consumerGroup: unique('audit') });
    await independent.connect();
    await independent.subscribe(['orders.#'], async (envelope) => {
      otherGroup.push(envelope.id);
    });
    const producer = createTransport({ exchange });
    await producer.connect();
    for (let count = 0; count < 10; count += 1) {
      await producer.publish(remoteEnvelope(), 'orders.placed');
    }
    await eventually(() => perConsumer[0]! + perConsumer[1]! === 10 && otherGroup.length === 10);
    expect(perConsumer[0]).toBeGreaterThan(0);
    expect(perConsumer[1]).toBeGreaterThan(0);
  });

  it('rejects a publish nobody is bound to, instead of losing it', async () => {
    const producer = createTransport({ exchange: unique('events') });
    await producer.connect();
    await expect(producer.publish(remoteEnvelope(), 'nobody.listens')).rejects.toThrow(
      UnroutableMessageError,
    );
    await expect(producer.publish(remoteEnvelope(), 'nobody.listens')).rejects.toThrow(
      UnroutableMessageError,
    );
    expect(producer.isConnected()).toBe(true);
  });

  it('lets unroutable publishes resolve when explicitly allowed', async () => {
    const producer = createTransport({ exchange: unique('events'), allowUnroutable: true });
    await producer.connect();
    await producer.publish(remoteEnvelope(), 'nobody.listens');
  });

  it('waits out a handler outage with growing delays instead of dead-lettering, then processes the event', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('outage');
    const queue = `relaybus.${consumerGroup}`;
    const outageMs = 20_000;
    const calls: number[] = [];
    const processed: string[] = [];
    const consumer = createTransport({
      exchange,
      consumerGroup,
      // Default policy shape with the delays scaled down, so the 20 s outage spans several redeliveries.
      redelivery: { initialDelay: '500ms', factor: 2, maxDelay: '4s', jitter: 'none' },
      handlerRetry: { attempts: 2, initialDelay: 20, maxDelay: 20, jitter: 'none' },
    });
    await consumer.connect();
    const started = Date.now();
    await consumer.subscribe(['orders.placed'], async (envelope) => {
      calls.push(Date.now() - started);
      if (Date.now() - started < outageMs) {
        throw new Error('database down');
      }
      processed.push(envelope.id);
    });
    const producer = createTransport({ exchange });
    await producer.connect();
    const envelope = remoteEnvelope();
    await producer.publish(envelope, 'orders.placed');

    await eventually(() => processed.length === 1, 60_000, 'the event to be processed');
    expect(processed).toEqual([envelope.id]);
    // 2 local attempts per delivery, redeliveries after 0.5 s, 1 s, 2 s, then every 4 s: no burst.
    expect(calls.length).toBeGreaterThanOrEqual(8);
    expect(calls.length).toBeLessThanOrEqual(20);
    await eventually(async () => (await inspectQueue(amqpUrl, queue)).messageCount === 0);
    expect((await inspectQueue(amqpUrl, `${queue}.dead`)).messageCount).toBe(0);
    for (const delay of [500, 1_000, 2_000, 4_000]) {
      expect((await inspectQueue(amqpUrl, `${queue}.retry.${delay}`)).messageCount).toBe(0);
    }
  });

  it('dead-letters a message that keeps failing once the retry window is over, with its reason and without retry state', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('window');
    let attempts = 0;
    const consumer = createTransport({
      exchange,
      consumerGroup,
      handlerRetry: { attempts: 1 },
      redelivery: { initialDelay: 100, factor: 2, maxDelay: 400, jitter: 'none', maxAge: 1_500 },
    });
    await consumer.connect();
    await consumer.subscribe(['orders.placed'], async () => {
      attempts += 1;
      throw new Error('cannot process');
    });
    const producer = createTransport({ exchange });
    await producer.connect();
    const envelope = remoteEnvelope();
    await producer.publish(envelope, 'orders.placed');

    const deadQueue = `relaybus.${consumerGroup}.dead`;
    await eventually(async () => (await inspectQueue(amqpUrl, deadQueue)).messageCount === 1);
    expect(attempts).toBeGreaterThanOrEqual(4);
    const dead = await takeMessage(amqpUrl, deadQueue);
    expect(dead?.headers['cloudEvents:id']).toBe(envelope.id);
    expect(dead?.headers['x-relaybus-dead-letter-reason']).toBe('redelivery window exhausted');
    expect(dead?.headers).not.toHaveProperty('x-relaybus-redeliveries');
    expect(dead?.headers).not.toHaveProperty('x-relaybus-first-failure-at');
    expect((await inspectQueue(amqpUrl, `relaybus.${consumerGroup}`)).messageCount).toBe(0);
  });

  it('lets the broker dead-letter a message that crashes its consumer past the delivery limit', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('crashy');
    const queue = `relaybus.${consumerGroup}`;
    const topology = createTransport({ exchange, consumerGroup, deliveryLimit: 2 });
    await topology.connect();
    await topology.subscribe(['orders.placed'], async () => undefined);
    await topology.close();
    const producer = createTransport({ exchange });
    await producer.connect();
    await producer.publish(remoteEnvelope(), 'orders.placed');

    // A consumer that takes the message and dies without settling it, twice.
    let crashes = 0;
    for (; crashes < 5; crashes += 1) {
      const connection = await amqplib.connect(amqpUrl);
      const channel = await connection.createChannel();
      const delivery = await channel.get(queue, { noAck: false });
      await connection.close();
      if (delivery === false) {
        break;
      }
    }
    expect(crashes).toBeGreaterThanOrEqual(2);
    expect(crashes).toBeLessThan(5);
    await eventually(
      async () => (await inspectQueue(amqpUrl, `${queue}.dead`)).messageCount === 1,
      30_000,
      'the broker to dead-letter',
    );
    const dead = await takeMessage(amqpUrl, `${queue}.dead`);
    expect(dead?.headers['x-first-death-reason']).toBe('delivery_limit');
  });

  it('dead-letters non-retryable failures and malformed messages at once', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('invalid');
    let handled = 0;
    const consumer = createTransport({ exchange, consumerGroup });
    await consumer.connect();
    await consumer.subscribe(['orders.placed'], async () => {
      handled += 1;
      throw new InvalidEnvelopeError('unknown event shape');
    });
    const producer = createTransport({ exchange });
    await producer.connect();
    await producer.publish(remoteEnvelope(), 'orders.placed');
    await publishRaw(amqpUrl, exchange, 'orders.placed', Buffer.from('not a cloud event'));

    const deadQueue = `relaybus.${consumerGroup}.dead`;
    await eventually(async () => (await inspectQueue(amqpUrl, deadQueue)).messageCount === 2);
    expect(handled).toBe(1);
    const reasons = [
      (await takeMessage(amqpUrl, deadQueue))?.headers['x-relaybus-dead-letter-reason'],
      (await takeMessage(amqpUrl, deadQueue))?.headers['x-relaybus-dead-letter-reason'],
    ];
    expect(reasons.sort()).toEqual(['malformed message', 'non-retryable handler error']);
    expect((await inspectQueue(amqpUrl, `relaybus.${consumerGroup}`)).messageCount).toBe(0);
  });

  it('refuses unserializable payloads and over-long ids without touching the broker', async () => {
    const producer = createTransport({ exchange: unique('events') });
    await producer.connect();
    await expect(producer.publish(remoteEnvelope({ payload: { n: 1n } }), 'a.b')).rejects.toThrow(
      UnserializablePayloadError,
    );
    await expect(producer.publish(remoteEnvelope({ id: 'x'.repeat(300) }), 'a.b')).rejects.toThrow(
      AmqpLimitExceededError,
    );
    expect(producer.isReady()).toBe(true);
  });

  it('reports readiness, losing it with the connection', async () => {
    const consumer = createTransport({
      url: proxiedUrl(),
      exchange: unique('events'),
      consumerGroup: unique('ready'),
    });
    expect(consumer.isReady()).toBe(false);
    await consumer.connect();
    await consumer.subscribe(['orders.placed'], async () => undefined);
    expect(consumer.isReady()).toBe(true);
    proxy.refuse(true);
    proxy.sever();
    await eventually(() => !consumer.isReady(), 30_000, 'readiness to drop');
    proxy.refuse(false);
    await eventually(() => consumer.isReady(), 30_000, 'readiness to return');
  });

  it('keeps strict queue order in ordered mode, even while a message keeps failing, with one active consumer', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('ordered');
    const queue = `relaybus.${consumerGroup}`;
    const seen: string[] = [];
    const activeConsumers = new Set<number>();
    let firstAttempts = 0;
    const options = {
      exchange,
      consumerGroup,
      ordered: true,
      handlerRetry: { attempts: 2, initialDelay: 10, maxDelay: 10, jitter: 'none' as const },
      redelivery: { initialDelay: 50, maxDelay: 50, jitter: 'none' as const },
    };
    const consumers = [createTransport(options), createTransport(options)];
    for (const [index, consumer] of consumers.entries()) {
      await consumer.connect();
      await consumer.subscribe(['orders.placed'], async (envelope) => {
        activeConsumers.add(index);
        if (envelope.key === 'k0') {
          firstAttempts += 1;
          if (firstAttempts < 6) {
            throw new Error('first message is flaky');
          }
        }
        seen.push(envelope.key!);
        await new Promise((resolve) => setTimeout(resolve, 5));
      });
    }
    const producer = createTransport({ exchange });
    await producer.connect();
    const keys = Array.from({ length: 12 }, (_, index) => `k${index}`);
    for (const key of keys) {
      await producer.publish(remoteEnvelope({ key }), 'orders.placed');
    }
    await eventually(() => seen.length === keys.length, 60_000, 'all messages');
    expect(seen).toEqual(keys);
    expect(activeConsumers.size).toBe(1);
    expect((await inspectQueue(amqpUrl, `${queue}.dead`)).messageCount).toBe(0);

    // The standby takes over when the active consumer goes away.
    const [active] = activeConsumers;
    await consumers[active!]!.close();
    activeConsumers.clear();
    await producer.publish(remoteEnvelope({ key: 'after-failover' }), 'orders.placed');
    await eventually(() => seen.includes('after-failover'), 30_000, 'the standby consumer');
    expect([...activeConsumers]).toEqual([1 - active!]);
  });

  it('declares the queues with at-least-once dead-lettering and reject-publish overflow', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('args');
    const consumer = createTransport({ exchange, consumerGroup });
    await consumer.connect();
    await consumer.subscribe(['orders.placed'], async () => undefined);
    const queues = await listQueueArguments(`relaybus.${consumerGroup}`);
    const main = queues.find((queue) => queue.name === `relaybus.${consumerGroup}`)!;
    expect(main.arguments).toMatchObject({
      'x-queue-type': 'quorum',
      'x-dead-letter-strategy': 'at-least-once',
      'x-overflow': 'reject-publish',
      'x-dead-letter-exchange': `${exchange}.dead`,
    });
    const retryQueues = queues.filter((queue) => queue.name.includes('.retry.'));
    expect(retryQueues).toHaveLength(9);
    expect(retryQueues[0]!.arguments).toMatchObject({
      'x-dead-letter-strategy': 'at-least-once',
      'x-overflow': 'reject-publish',
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': `relaybus.${consumerGroup}`,
    });
  });

  it('refuses to start on a queue declared by an older version, rather than corrupting it', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('legacy');
    const connection = await amqplib.connect(amqpUrl);
    const channel = await connection.createChannel();
    await channel.assertQueue(`relaybus.${consumerGroup}`, {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': 10,
        'x-dead-letter-exchange': `${exchange}.dead`,
        'x-dead-letter-routing-key': `relaybus.${consumerGroup}`,
      },
    });
    await connection.close();
    const consumer = createTransport({ exchange, consumerGroup });
    await consumer.connect();
    await expect(consumer.subscribe(['orders.placed'], async () => undefined)).rejects.toThrow(
      /PRECONDITION/i,
    );
  });

  it('retries a transient handler failure on the same delivery and then acknowledges', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('flaky');
    let attempts = 0;
    const consumer = createTransport({
      exchange,
      consumerGroup,
      handlerRetry: { attempts: 3, initialDelay: 10, maxDelay: 10, jitter: 'none' },
    });
    await consumer.connect();
    await consumer.subscribe(['orders.placed'], async () => {
      attempts += 1;
      if (attempts < 3) {
        throw new Error('database down');
      }
    });
    const producer = createTransport({ exchange });
    await producer.connect();
    await producer.publish(remoteEnvelope(), 'orders.placed');
    await eventually(async () => {
      const { messageCount } = await inspectQueue(amqpUrl, `relaybus.${consumerGroup}`);
      return attempts === 3 && messageCount === 0;
    });
    const { messageCount } = await inspectQueue(amqpUrl, `relaybus.${consumerGroup}.dead`);
    expect(messageCount).toBe(0);
  });

  it('waits for a running handler on close and acknowledges its message', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('graceful');
    let started = false;
    let finished = false;
    const consumer = createTransport({ exchange, consumerGroup });
    await consumer.connect();
    await consumer.subscribe(['orders.placed'], async () => {
      started = true;
      await new Promise((resolve) => setTimeout(resolve, 300));
      finished = true;
    });
    const producer = createTransport({ exchange });
    await producer.connect();
    await producer.publish(remoteEnvelope(), 'orders.placed');
    await eventually(() => started);
    await consumer.close();
    expect(finished).toBe(true);
    expect((await inspectQueue(amqpUrl, `relaybus.${consumerGroup}`)).messageCount).toBe(0);
  });

  it('fails startup when the broker is unreachable', async () => {
    const transport = new RabbitMqTransport({
      url: 'amqp://guest:guest@127.0.0.1:1',
      logger: silent,
    });
    await expect(transport.connect()).rejects.toThrow(/ECONNREFUSED/);
  });

  it('can start while the broker is down and connects once it is reachable', async () => {
    proxy.refuse(true);
    const transport = createTransport({ url: proxiedUrl(), failOnConnectError: false });
    await transport.connect();
    expect(transport.isConnected()).toBe(false);
    await expect(transport.publish(remoteEnvelope(), 'orders.placed')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    proxy.refuse(false);
    await eventually(() => transport.isConnected(), 30_000, 'the transport to connect');
  });

  it('rejects publishes during a connection loss, then reconnects and resumes consuming', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('resilient');
    const received: string[] = [];
    const transport = createTransport({ url: proxiedUrl(), exchange, consumerGroup });
    await transport.connect();
    await transport.subscribe(['orders.placed'], async (envelope) => {
      received.push(envelope.id);
    });
    const first = remoteEnvelope();
    await transport.publish(first, 'orders.placed');
    await eventually(() => received.includes(first.id));

    proxy.refuse(true);
    proxy.sever();
    await eventually(() => !transport.isConnected(), 30_000, 'the loss to be noticed');
    await expect(transport.publish(remoteEnvelope(), 'orders.placed')).rejects.toThrow(
      RabbitMqUnavailableError,
    );

    // A producer that still reaches the broker keeps filling the durable queue meanwhile.
    const direct = createTransport({ exchange });
    await direct.connect();
    const whileDown = remoteEnvelope();
    await direct.publish(whileDown, 'orders.placed');

    proxy.refuse(false);
    await eventually(() => transport.isConnected(), 30_000, 'the transport to reconnect');
    await eventually(() => received.includes(whileDown.id), 30_000, 'the queued message');
    const after = remoteEnvelope();
    await transport.publish(after, 'orders.placed');
    await eventually(() => received.includes(after.id));
    // At-least-once: `first` may be redelivered if its ack was cut off with the connection.
    expect(new Set(received)).toEqual(new Set([first.id, whileDown.id, after.id]));
  });
});
