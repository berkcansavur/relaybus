import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import type { StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import { InvalidEnvelopeError, type EventEnvelope } from '@relaybus/core';
import { UnroutableMessageError } from '../src/errors.js';
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
} from './support/broker.js';
import { TcpProxy } from './support/tcp-proxy.js';
import { RabbitMqUnavailableError } from '../src/errors.js';

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: StartedRabbitMQContainer;
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

  it('dead-letters a poison message after the delivery limit instead of looping forever', async () => {
    const exchange = unique('events');
    const consumerGroup = unique('poison');
    let attempts = 0;
    const consumer = createTransport({
      exchange,
      consumerGroup,
      deliveryLimit: 2,
      handlerRetry: { attempts: 2, initialDelay: 10, maxDelay: 10, jitter: 'none' },
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
    expect(attempts).toBe(4);
    const dead = await takeMessage(amqpUrl, deadQueue);
    expect(dead?.headers['cloudEvents:id']).toBe(envelope.id);
    expect(dead?.headers['x-first-death-reason']).toBe('rejected');
    expect((await inspectQueue(amqpUrl, `relaybus.${consumerGroup}`)).messageCount).toBe(0);
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
