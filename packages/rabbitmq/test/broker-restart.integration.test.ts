import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import { RabbitMqUnavailableError } from '../src/errors.js';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import { eventually, remoteEnvelope, unique } from './support/broker.js';
import { startDedicatedBroker } from './support/broker-server.js';
import { TcpProxy } from './support/tcp-proxy.js';

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: StartedRabbitMQContainer;
let proxy: TcpProxy;

beforeAll(async () => {
  broker = await startDedicatedBroker();
  proxy = new TcpProxy(() => broker.getMappedPort(5672));
  await proxy.listen();
});

afterAll(async () => {
  await proxy.close();
  await broker.stop();
});

describe('broker restart', () => {
  it('rejects publishes while the broker is down, then recovers topology, consumer and messages', async () => {
    const exchange = unique('events');
    const received: string[] = [];
    const transport = new RabbitMqTransport({
      url: `amqp://guest:guest@127.0.0.1:${proxy.port}?heartbeat=5`,
      exchange,
      consumerGroup: unique('durable'),
      reconnect: { initialDelay: '200ms', maxDelay: '1s' },
      logger: silent,
    });
    await transport.connect();
    await transport.subscribe(['orders.placed'], async (envelope) => {
      received.push(envelope.id);
    });
    const before = remoteEnvelope();
    await transport.publish(before, 'orders.placed');
    await eventually(() => received.includes(before.id));

    const restarting = broker.restart({ timeout: 5_000 });
    await eventually(() => !transport.isConnected(), 60_000, 'the restart to be noticed');
    await expect(transport.publish(remoteEnvelope(), 'orders.placed')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    await restarting;

    await eventually(() => transport.isConnected(), 120_000, 'the transport to reconnect');
    const after = remoteEnvelope();
    await transport.publish(after, 'orders.placed');
    await eventually(() => received.includes(after.id), 60_000, 'delivery after restart');
    expect(received).toEqual([before.id, after.id]);
    await transport.close();
  });
});
