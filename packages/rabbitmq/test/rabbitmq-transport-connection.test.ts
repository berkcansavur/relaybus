import { describe, expect, it } from 'vitest';
import { RabbitMqUnavailableError } from '../src/errors.js';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import {
  type FakeChannel,
  FakeConnection,
  createEnvelope,
  createLogger,
  waitFor,
} from './fakes.js';
import { createTransport } from './support/transport-setup.js';

describe('RabbitMqTransport connection', () => {
  it('declares the exchange on connect and reports readiness', async () => {
    const { transport, connections } = createTransport({ name: 'broker', exchange: 'events' });
    expect(transport.name).toBe('broker');
    expect(transport.isConnected()).toBe(false);
    await transport.connect();
    expect(transport.isConnected()).toBe(true);
    expect(connections[0]!.publisher.assertExchange).toHaveBeenCalledWith('events', 'topic', {
      durable: true,
    });
    await transport.close();
    expect(transport.isConnected()).toBe(false);
  });

  it('fails startup when the broker is unreachable', async () => {
    const transport = new RabbitMqTransport({
      connect: async () => {
        throw new Error('ECONNREFUSED');
      },
    });
    await expect(transport.connect()).rejects.toThrow('ECONNREFUSED');
  });

  it('can keep trying in the background instead of failing startup', async () => {
    let reachable = false;
    const connection = new FakeConnection();
    const transport = new RabbitMqTransport({
      failOnConnectError: false,
      reconnect: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
      logger: createLogger(),
      connect: async () => {
        if (!reachable) {
          throw new Error('ECONNREFUSED');
        }
        return connection;
      },
    });
    await transport.connect();
    expect(transport.isConnected()).toBe(false);
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    reachable = true;
    await waitFor(() => transport.isConnected());
    await transport.publish(createEnvelope(), 'orders');
    await transport.close();
  });

  it('fails startup when the exchange cannot be declared and closes the connection', async () => {
    const connection = new FakeConnection();
    const createConfirmChannel = connection.createConfirmChannel.getMockImplementation()!;
    connection.createConfirmChannel.mockImplementation(async () => {
      const channel = (await createConfirmChannel()) as FakeChannel;
      channel.assertExchange.mockRejectedValue(new Error('PRECONDITION_FAILED'));
      return channel;
    });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await expect(transport.connect()).rejects.toThrow('PRECONDITION_FAILED');
    expect(connection.close).toHaveBeenCalledTimes(1);
    expect(transport.isConnected()).toBe(false);
  });
});

describe('RabbitMqTransport readiness', () => {
  it('is ready only while connected, with an open publisher channel and an active consumer', async () => {
    const { transport, connections } = createTransport();
    expect(transport.isReady()).toBe(false);
    await transport.connect();
    expect(transport.isReady()).toBe(true);
    await transport.subscribe(['orders'], async () => undefined);
    expect(transport.isReady()).toBe(true);

    connections[0]!.consumerChannel.deliver!(null);
    expect(transport.isReady()).toBe(false);
    await waitFor(() => connections.length === 2 && transport.isReady());

    connections[1]!.emit('close');
    expect(transport.isReady()).toBe(false);
    await waitFor(() => connections.length === 3 && transport.isReady());
    await transport.close();
    expect(transport.isReady()).toBe(false);
  });

  it('is not ready as soon as the publisher channel closes', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    connections[0]!.publisher.emit('close');
    expect(transport.isReady()).toBe(false);
    await waitFor(() => transport.isReady());
    await transport.close();
  });

  it('does not report a connection whose publisher channel closed while it was being set up', async () => {
    const connections: FakeConnection[] = [];
    const logger = createLogger();
    const transport = new RabbitMqTransport({
      logger,
      reconnect: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
      connect: async () => {
        const connection = new FakeConnection();
        connections.push(connection);
        if (connections.length === 1) {
          const createConfirmChannel = connection.createConfirmChannel.getMockImplementation()!;
          connection.createConfirmChannel.mockImplementation(async () => {
            const channel = (await createConfirmChannel()) as FakeChannel;
            channel.assertExchange.mockImplementation(async () => {
              // The broker closes the channel after the declare, before the transport is live.
              channel.emit('close');
              return {};
            });
            return channel;
          });
        }
        return connection;
      },
    });
    await expect(transport.connect()).rejects.toThrow(
      /Publisher channel closed while the transport was being set up/,
    );
    expect(transport.isConnected()).toBe(false);
    expect(transport.isReady()).toBe(false);
    expect(connections[0]!.close).toHaveBeenCalled();
    await transport.close();
  });

  it('reconnects when the publisher channel closed during a background reconnect', async () => {
    const connections: FakeConnection[] = [];
    const transport = new RabbitMqTransport({
      logger: createLogger(),
      failOnConnectError: false,
      reconnect: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
      connect: async () => {
        const connection = new FakeConnection();
        connections.push(connection);
        if (connections.length === 1) {
          const createConfirmChannel = connection.createConfirmChannel.getMockImplementation()!;
          connection.createConfirmChannel.mockImplementation(async () => {
            const channel = (await createConfirmChannel()) as FakeChannel;
            channel.assertExchange.mockImplementation(async () => {
              channel.emit('close');
              return {};
            });
            return channel;
          });
        }
        return connection;
      },
    });
    await transport.connect();
    await waitFor(() => transport.isReady());
    expect(connections).toHaveLength(2);
    await transport.close();
  });
});
