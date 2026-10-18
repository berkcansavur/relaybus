import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import type { AmqpMessage } from '../src/amqp-types.js';
import {
  RabbitMqPublishRejectedError,
  RabbitMqUnavailableError,
  UnroutableMessageError,
} from '../src/errors.js';
import type { RabbitMqTransportOptions } from '../src/options.js';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import {
  type FakeChannel,
  FakeConnection,
  createDelivery,
  createEnvelope,
  createLogger,
  waitFor,
} from './fakes.js';

function createTransport(options: Partial<RabbitMqTransportOptions> = {}) {
  const connections: FakeConnection[] = [];
  const logger = createLogger();
  const transport = new RabbitMqTransport({
    connect: async () => {
      const connection = new FakeConnection();
      connections.push(connection);
      return connection;
    },
    consumerGroup: 'billing',
    reconnect: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
    logger,
    random: () => 0.5,
    ...options,
  });
  return { transport, connections, logger };
}

const returned = (messageId: string, routingKey = 'orders'): AmqpMessage => ({
  content: Buffer.from('{}'),
  fields: { routingKey },
  properties: { messageId },
});

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

describe('RabbitMqTransport publish', () => {
  async function connected(options: Partial<RabbitMqTransportOptions> = {}) {
    const context = createTransport(options);
    await context.transport.connect();
    return { ...context, publisher: context.connections[0]!.publisher };
  }

  it('resolves after the broker confirm, publishing persistent and mandatory', async () => {
    const { transport, publisher } = await connected({ exchange: 'events' });
    const envelope = createEnvelope();
    let confirm!: (error: Error | null) => void;
    publisher.publishBehaviour = (callback) => {
      confirm = callback;
    };
    let resolved = false;
    const publishing = transport.publish(envelope, 'order.placed.v1').then(() => {
      resolved = true;
    });
    await waitFor(() => publisher.published.length === 1);
    expect(resolved).toBe(false);
    confirm(null);
    await publishing;
    expect(publisher.published[0]!.routingKey).toBe('order.placed.v1');
    expect(publisher.published[0]!.options).toMatchObject({
      persistent: true,
      mandatory: true,
      messageId: envelope.id,
    });
    await transport.close();
  });

  it('rejects while disconnected and after close, never dropping silently', async () => {
    const { transport } = createTransport();
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      /Not connected to the broker \| transport=rabbitmq destination=orders/,
    );
    await transport.connect();
    await transport.close();
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
  });

  it('rejects when the broker nacks', async () => {
    const { transport, publisher } = await connected();
    publisher.publishBehaviour = (callback) => callback(new Error('message nacked'));
    const error = await transport.publish(createEnvelope(), 'orders').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RabbitMqPublishRejectedError);
    expect((error as Error).message).toContain('Broker nacked the message');
    await transport.close();
  });

  it('rejects when the channel closes before the confirm', async () => {
    const { transport, publisher } = await connected();
    publisher.publishBehaviour = (callback) => callback(new Error('channel closed'));
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    await transport.close();
  });

  it('rejects when publishing throws synchronously', async () => {
    const { transport, publisher } = await connected();
    publisher.publishBehaviour = () => {
      throw new Error('Channel closed');
    };
    const error = await transport.publish(createEnvelope(), 'orders').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RabbitMqUnavailableError);
    expect(((error as Error).cause as Error).message).toBe('Channel closed');
    await transport.close();
  });

  it('rejects an event whose payload cannot be serialized', async () => {
    const { transport } = await connected();
    await expect(transport.publish(createEnvelope({ payload: 1n }), 'orders')).rejects.toThrow(
      TypeError,
    );
    await transport.close();
  });

  it('rejects when no confirm arrives within the timeout', async () => {
    const { transport, publisher } = await connected({ publishTimeout: 20 });
    publisher.publishBehaviour = () => undefined;
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      /No broker confirm within 20ms/,
    );
    await transport.close();
  });

  it('rejects an unroutable message even though the broker acked it', async () => {
    const { transport, publisher } = await connected();
    const envelope = createEnvelope();
    publisher.publishBehaviour = (callback) => {
      publisher.emit('return', returned(envelope.id));
      callback(null);
    };
    await expect(transport.publish(envelope, 'orders')).rejects.toThrow(UnroutableMessageError);
    await transport.close();
  });

  it('matches returns to publishes by event id and destination', async () => {
    const { transport, publisher } = await connected();
    const envelope = createEnvelope();
    let returnFor: string | undefined;
    publisher.publishBehaviour = (callback, routingKey) => {
      if (routingKey === returnFor) {
        publisher.emit('return', returned(envelope.id, routingKey));
        publisher.emit('return', returned(envelope.id, routingKey));
      }
      callback(null);
    };
    returnFor = 'unbound';
    await expect(transport.publish(envelope, 'unbound')).rejects.toThrow(UnroutableMessageError);
    await expect(transport.publish(envelope, 'unbound')).rejects.toThrow(UnroutableMessageError);
    returnFor = undefined;
    await transport.publish(envelope, 'orders');
    await transport.close();
  });

  it('lets unroutable messages through when allowed, without the mandatory flag', async () => {
    const { transport, publisher } = await connected({ allowUnroutable: true });
    await transport.publish(createEnvelope(), 'orders');
    expect(publisher.published[0]!.options.mandatory).toBe(false);
    await transport.close();
  });

  it('logs publisher channel errors', async () => {
    const { transport, publisher, logger } = await connected();
    publisher.emit('error', new Error('NOT_FOUND'));
    expect(logger.warn).toHaveBeenCalledWith(
      '[RabbitMqTransport](establishChannels) Publisher channel error | transport=rabbitmq error=NOT_FOUND',
    );
    await transport.close();
  });
});

describe('RabbitMqTransport subscribe', () => {
  it('declares a quorum queue with a delivery limit and dead letter queue, bound per destination', async () => {
    const { transport, connections } = createTransport({
      exchange: 'events',
      deliveryLimit: 4,
      prefetch: 9,
    });
    await transport.connect();
    await transport.subscribe(['order.*', 'invoice.paid.v1'], async () => undefined);
    const channel = connections[0]!.consumerChannel;
    expect(channel.assertExchange).toHaveBeenCalledWith('events', 'topic', { durable: true });
    expect(channel.assertExchange).toHaveBeenCalledWith('events.dead', 'direct', { durable: true });
    expect(channel.assertQueue).toHaveBeenCalledWith('relaybus.billing.dead', {
      durable: true,
      arguments: { 'x-queue-type': 'quorum' },
    });
    expect(channel.assertQueue).toHaveBeenCalledWith('relaybus.billing', {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': 4,
        'x-dead-letter-exchange': 'events.dead',
        'x-dead-letter-routing-key': 'relaybus.billing',
      },
    });
    expect(channel.bindQueue.mock.calls).toEqual([
      ['relaybus.billing.dead', 'events.dead', 'relaybus.billing'],
      ['relaybus.billing', 'events', 'order.*'],
      ['relaybus.billing', 'events', 'invoice.paid.v1'],
    ]);
    expect(channel.prefetch).toHaveBeenCalledWith(9);
    await transport.close();
  });

  it('honours explicit queue and dead-letter names', async () => {
    const { transport, connections } = createTransport({
      queue: 'billing-events',
      deadLetterExchange: 'dlx',
      deadLetterQueue: 'billing-graveyard',
    });
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    const names = connections[0]!.consumerChannel.assertQueue.mock.calls.map((call) => call[0]);
    expect(names).toEqual(['billing-graveyard', 'billing-events']);
    await transport.close();
  });

  it('hands decoded events to the handler', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    const handler = vi.fn(async () => undefined);
    await transport.subscribe(['orders'], handler);
    connections[0]!.consumerChannel.deliver!(createDelivery());
    await waitFor(() => handler.mock.calls.length === 1);
    expect(handler).toHaveBeenCalledWith(createEnvelope());
    await transport.close();
  });

  it('registers the consumer when the connection comes up later', async () => {
    let reachable = false;
    const connections: FakeConnection[] = [];
    const transport = new RabbitMqTransport({
      consumerGroup: 'billing',
      failOnConnectError: false,
      reconnect: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
      logger: createLogger(),
      connect: async () => {
        if (!reachable) {
          throw new Error('ECONNREFUSED');
        }
        const connection = new FakeConnection();
        connections.push(connection);
        return connection;
      },
    });
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    expect(connections).toHaveLength(0);
    reachable = true;
    await waitFor(() => transport.isConnected());
    expect(connections[0]!.consumerChannel.consume).toHaveBeenCalledTimes(1);
    await transport.close();
  });

  it('rejects a second subscription, no destinations, and a missing consumer group', async () => {
    const { transport } = createTransport();
    await transport.connect();
    await expect(transport.subscribe([], async () => undefined)).rejects.toThrow(
      /at least one destination/,
    );
    await transport.subscribe(['orders'], async () => undefined);
    await expect(transport.subscribe(['other'], async () => undefined)).rejects.toThrow(
      ConfigurationError,
    );
    await transport.close();

    const ungrouped = new RabbitMqTransport({ connect: async () => new FakeConnection() });
    await expect(ungrouped.subscribe(['orders'], async () => undefined)).rejects.toThrow(
      /consumerGroup/,
    );
  });
});

describe('RabbitMqTransport recovery', () => {
  it('rebuilds channels, topology and the consumer after the connection drops', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    const handler = vi.fn(async () => undefined);
    await transport.subscribe(['orders'], handler);
    connections[0]!.emit('close', new Error('CONNECTION_FORCED'));
    expect(transport.isConnected()).toBe(false);
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    await waitFor(() => transport.isConnected());
    expect(connections).toHaveLength(2);
    expect(connections[1]!.consumerChannel.consume).toHaveBeenCalledTimes(1);
    await transport.publish(createEnvelope(), 'orders');
    connections[1]!.consumerChannel.deliver!(createDelivery());
    await waitFor(() => handler.mock.calls.length === 1);
    await transport.close();
  });

  it('reconnects when the publisher channel closes on its own', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    connections[0]!.publisher.emit('close');
    expect(transport.isConnected()).toBe(false);
    await waitFor(() => transport.isConnected());
    expect(connections[0]!.close).toHaveBeenCalled();
    expect(connections).toHaveLength(2);
    await transport.close();
  });

  it('reconnects when the consumer channel closes or the consumer is cancelled', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    connections[0]!.consumerChannel.emit('close');
    await waitFor(() => connections.length === 2 && transport.isConnected());
    connections[1]!.consumerChannel.deliver!(null);
    await waitFor(() => connections.length === 3 && transport.isConnected());
    await transport.close();
  });

  it('retries when restoring the topology fails', async () => {
    const { transport, connections, logger } = createTransport();
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    const original = connections.push.bind(connections);
    connections.push = (...added: FakeConnection[]) => {
      if (connections.length === 1) {
        added[0]!.createChannel.mockRejectedValueOnce(new Error('channel limit'));
      }
      return original(...added);
    };
    connections[0]!.emit('close');
    await waitFor(() => connections.length === 3 && transport.isConnected());
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Attempt failed | attempt=1'));
    await transport.close();
  });
});

describe('RabbitMqTransport close', () => {
  it('closes channels and connection once, and may be called again', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    await transport.close();
    await transport.close();
    expect(connections[0]!.publisher.close).toHaveBeenCalledTimes(1);
    expect(connections[0]!.consumerChannel.close).toHaveBeenCalledTimes(1);
    expect(connections[0]!.close).toHaveBeenCalledTimes(1);
  });

  it('closes without ever having connected', async () => {
    const { transport } = createTransport();
    await transport.close();
    await expect(transport.connect()).rejects.toThrow(RabbitMqUnavailableError);
  });

  it('survives a publisher channel that fails to close', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    connections[0]!.publisher.close.mockRejectedValue(new Error('boom'));
    await transport.close();
  });
});
