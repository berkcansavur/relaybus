import { createServer, type Socket } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import type { AmqpMessage } from '../src/amqp-types.js';
import {
  AmqpLimitExceededError,
  RabbitMqPublishRejectedError,
  RabbitMqUnavailableError,
  UnroutableMessageError,
  UnserializablePayloadError,
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

  it('rejects, without retrying, an event whose payload cannot be serialized', async () => {
    const { transport, publisher } = await connected();
    await expect(transport.publish(createEnvelope({ payload: 1n }), 'orders')).rejects.toThrow(
      UnserializablePayloadError,
    );
    await expect(
      transport.publish(createEnvelope({ payload: undefined }), 'orders'),
    ).rejects.toThrow(UnserializablePayloadError);
    expect(publisher.published).toHaveLength(0);
    await transport.close();
  });

  it('rejects, without retrying, a destination or id beyond the AMQP short string limit', async () => {
    const { transport, publisher } = await connected();
    await expect(transport.publish(createEnvelope(), 'k'.repeat(256))).rejects.toThrow(
      AmqpLimitExceededError,
    );
    await expect(
      transport.publish(createEnvelope({ id: 'i'.repeat(256) }), 'orders'),
    ).rejects.toThrow(AmqpLimitExceededError);
    expect(publisher.published).toHaveLength(0);
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
  it('declares quorum queues: main with at-least-once dead-lettering, one retry queue per delay tier, dead-letter queue', async () => {
    const { transport, connections } = createTransport({
      exchange: 'events',
      deliveryLimit: 4,
      prefetch: 9,
      redelivery: { initialDelay: '1s', maxDelay: '4s', jitter: 'none' },
    });
    await transport.connect();
    await transport.subscribe(['order.*', 'invoice.paid.v1'], async () => undefined);
    const channel = connections[0]!.consumerChannel;
    expect(channel.assertExchange).toHaveBeenCalledWith('events', 'topic', { durable: true });
    expect(channel.assertExchange).toHaveBeenCalledWith('events.dead', 'direct', { durable: true });
    expect(channel.assertQueue.mock.calls).toEqual([
      ['relaybus.billing.dead', { durable: true, arguments: { 'x-queue-type': 'quorum' } }],
      ...[1_000, 2_000, 4_000].map((delay) => [
        `relaybus.billing.retry.${delay}`,
        {
          durable: true,
          arguments: {
            'x-queue-type': 'quorum',
            'x-message-ttl': delay,
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': 'relaybus.billing',
            'x-dead-letter-strategy': 'at-least-once',
            'x-overflow': 'reject-publish',
          },
        },
      ]),
      [
        'relaybus.billing',
        {
          durable: true,
          arguments: {
            'x-queue-type': 'quorum',
            'x-delivery-limit': 4,
            'x-dead-letter-exchange': 'events.dead',
            'x-dead-letter-routing-key': 'relaybus.billing',
            'x-dead-letter-strategy': 'at-least-once',
            'x-overflow': 'reject-publish',
          },
        },
      ],
    ]);
    expect(channel.bindQueue.mock.calls).toEqual([
      ['relaybus.billing.dead', 'events.dead', 'relaybus.billing'],
      ['relaybus.billing', 'events', 'order.*'],
      ['relaybus.billing', 'events', 'invoice.paid.v1'],
    ]);
    expect(channel.prefetch).toHaveBeenCalledWith(9);
    await transport.close();
  });

  it('ordered consumption uses one single-active consumer queue, prefetch 1 and no retry queues', async () => {
    const { transport, connections } = createTransport({ ordered: true });
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    const channel = connections[0]!.consumerChannel;
    const declared = Object.fromEntries(
      channel.assertQueue.mock.calls.map(([name, options]) => [name, options]),
    );
    expect(Object.keys(declared)).toEqual(['relaybus.billing.dead', 'relaybus.billing']);
    expect(declared['relaybus.billing']).toMatchObject({
      arguments: { 'x-single-active-consumer': true, 'x-overflow': 'reject-publish' },
    });
    expect(channel.prefetch).toHaveBeenCalledWith(1);
    await transport.close();
  });

  it('honours explicit queue and dead-letter names', async () => {
    const { transport, connections } = createTransport({
      queue: 'billing-events',
      deadLetterExchange: 'dlx',
      deadLetterQueue: 'billing-graveyard',
      redelivery: { initialDelay: '5s', maxDelay: '5s' },
    });
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    const names = connections[0]!.consumerChannel.assertQueue.mock.calls.map((call) => call[0]);
    expect(names).toEqual(['billing-graveyard', 'billing-events.retry.5000', 'billing-events']);
    await transport.close();
  });

  it('parks failing messages in a retry queue through the publisher channel, mandatory even when unroutable publishes are allowed', async () => {
    const { transport, connections } = createTransport({
      allowUnroutable: true,
      handlerRetry: { attempts: 1 },
      redelivery: { initialDelay: '1s', maxDelay: '1s' },
    });
    await transport.connect();
    await transport.subscribe(['orders'], async () => {
      throw new Error('database down');
    });
    const { consumerChannel, publisher } = connections[0]!;
    consumerChannel.deliver!(createDelivery());
    await waitFor(() => consumerChannel.ack.mock.calls.length === 1);
    expect(publisher.published).toHaveLength(1);
    expect(publisher.published[0]!.routingKey).toBe('relaybus.billing.retry.1000');
    expect(publisher.published[0]!.options.mandatory).toBe(true);
    await transport.close();
  });

  it('never acknowledges a message whose retry copy came back unroutable', async () => {
    const { transport, connections } = createTransport({
      handlerRetry: { attempts: 1 },
      redelivery: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
    });
    await transport.connect();
    await transport.subscribe(['orders'], async () => {
      throw new Error('database down');
    });
    const { consumerChannel, publisher } = connections[0]!;
    publisher.publishBehaviour = (callback, routingKey) => {
      publisher.emit('return', returned(createEnvelope().id, routingKey));
      callback(null);
    };
    consumerChannel.deliver!(createDelivery());
    await waitFor(() => consumerChannel.nack.mock.calls.length === 1);
    expect(consumerChannel.nack).toHaveBeenCalledWith(expect.anything(), false, true);
    expect(consumerChannel.ack).not.toHaveBeenCalled();
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

describe('RabbitMqTransport forced close', () => {
  /** A connection whose closing handshake never completes, with the socket amqplib keeps under it. */
  function hangingConnection(stream?: { destroy: () => void }): FakeConnection {
    const connection = new FakeConnection();
    connection.close.mockImplementation(() => new Promise<void>(() => undefined));
    Object.assign(connection, { connection: stream === undefined ? {} : { stream } });
    return connection;
  }

  it('destroys the socket of its connection when the shutdown deadline passes', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await transport.connect();
    const deadline = new AbortController();
    void transport.close({ signal: deadline.signal });
    // The channels are closed and the closing handshake of the connection hangs.
    await waitFor(() => connection.close.mock.calls.length === 1);
    expect(destroy).not.toHaveBeenCalled();
    deadline.abort();
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('destroys the socket when the deadline passes while consumers still drain', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    const transport = new RabbitMqTransport({
      connect: async () => connection,
      consumerGroup: 'billing',
    });
    await transport.connect();
    await transport.subscribe(['orders'], () => new Promise<void>(() => undefined));
    const consumerChannel = connection.consumerChannel;
    consumerChannel.cancel.mockImplementation(() => new Promise(() => undefined));
    const deadline = new AbortController();
    void transport.close({ signal: deadline.signal });
    await waitFor(() => consumerChannel.cancel.mock.calls.length === 1);
    deadline.abort();
    expect(destroy).toHaveBeenCalledTimes(1);
    expect(connection.close).not.toHaveBeenCalled();
  });

  it('destroys it at once when the deadline passed before close was called', async () => {
    const destroy = vi.fn();
    const transport = new RabbitMqTransport({
      connect: async () => hangingConnection({ destroy }),
    });
    await transport.connect();
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('forces nothing when there is no connection or no socket to destroy', async () => {
    const withoutSocket = new RabbitMqTransport({ connect: async () => hangingConnection() });
    await withoutSocket.connect();
    void withoutSocket.close({ signal: AbortSignal.abort() });
    const plain = new RabbitMqTransport({ connect: async () => new FakeConnection() });
    await plain.connect();
    await expect(plain.close({ signal: AbortSignal.abort() })).resolves.toBeUndefined();
    const unconnected = new RabbitMqTransport({ connect: async () => new FakeConnection() });
    await expect(unconnected.close({ signal: AbortSignal.abort() })).resolves.toBeUndefined();
  });

  it('cuts a connection that opens only after a forced close', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    let open!: (opened: FakeConnection) => void;
    const transport = new RabbitMqTransport({
      connect: () => new Promise<FakeConnection>((resolve) => (open = resolve)),
    });
    const connecting = transport.connect();
    await transport.close({ signal: AbortSignal.abort() });
    open(connection);
    await expect(connecting).rejects.toThrow(RabbitMqUnavailableError);
    expect(destroy).toHaveBeenCalledTimes(1);
    expect(connection.close).not.toHaveBeenCalled();
  });

  it('closes a connection that opens only after a graceful close', async () => {
    const connection = new FakeConnection();
    let open!: (opened: FakeConnection) => void;
    const transport = new RabbitMqTransport({
      connect: () => new Promise<FakeConnection>((resolve) => (open = resolve)),
    });
    const connecting = transport.connect();
    await transport.close();
    open(connection);
    await expect(connecting).rejects.toThrow(RabbitMqUnavailableError);
    expect(connection.close).toHaveBeenCalledTimes(1);
  });

  it('cuts a discarded connection whose closing handshake hangs', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    connection.createConfirmChannel.mockRejectedValue(new Error('channel refused'));
    const transport = new RabbitMqTransport({ connect: async () => connection });
    void transport.connect().catch(() => undefined);
    await waitFor(() => connection.close.mock.calls.length === 1);
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('lets a second close cut the connection the first one is still closing', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await transport.connect();
    void transport.close();
    await waitFor(() => connection.close.mock.calls.length === 1);
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('destroys the socket with an error, so amqplib runs its own cleanup', async () => {
    const destroy = vi.fn();
    const transport = new RabbitMqTransport({
      connect: async () => hangingConnection({ destroy }),
    });
    await transport.connect();
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledWith(expect.any(Error));
  });

  it('cuts a socket still in the AMQP handshake with a broker that never answers', async () => {
    const accepted: Socket[] = [];
    // Reads and ignores whatever the client sends, so it sees the client hang up.
    const server = createServer((socket) => {
      socket.resume();
      accepted.push(socket);
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as { port: number };
    const transport = new RabbitMqTransport({ url: `amqp://127.0.0.1:${port}` });
    const connecting = transport.connect();
    await waitFor(() => accepted.length === 1);
    const serverSaw = new Promise<void>((resolve) => accepted[0]!.once('close', () => resolve()));
    const closing = transport.close({ signal: AbortSignal.abort() });
    await expect(connecting).rejects.toThrow();
    await serverSaw;
    await closing;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }, 10_000);

  it('passes the deadline signal to a connect function', async () => {
    let received: AbortSignal | undefined;
    const transport = new RabbitMqTransport({
      connect: async ({ signal }) => {
        received = signal;
        return new FakeConnection();
      },
    });
    await transport.connect();
    expect(received!.aborted).toBe(false);
    await transport.close({ signal: AbortSignal.abort() });
    expect(received!.aborted).toBe(true);
  });

  it('stops listening to the deadline once it closed', async () => {
    const destroy = vi.fn();
    const connection = new FakeConnection();
    Object.assign(connection, { connection: { stream: { destroy } } });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await transport.connect();
    const deadline = new AbortController();
    await transport.close({ signal: deadline.signal });
    deadline.abort();
    expect(destroy).not.toHaveBeenCalled();
  });
});
