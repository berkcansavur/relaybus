import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import { FakeConnection, createDelivery, createEnvelope, createLogger, waitFor } from './fakes.js';
import { createTransport, returned } from './support/transport-setup.js';

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
    // The routing key the event was published with is where it arrived.
    expect(handler).toHaveBeenCalledWith(createEnvelope(), { destination: 'order.placed.v1' });
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
