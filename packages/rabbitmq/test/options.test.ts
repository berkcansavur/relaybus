import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import {
  resolveQueueName,
  resolveSettings,
  type RabbitMqTransportOptions,
} from '../src/options.js';
import { FakeConnection } from './fakes.js';

const connect = async () => new FakeConnection();

describe('options', () => {
  it('applies defaults', async () => {
    const settings = resolveSettings({ connect });
    expect(settings).toMatchObject({
      name: 'rabbitmq',
      exchange: 'relaybus',
      prefetch: 50,
      queuePrefix: 'relaybus',
      deadLetterExchange: 'relaybus.dead',
      deliveryLimit: 10,
      failOnConnectError: true,
      allowUnroutable: false,
      publishTimeoutMs: 30_000,
      shutdownTimeoutMs: 30_000,
      logger: console,
      random: Math.random,
      handlerRetry: { attempts: 3, initialDelayMs: 200, maxDelayMs: 5_000, jitter: 'equal' },
      reconnect: { initialDelayMs: 500, maxDelayMs: 30_000, factor: 2, jitter: 'equal' },
    });
    expect(await settings.connect()).toBeInstanceOf(FakeConnection);
  });

  it('accepts every option', () => {
    const logger = { log: () => undefined, warn: () => undefined, error: () => undefined };
    const random = () => 0.5;
    const settings = resolveSettings({
      name: 'broker',
      connect,
      exchange: 'events',
      prefetch: 5,
      queuePrefix: 'svc',
      consumerGroup: 'billing',
      queue: 'explicit',
      deadLetterExchange: 'dlx',
      deadLetterQueue: 'dlq',
      deliveryLimit: 3,
      handlerRetry: { attempts: 2, initialDelay: '1s', maxDelay: '2s' },
      reconnect: { initialDelay: '1s', maxDelay: '4s', factor: 3, jitter: 'full' },
      failOnConnectError: false,
      allowUnroutable: true,
      publishTimeout: '5s',
      shutdownTimeout: 0,
      logger,
      random,
    });
    expect(settings).toMatchObject({
      name: 'broker',
      exchange: 'events',
      prefetch: 5,
      queuePrefix: 'svc',
      consumerGroup: 'billing',
      queue: 'explicit',
      deadLetterExchange: 'dlx',
      deadLetterQueue: 'dlq',
      deliveryLimit: 3,
      failOnConnectError: false,
      allowUnroutable: true,
      publishTimeoutMs: 5_000,
      shutdownTimeoutMs: 0,
      handlerRetry: { attempts: 2, initialDelayMs: 1_000, maxDelayMs: 2_000 },
      reconnect: { initialDelayMs: 1_000, maxDelayMs: 4_000, factor: 3, jitter: 'full' },
      logger,
      random,
    });
  });

  it('allows disabling reconnection when startup must succeed', () => {
    expect(resolveSettings({ connect, reconnect: false }).reconnect).toBeUndefined();
  });

  it.each([
    ['neither url nor connect', {}, /exactly one/],
    ['both url and connect', { url: 'amqp://x', connect }, /exactly one/],
    ['an empty exchange', { connect, exchange: '' }, /exchange must not be empty/],
    ['an empty name', { connect, name: '' }, /name must not be empty/],
    ['an empty queue prefix', { connect, queuePrefix: '' }, /queuePrefix/],
    ['an empty consumer group', { connect, consumerGroup: '' }, /consumerGroup/],
    ['a fractional prefetch', { connect, prefetch: 1.5 }, /prefetch must be a positive integer/],
    ['a zero delivery limit', { connect, deliveryLimit: 0 }, /deliveryLimit/],
    ['a zero publish timeout', { connect, publishTimeout: 0 }, /publishTimeout must be greater/],
    [
      'a malformed shutdown timeout',
      { connect, shutdownTimeout: 'soon' as '1s' },
      /shutdownTimeout/,
    ],
    [
      'background connect without reconnect',
      { connect, failOnConnectError: false, reconnect: false as const },
      /needs reconnect/,
    ],
    ['an invalid handler retry', { connect, handlerRetry: { attempts: 0 } }, /retry.attempts/],
    ['an invalid reconnect backoff', { connect, reconnect: { factor: 0 } }, /retry.factor/],
  ] satisfies [string, RabbitMqTransportOptions, RegExp][])(
    'rejects %s',
    (_label, options, pattern) => {
      expect(() => resolveSettings(options)).toThrow(ConfigurationError);
      expect(() => resolveSettings(options)).toThrow(pattern);
    },
  );

  it('derives the queue name from the prefix and group, or uses the explicit queue', () => {
    expect(resolveQueueName(resolveSettings({ connect, consumerGroup: 'billing' }))).toBe(
      'relaybus.billing',
    );
    expect(
      resolveQueueName(resolveSettings({ connect, queuePrefix: 'a', consumerGroup: 'b' })),
    ).toBe('a.b');
    expect(resolveQueueName(resolveSettings({ connect, queue: 'mine', consumerGroup: 'b' }))).toBe(
      'mine',
    );
    expect(() => resolveQueueName(resolveSettings({ connect }))).toThrow(/consumerGroup/);
  });
});
