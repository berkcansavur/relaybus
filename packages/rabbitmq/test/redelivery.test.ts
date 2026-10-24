import { describe, expect, it } from 'vitest';
import { ConfigurationError, createRetryPolicy } from '@relaybus/core';
import {
  FIRST_FAILURE_HEADER,
  REDELIVERIES_HEADER,
  buildRetryLadder,
  chooseRetryTier,
  ORIGINAL_ROUTING_KEY_HEADER,
  copyForRepublish,
  originalRoutingKey,
  readCounterHeader,
  withOriginalRoutingKey,
} from '../src/redelivery.js';
import { createDelivery } from './fakes.js';

const policy = (options: Parameters<typeof createRetryPolicy>[0]) =>
  createRetryPolicy({ jitter: 'none', ...options });

describe('buildRetryLadder', () => {
  it('grows the initial delay by the factor until the cap', () => {
    expect(buildRetryLadder(policy({ initialDelay: 2_000, maxDelay: 300_000 }))).toEqual([
      2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 300_000,
    ]);
    expect(buildRetryLadder(policy({ initialDelay: 100, factor: 1.5, maxDelay: 300 }))).toEqual([
      100, 150, 225, 300,
    ]);
  });

  it('has one tier when the delay is constant or already at the cap', () => {
    expect(buildRetryLadder(policy({ initialDelay: 500, factor: 1, maxDelay: 5_000 }))).toEqual([
      500,
    ]);
    expect(buildRetryLadder(policy({ initialDelay: 5_000, maxDelay: 5_000 }))).toEqual([5_000]);
  });

  it('rejects a zero delay and a ladder with too many retry queues', () => {
    expect(() => buildRetryLadder(policy({ initialDelay: 0, maxDelay: 10 }))).toThrow(
      ConfigurationError,
    );
    expect(() =>
      buildRetryLadder(policy({ initialDelay: 1, factor: 1.1, maxDelay: '1h' })),
    ).toThrow(/more than 16 retry queues/);
  });
});

describe('chooseRetryTier', () => {
  const ladder = [100, 200, 400];

  it('uses the nearest tier at the ends and exact matches', () => {
    expect(chooseRetryTier(ladder, { delayMs: 0, random: () => 0.5 })).toBe(100);
    expect(chooseRetryTier(ladder, { delayMs: 100, random: () => 0.5 })).toBe(100);
    expect(chooseRetryTier(ladder, { delayMs: 400, random: () => 0.5 })).toBe(400);
    expect(chooseRetryTier(ladder, { delayMs: 9_999, random: () => 0.5 })).toBe(400);
  });

  it('rounds a delay between two tiers up or down so the expected delay is preserved', () => {
    // 150 is halfway between 100 and 200.
    expect(chooseRetryTier(ladder, { delayMs: 150, random: () => 0.49 })).toBe(200);
    expect(chooseRetryTier(ladder, { delayMs: 150, random: () => 0.5 })).toBe(100);
    expect(chooseRetryTier(ladder, { delayMs: 300, random: () => 0.1 })).toBe(400);
    expect(chooseRetryTier(ladder, { delayMs: 300, random: () => 0.9 })).toBe(200);
  });
});

describe('readCounterHeader', () => {
  const read = (value: unknown) =>
    readCounterHeader(
      {
        ...createDelivery(),
        properties: { headers: { [REDELIVERIES_HEADER]: value } },
      },
      REDELIVERIES_HEADER,
    );

  it('reads non-negative integers given as numbers or numeric strings', () => {
    expect(read(3)).toBe(3);
    expect(read('1700000000000')).toBe(1_700_000_000_000);
    expect(read(0)).toBe(0);
  });

  it.each([undefined, -1, 1.5, 'many', '', null, {}, Number.NaN])('ignores %s', (value) => {
    expect(read(value)).toBeUndefined();
  });
});

describe('copyForRepublish', () => {
  it('copies body and properties, drops broker counters and stale state, applies changes', () => {
    const original = createDelivery();
    const message = {
      ...original,
      properties: {
        ...original.properties,
        correlationId: 'flow-1',
        contentType: 'application/json; charset=utf-8',
        headers: {
          ...original.properties.headers,
          'x-death': [{ count: 1 }],
          'x-delivery-count': 2,
          [REDELIVERIES_HEADER]: '3',
          [FIRST_FAILURE_HEADER]: '5',
        },
      },
    };
    const copy = copyForRepublish(message, { [REDELIVERIES_HEADER]: '4' });
    expect(copy.content).toBe(message.content);
    expect(copy.options).toMatchObject({
      contentType: 'application/json; charset=utf-8',
      persistent: true,
      messageId: message.properties.messageId,
      correlationId: 'flow-1',
      timestamp: message.properties.timestamp,
    });
    expect(copy.options.headers).toMatchObject({
      'x-death': [{ count: 1 }],
      [REDELIVERIES_HEADER]: '4',
    });
    expect(copy.options.headers).not.toHaveProperty('x-delivery-count');
    expect(copy.options.headers).not.toHaveProperty(FIRST_FAILURE_HEADER);
  });
});

describe('original routing key', () => {
  it('reads no counter from a message without headers', () => {
    expect(
      readCounterHeader({ ...createDelivery(), properties: {} }, REDELIVERIES_HEADER),
    ).toBeUndefined();
  });

  it('restores the routing key a retry copy was first published with', () => {
    const delivery = createDelivery();
    expect(originalRoutingKey(delivery)).toBe('order.placed.v1');
    expect(withOriginalRoutingKey(delivery)).toBe(delivery);
    const copy = {
      ...delivery,
      fields: { routingKey: 'relaybus.billing' },
      properties: { headers: { [ORIGINAL_ROUTING_KEY_HEADER]: 'orders.placed' } },
    };
    expect(originalRoutingKey(copy)).toBe('orders.placed');
    expect(withOriginalRoutingKey(copy)).toEqual({
      ...copy,
      fields: { routingKey: 'orders.placed' },
    });
    expect(originalRoutingKey({ ...delivery, properties: {} })).toBe('order.placed.v1');
  });
});
