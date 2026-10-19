import { describe, expect, it } from 'vitest';
import { createUuidV7 } from '@relaybus/core';
import { LANE_COUNT, formatLeaseToken, laneOf, parseLeaseToken } from '../src/lanes.js';

describe('laneOf', () => {
  it.each([
    // Low byte of the published 32-bit FNV-1a test vectors: '' 0x811c9dc5, 'a' 0xe40c292c, 'foobar' 0xbf9cf968.
    ['', 0xc5],
    ['a', 0x2c],
    ['foobar', 0x68],
    ['order-42', 244],
    // Hashed as UTF-8 bytes, not UTF-16 code units.
    ['çğıöşü 🚀', 82],
  ])('hashes the key %j to a stable lane', (key, lane) => {
    expect(laneOf(key, 'ignored-event-id')).toBe(lane);
  });

  it('uses the event id when the event has no key', () => {
    expect(laneOf(null, 'foobar')).toBe(0x68);
    expect(laneOf(null, 'foobar')).toBe(laneOf('foobar', 'other'));
  });

  it('puts every delivery of a key in the same lane, whatever its event id', () => {
    const lanes = new Set(Array.from({ length: 50 }, () => laneOf('customer-7', createUuidV7())));
    expect([...lanes]).toEqual([laneOf('customer-7', 'x')]);
  });

  it('stays inside 0..255 and spreads event ids over many lanes', () => {
    const lanes = Array.from({ length: 4_000 }, () => laneOf(null, createUuidV7()));
    expect(lanes.every((lane) => Number.isInteger(lane) && lane >= 0 && lane < LANE_COUNT)).toBe(
      true,
    );
    expect(new Set(lanes).size).toBeGreaterThan(240);
    expect(LANE_COUNT).toBe(256);
  });
});

describe('lease tokens', () => {
  it('round-trips the lane, the epoch and the claim generation', () => {
    expect(formatLeaseToken(7, 12, 3)).toBe('7:12:3');
    expect(parseLeaseToken('7:12:3')).toEqual({ laneId: 7, epoch: 12, claims: 3 });
    expect(parseLeaseToken('255:0:0')).toEqual({ laneId: 255, epoch: 0, claims: 0 });
    expect(parseLeaseToken(formatLeaseToken(0, 999_999_999_999_999, 999_999_999_999_999))).toEqual({
      laneId: 0,
      epoch: 999_999_999_999_999,
      claims: 999_999_999_999_999,
    });
  });

  it.each([
    '',
    'not-a-token',
    '256:1',
    '999:1',
    '3:x',
    '3:',
    ':3',
    '-1:2',
    '3:-2',
    '3:1.5',
    ' 3:1',
    '3:1 ',
    '7:1234567890123456',
    '018f2b6e-7a5c-7c3e-9a1b-1234567890ab',
    // Two-part tokens of the previous format name no claim generation.
    '3:1',
    '256:1:0',
    '999:1:0',
    '3:x:0',
    '3:1:x',
    '3:1:',
    '3::1',
    '3:1:-1',
    '3:1:1.5',
    '1:2:3:4',
    ' 3:1:0',
    '3:1:0 ',
    '7:1:1234567890123456',
  ])('refuses %j, which this store cannot have issued', (token) => {
    expect(parseLeaseToken(token)).toBeUndefined();
  });
});
