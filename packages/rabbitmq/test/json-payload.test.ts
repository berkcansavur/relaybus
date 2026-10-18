import { describe, expect, it } from 'vitest';
import { NonRetryableError } from '@relaybus/core';
import { UnserializablePayloadError } from '../src/errors.js';
import { assertJsonPayload } from '../src/json-payload.js';

describe('assertJsonPayload', () => {
  it.each([
    ['null', null],
    ['a string', 'text'],
    ['a boolean', false],
    ['a number', -1.5],
    ['an array', [1, 'two', [null], { three: true }]],
    ['a nested plain object', { a: { b: [1, 2], c: null } }],
    ['an object without prototype', Object.assign(Object.create(null) as object, { a: 1 })],
    [
      'a repeated, non-circular reference',
      (() => {
        const shared = { a: 1 };
        return { first: shared, second: shared };
      })(),
    ],
  ])('accepts %s', (_label, payload) => {
    expect(() => assertJsonPayload(payload, 'event-1')).not.toThrow();
  });

  const circular: Record<string, unknown> = {};
  circular['self'] = circular;
  const sparse = [1, , 3]; // eslint-disable-line no-sparse-arrays

  it.each([
    ['undefined', undefined, '$ is undefined'],
    ['a function', () => 1, '$ is function'],
    ['a symbol', Symbol('s'), '$ is symbol'],
    ['a BigInt', 10n, '$ is bigint'],
    ['NaN', { value: Number.NaN }, '$.value is NaN'],
    ['Infinity', [Number.POSITIVE_INFINITY], '$[0] is Infinity'],
    ['an undefined property', { a: { b: undefined } }, '$.a.b is undefined'],
    ['a function property', { callback: () => 1 }, '$.callback is function'],
    ['a Date', { at: new Date() }, '$.at is not a plain object'],
    ['a Map', new Map(), '$ is not a plain object'],
    ['a class instance', new (class Money {})(), '$ is not a plain object'],
    ['a typed array', { bytes: new Uint8Array(2) }, '$.bytes is not a plain object'],
    ['an array hole', sparse, '$[1] is an array hole'],
    ['a symbol key', { [Symbol('hidden')]: 1 }, '$ has a symbol key'],
    ['a cycle', circular, '$.self is a circular reference'],
  ])('refuses %s, naming where', (_label, payload, where) => {
    const error = (() => {
      try {
        assertJsonPayload(payload, 'event-1');
      } catch (caught) {
        return caught as Error;
      }
      return undefined;
    })();
    expect(error).toBeInstanceOf(UnserializablePayloadError);
    expect(error).toBeInstanceOf(NonRetryableError);
    expect(error!.message).toContain('Event event-1 has a payload JSON cannot represent');
    expect(error!.message).toContain(where);
  });
});
