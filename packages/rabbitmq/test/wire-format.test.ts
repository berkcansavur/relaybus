import { describe, expect, it } from 'vitest';
import { NonRetryableError } from '@relaybus/core';
import type { AmqpMessage } from '../src/amqp-types.js';
import {
  AmqpLimitExceededError,
  MalformedMessageError,
  UnserializablePayloadError,
} from '../src/errors.js';
import { assertShortString, decodeEnvelope, encodeEnvelope } from '../src/wire-format.js';
import { createEnvelope } from './fakes.js';

function asDelivery(encoded: ReturnType<typeof encodeEnvelope>): AmqpMessage {
  return {
    content: encoded.content,
    fields: { routingKey: 'x' },
    properties: { ...encoded.options },
  };
}

function withHeaders(
  headers: Record<string, unknown>,
  overrides: Partial<AmqpMessage['properties']> = {},
  content = Buffer.from('{"a":1}'),
): AmqpMessage {
  return {
    content,
    fields: { routingKey: 'x' },
    properties: { headers: { 'cloudEvents:specversion': '1.0', ...headers }, ...overrides },
  };
}

describe('wire format', () => {
  it('maps the envelope onto CloudEvents headers and AMQP properties', () => {
    const { content, options } = encodeEnvelope(createEnvelope());
    expect(JSON.parse(content.toString())).toEqual({ orderId: '1' });
    expect(options).toEqual({
      contentType: 'application/json',
      persistent: true,
      messageId: '0190a000-0000-7000-8000-000000000001',
      type: 'order.placed.v1',
      appId: 'orders-service',
      timestamp: 1_767_262_830,
      correlationId: 'flow-1',
      headers: {
        'cloudEvents:specversion': '1.0',
        'cloudEvents:id': '0190a000-0000-7000-8000-000000000001',
        'cloudEvents:type': 'order.placed.v1',
        'cloudEvents:source': 'orders-service',
        'cloudEvents:time': '2026-01-01T10:20:30.456Z',
        'cloudEvents:datacontenttype': 'application/json',
        'cloudEvents:subject': 'order-1',
        'cloudEvents:causationid': 'cause-1',
        'cloudEvents:correlationid': 'flow-1',
        'relaybus:traceparent': '00-abc-def-01',
      },
    });
  });

  it('round-trips every envelope field exactly, including milliseconds', () => {
    const envelope = createEnvelope({ payload: { nested: [1, 'two', null], flag: true } });
    expect(decodeEnvelope(asDelivery(encodeEnvelope(envelope)))).toEqual(envelope);
  });

  it('round-trips an envelope without key, ids and headers', () => {
    const envelope = createEnvelope({
      key: null,
      correlationId: null,
      causationId: null,
      headers: {},
    });
    const encoded = encodeEnvelope(envelope);
    expect(encoded.options).not.toHaveProperty('correlationId');
    expect(Object.keys(encoded.options.headers)).not.toContain('cloudEvents:subject');
    expect(decodeEnvelope(asDelivery(encoded))).toEqual(envelope);
  });

  it('round-trips empty strings and header names that look like attributes', () => {
    const envelope = createEnvelope({
      key: '',
      correlationId: '',
      headers: { 'cloudEvents:id': 'user-value', 'x-death': 'mine', '': 'empty-name' },
    });
    expect(decodeEnvelope(asDelivery(encodeEnvelope(envelope)))).toEqual(envelope);
  });

  it('round-trips a null payload and refuses an undefined one', () => {
    expect(
      decodeEnvelope(asDelivery(encodeEnvelope(createEnvelope({ payload: null })))).payload,
    ).toBeNull();
    expect(() => encodeEnvelope(createEnvelope({ payload: undefined }))).toThrow(
      UnserializablePayloadError,
    );
  });

  it('refuses a payload that cannot be serialized, with a non-retryable error', () => {
    const error = (() => {
      try {
        encodeEnvelope(createEnvelope({ payload: { amount: 10n } }));
      } catch (caught) {
        return caught as Error;
      }
      return undefined;
    })();
    expect(error).toBeInstanceOf(UnserializablePayloadError);
    expect(error).toBeInstanceOf(NonRetryableError);
    expect(error!.message).toContain('$.amount is bigint');
  });

  it('keeps ids and header names that fit in 255 bytes, counting UTF-8 bytes', () => {
    const edge = 'a'.repeat(255);
    const encoded = encodeEnvelope(
      createEnvelope({ id: edge, headers: { ['b'.repeat(246)]: 'v' } }),
    );
    expect(encoded.options.messageId).toBe(edge);
    expect(() => encodeEnvelope(createEnvelope({ id: 'é'.repeat(128) }))).toThrow(
      AmqpLimitExceededError,
    );
  });

  it('refuses an id or header name beyond the AMQP short string limit, as non-retryable', () => {
    expect(() => encodeEnvelope(createEnvelope({ id: 'x'.repeat(256) }))).toThrow(
      /Event id is 256 bytes, above the AMQP limit of 255 bytes/,
    );
    expect(() => encodeEnvelope(createEnvelope({ headers: { ['h'.repeat(247)]: 'v' } }))).toThrow(
      AmqpLimitExceededError,
    );
    expect(() => assertShortString('Routing key', 'k'.repeat(256))).toThrow(NonRetryableError);
  });

  it('mirrors type, source and correlation id into AMQP properties only when they fit, the headers always carry them', () => {
    const long = 'z'.repeat(300);
    const envelope = createEnvelope({ type: `a.${long}.v1`, source: long, correlationId: long });
    const encoded = encodeEnvelope(envelope);
    expect(encoded.options).not.toHaveProperty('type');
    expect(encoded.options).not.toHaveProperty('appId');
    expect(encoded.options).not.toHaveProperty('correlationId');
    expect(decodeEnvelope(asDelivery(encoded))).toEqual(envelope);
  });

  it('keeps a header literally named __proto__ instead of losing it', () => {
    const headers = JSON.parse('{"__proto__":"polluted","other":"kept"}') as Record<string, string>;
    const envelope = createEnvelope({ headers });
    const decoded = decodeEnvelope(asDelivery(encodeEnvelope(envelope)));
    expect(Object.keys(decoded.headers)).toEqual(['__proto__', 'other']);
    expect(Object.getOwnPropertyDescriptor(decoded.headers, '__proto__')?.value).toBe('polluted');
    expect(Object.getPrototypeOf(decoded.headers)).toBe(Object.prototype);
  });

  it('ignores broker headers and non-string event headers when decoding', () => {
    const decoded = decodeEnvelope(
      withHeaders({
        'cloudEvents:id': 'a',
        'cloudEvents:type': 'b.c.v1',
        'cloudEvents:source': 's',
        'cloudEvents:time': '2026-01-01T00:00:00.000Z',
        'x-death': [{ count: 1 }],
        'relaybus:number': 5,
        'relaybus:text': 'kept',
      }),
    );
    expect(decoded.headers).toEqual({ text: 'kept' });
  });

  it('falls back to AMQP properties for events from other CloudEvents producers', () => {
    const decoded = decodeEnvelope(
      withHeaders(
        {},
        {
          messageId: 'id-1',
          type: 'foreign.event.v1',
          appId: 'other-service',
          correlationId: 'corr-1',
          timestamp: 1_767_225_600,
        },
      ),
    );
    expect(decoded).toEqual({
      id: 'id-1',
      type: 'foreign.event.v1',
      source: 'other-service',
      key: null,
      payload: { a: 1 },
      headers: {},
      correlationId: 'corr-1',
      causationId: null,
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    });
  });

  it('handles a message without any headers', () => {
    expect(() =>
      decodeEnvelope({ content: Buffer.from('1'), fields: { routingKey: 'x' }, properties: {} }),
    ).toThrow(/CloudEvents 1.0/);
  });

  it.each([
    ['an unknown spec version', { 'cloudEvents:specversion': '0.3' }, /CloudEvents 1.0/],
    ['no id', { 'cloudEvents:id': '' }, /no id/],
    ['no type', { 'cloudEvents:type': undefined }, /no type/],
    ['no source', { 'cloudEvents:source': undefined }, /no source/],
    ['an unparsable time', { 'cloudEvents:time': 'yesterday' }, /not an RFC 3339/],
    ['a lenient date', { 'cloudEvents:time': 'Jan 1 2026' }, /not an RFC 3339/],
    ['a date without time zone', { 'cloudEvents:time': '2026-01-01T00:00:00' }, /not an RFC 3339/],
    ['an impossible date', { 'cloudEvents:time': '2026-13-45T00:00:00Z' }, /not an RFC 3339/],
    ['a time that is not text', { 'cloudEvents:time': 1_767_225_600 }, /not an RFC 3339/],
    ['no time at all', { 'cloudEvents:time': undefined }, /no valid time/],
  ])('rejects a message with %s', (_label, overrides, pattern) => {
    const headers = {
      'cloudEvents:id': 'a',
      'cloudEvents:type': 'b.c.v1',
      'cloudEvents:source': 's',
      'cloudEvents:time': '2026-01-01T00:00:00.000Z',
      ...overrides,
    };
    expect(() => decodeEnvelope(withHeaders(headers))).toThrow(MalformedMessageError);
    expect(() => decodeEnvelope(withHeaders(headers))).toThrow(pattern);
  });

  it('accepts RFC 3339 times with offsets, lower case and fractions', () => {
    const decode = (time: string) =>
      decodeEnvelope(
        withHeaders({
          'cloudEvents:id': 'a',
          'cloudEvents:type': 'b.c.v1',
          'cloudEvents:source': 's',
          'cloudEvents:time': time,
        }),
      ).occurredAt.toISOString();
    expect(decode('2026-01-01T02:00:00+02:00')).toBe('2026-01-01T00:00:00.000Z');
    expect(decode('2026-01-01t00:00:00.5z')).toBe('2026-01-01T00:00:00.500Z');
  });

  it('uses the AMQP timestamp only when the time header is absent', () => {
    const base = { 'cloudEvents:id': 'a', 'cloudEvents:type': 'b.c.v1', 'cloudEvents:source': 's' };
    expect(
      decodeEnvelope(withHeaders(base, { timestamp: 1_767_225_600 })).occurredAt.toISOString(),
    ).toBe('2026-01-01T00:00:00.000Z');
    expect(() =>
      decodeEnvelope(withHeaders({ ...base, 'cloudEvents:time': 'yesterday' }, { timestamp: 1 })),
    ).toThrow(/not an RFC 3339/);
    expect(() => decodeEnvelope(withHeaders(base, { timestamp: Number.NaN }))).toThrow(
      /no valid time/,
    );
    expect(() => decodeEnvelope(withHeaders(base, { timestamp: '1767225600' }))).toThrow(
      /no valid time/,
    );
  });

  it('rejects a body that is not JSON', () => {
    const message = asDelivery(encodeEnvelope(createEnvelope()));
    expect(() => decodeEnvelope({ ...message, content: Buffer.from('{oops') })).toThrow(
      'Message body is not valid JSON',
    );
  });
});
