import { describe, expect, it } from 'vitest';
import type { AmqpMessage } from '../src/amqp-types.js';
import { MalformedMessageError } from '../src/errors.js';
import { decodeEnvelope, encodeEnvelope } from '../src/wire-format.js';
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

  it('encodes an undefined payload as JSON null', () => {
    const envelope = createEnvelope({ payload: undefined });
    expect(decodeEnvelope(asDelivery(encodeEnvelope(envelope))).payload).toBeNull();
  });

  it('refuses a payload that cannot be serialized', () => {
    expect(() => encodeEnvelope(createEnvelope({ payload: 10n }))).toThrow(TypeError);
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
    ['an unparsable time', { 'cloudEvents:time': 'yesterday' }, /no valid time/],
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

  it('rejects a body that is not JSON', () => {
    const message = asDelivery(encodeEnvelope(createEnvelope()));
    expect(() => decodeEnvelope({ ...message, content: Buffer.from('{oops') })).toThrow(
      'Message body is not valid JSON',
    );
  });
});
