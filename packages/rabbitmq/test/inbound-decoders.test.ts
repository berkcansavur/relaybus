import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import type { AmqpMessage, AmqpMessageProperties } from '../src/amqp-types.js';
import { MalformedMessageError } from '../src/errors.js';
import { decodeCloudEvent, fromHeader, fromPayloadField, plainJsonDecoder } from '../src/index.js';
import { completeDecodedEvent } from '../src/inbound-decoders.js';
import { decodeEnvelope } from '../src/wire-format.js';
import { createDelivery, createEnvelope } from './fakes.js';

const NOW = new Date('2026-06-01T08:00:00.000Z');

beforeEach(() => {
  vi.useFakeTimers({ now: NOW, toFake: ['Date'] });
});

afterEach(() => {
  vi.useRealTimers();
});

function message(body: string, properties: AmqpMessageProperties = {}): AmqpMessage {
  return { content: Buffer.from(body), fields: { routingKey: 'orders.placed' }, properties };
}

function structured(body: unknown, properties: AmqpMessageProperties = {}): AmqpMessage {
  return message(JSON.stringify(body), {
    contentType: 'application/cloudevents+json; charset=utf-8',
    ...properties,
  });
}

const minimalStructured = {
  specversion: '1.0',
  id: 'evt-1',
  type: 'order.placed.v1',
  source: '//legacy/orders',
};

function malformedReason(decode: () => unknown): string {
  try {
    decode();
  } catch (error) {
    expect(error).toBeInstanceOf(MalformedMessageError);
    return (error as Error).message;
  }
  throw new Error('expected the message to be malformed');
}

describe('decodeCloudEvent', () => {
  it('decodes binary mode exactly like the default decoder', () => {
    const delivery = createDelivery();
    expect(decodeCloudEvent(delivery)).toEqual(decodeEnvelope(delivery));
    expect(decodeCloudEvent(delivery)).toEqual(createEnvelope());
  });

  it('still rejects a plain JSON message, like the default decoder', () => {
    expect(malformedReason(() => decodeCloudEvent(message('{}')))).toContain('specversion');
  });

  it('decodes structured mode with every attribute', () => {
    const decoded = decodeCloudEvent(
      structured(
        {
          ...minimalStructured,
          time: '2026-03-04T05:06:07.890Z',
          subject: 'order-9',
          datacontenttype: 'application/json',
          data: { orderId: '9' },
          correlationid: 'flow-1',
          causationid: 'cause-1',
          traceparent: '00-abc-def-01',
          sequence: 4,
        },
        { headers: { tenant: 'acme', 'x-death': [{ count: 1 }] } },
      ),
    );
    expect(decoded).toEqual({
      id: 'evt-1',
      type: 'order.placed.v1',
      source: '//legacy/orders',
      key: 'order-9',
      payload: { orderId: '9' },
      headers: { tenant: 'acme', traceparent: '00-abc-def-01' },
      correlationId: 'flow-1',
      causationId: 'cause-1',
      occurredAt: new Date('2026-03-04T05:06:07.890Z'),
    });
  });

  it('fills a structured event without optional attributes', () => {
    expect(decodeCloudEvent(structured(minimalStructured, { timestamp: 1_767_225_600 }))).toEqual({
      id: 'evt-1',
      type: 'order.placed.v1',
      source: '//legacy/orders',
      key: null,
      payload: null,
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date(1_767_225_600_000),
    });
    expect(decodeCloudEvent(structured(minimalStructured)).occurredAt).toEqual(NOW);
  });

  it.each(['application/json', 'text/json', 'application/vnd.orders+json'])(
    'accepts the JSON datacontenttype %s',
    (datacontenttype) => {
      expect(decodeCloudEvent(structured({ ...minimalStructured, datacontenttype })).id).toBe(
        'evt-1',
      );
    },
  );

  it.each([
    ['a body that is not JSON', message('{nope', { contentType: 'application/cloudevents+json' })],
    ['a JSON array', structured([minimalStructured])],
    ['another specversion', structured({ ...minimalStructured, specversion: '0.3' })],
    ['a missing id', structured({ ...minimalStructured, id: undefined })],
    ['an empty source', structured({ ...minimalStructured, source: '' })],
    ['a numeric type', structured({ ...minimalStructured, type: 7 })],
    ['an invalid type', structured({ ...minimalStructured, type: 'has spaces' })],
    ['binary data', structured({ ...minimalStructured, data_base64: 'AAEC' })],
    [
      'a non-JSON datacontenttype',
      structured({ ...minimalStructured, datacontenttype: 'text/plain' }),
    ],
    ['a time that is not RFC 3339', structured({ ...minimalStructured, time: 'yesterday' })],
    ['an impossible time', structured({ ...minimalStructured, time: '2026-13-45T00:00:00Z' })],
  ])('rejects a structured event with %s', (_label, input) => {
    expect(() => decodeCloudEvent(input)).toThrow(MalformedMessageError);
  });
});

describe('plainJsonDecoder', () => {
  it('maps a plain JSON message with a fixed type and defaults for everything else', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    const decoded = decode(
      message('{"orderId":"7"}', {
        messageId: 'msg-7',
        appId: 'legacy-java',
        timestamp: 1_767_225_600,
        headers: {
          traceparent: 'tp',
          'x-delivery-count': 2,
          'x-first-death-queue': 'q',
          'x-last-death-reason': 'expired',
          'x-relaybus-redeliveries': '1',
        },
      }),
    );
    expect(decoded).toEqual({
      id: 'msg-7',
      type: 'order.placed.v1',
      source: 'legacy-java',
      key: null,
      payload: { orderId: '7' },
      headers: { traceparent: 'tp' },
      correlationId: null,
      causationId: null,
      occurredAt: new Date(1_767_225_600_000),
    });
  });

  it('gives every delivery of a message the same id, its messageId', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    const first = decode(message('{}', { messageId: 'm-1' }));
    const redelivered = decode(
      message('{}', { messageId: 'm-1', headers: { 'x-delivery-count': 1 } }),
    );
    expect(redelivered.id).toBe(first.id);
  });

  it('falls back to the routing key and the time of receipt', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(decode(message('{}', { messageId: 'm-1', appId: '' }))).toMatchObject({
      source: 'orders.placed',
      occurredAt: NOW,
    });
  });

  it('reads type, id, key, source and time through the options', () => {
    const decode = plainJsonDecoder({
      type: (input) => input.properties.type as string,
      id: fromHeader('event-id'),
      key: fromPayloadField('customer.id'),
      source: 'shipping',
      occurredAt: (input) => new Date((input.payload as { at: string }).at),
    });
    const decoded = decode(
      message('{"customer":{"id":42},"at":"2026-02-02T00:00:00Z"}', {
        type: 'order.shipped.v1',
        headers: { 'event-id': 'e-1' },
      }),
    );
    expect(decoded).toMatchObject({
      id: 'e-1',
      type: 'order.shipped.v1',
      key: '42',
      source: 'shipping',
      occurredAt: new Date('2026-02-02T00:00:00Z'),
    });
  });

  it('prefers a header type over the payload, or the payload when the header is absent', () => {
    const decode = plainJsonDecoder({
      type: (input) => fromHeader('event-type')(input) ?? fromPayloadField('type')(input),
    });
    const withHeader = message('{"type":"a.v1"}', {
      messageId: 'm',
      headers: { 'event-type': 'b.v1' },
    });
    expect(decode(withHeader).type).toBe('b.v1');
    expect(decode(message('{"type":"a.v1"}', { messageId: 'm' })).type).toBe('a.v1');
  });

  it('falls back to messageId and appId when the extractors find nothing', () => {
    const decode = plainJsonDecoder({
      type: 'order.placed.v1',
      id: fromHeader('event-id'),
      source: () => undefined,
      key: () => '',
    });
    expect(decode(message('{}', { messageId: 'm-1', appId: 'legacy' }))).toMatchObject({
      id: 'm-1',
      source: 'legacy',
      key: null,
    });
  });

  it('decodes a binary-mode CloudEvent in the same queue as a CloudEvent', () => {
    const decode = plainJsonDecoder({ type: 'something.else.v1' });
    expect(decode(createDelivery())).toEqual(createEnvelope());
  });

  it('decodes a structured CloudEvent in the same queue as a CloudEvent', () => {
    const decode = plainJsonDecoder({ type: 'something.else.v1' });
    expect(decode(structured(minimalStructured))).toMatchObject({ id: 'evt-1' });
  });

  it.each([
    ['a body that is not JSON', 'order 7 placed', 'Message body is not valid JSON'],
    ['an empty body', '', 'Message body is not valid JSON'],
  ])('rejects a message with %s', (_label, body, reason) => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(malformedReason(() => decode(message(body, { messageId: 'm' })))).toBe(reason);
  });

  it('rejects a body that is not valid UTF-8 instead of replacing the bytes', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    const input: AmqpMessage = {
      content: Buffer.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d]),
      fields: { routingKey: 'orders.placed' },
      properties: { messageId: 'm' },
    };
    expect(malformedReason(() => decode(input))).toBe('Message body is not valid UTF-8');
  });

  it('rejects a message without any id, because a redelivery could not be recognised', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(malformedReason(() => decode(message('{}')))).toBe(
      'Message has no id: set the messageId property on the producer or configure the id option',
    );
    expect(() => decode(message('{}', { messageId: '' }))).toThrow(MalformedMessageError);
  });

  it('rejects a message whose type cannot be found or is invalid', () => {
    const decode = plainJsonDecoder({ type: fromHeader('event-type') });
    expect(malformedReason(() => decode(message('{}', { messageId: 'm' })))).toBe(
      'Message has no event type: the type option found none',
    );
    expect(
      malformedReason(() =>
        decode(message('{}', { messageId: 'm', headers: { 'event-type': 'not valid' } })),
      ),
    ).toMatch(/^The event type is not a valid event type/);
  });

  it('rejects a message when an extractor returns something other than a string', () => {
    const decode = plainJsonDecoder({ type: (input) => input.properties.type as string });
    expect(malformedReason(() => decode(message('{}', { messageId: 'm', type: 5 })))).toBe(
      'The type option returned number, expected a string',
    );
  });

  it.each([
    ['not a Date', () => 1 as unknown as Date],
    ['an invalid Date', () => new Date('nope')],
  ])('rejects a message when occurredAt returns %s', (_label, occurredAt) => {
    const decode = plainJsonDecoder({ type: 'a.v1', occurredAt });
    expect(malformedReason(() => decode(message('{}', { messageId: 'm' })))).toBe(
      'The occurredAt option returned no valid Date',
    );
  });

  it('refuses async extractors and observes their rejections', () => {
    const asyncType = plainJsonDecoder({
      type: (() => Promise.reject(new Error('lookup failed'))) as unknown as () => string,
    });
    expect(() => asyncType(message('{}', { messageId: 'm' }))).toThrow(MalformedMessageError);
    const asyncTime = plainJsonDecoder({
      type: 'a.v1',
      occurredAt: (() => Promise.reject(new Error('lookup failed'))) as unknown as () => Date,
    });
    expect(() => asyncTime(message('{}', { messageId: 'm' }))).toThrow(MalformedMessageError);
  });

  it('refuses an invalid fixed type at construction', () => {
    expect(() => plainJsonDecoder({ type: 'not valid' })).toThrow(ConfigurationError);
  });
});

describe('field extractors', () => {
  it('reads a header by its exact name and a payload field by its path', () => {
    expect(fromHeader('Type')({ headers: { Type: 'a' } })).toBe('a');
    expect(fromHeader('type')({ headers: { Type: 'a' } })).toBeUndefined();
    expect(fromPayloadField('a.b')({ payload: { a: { b: 3 } } })).toBe('3');
    expect(fromPayloadField('a.b')({ payload: { a: { b: [] } } })).toBeUndefined();
    expect(fromPayloadField('a.b')({ payload: { a: null } })).toBeUndefined();
  });

  it('refuses an empty header name or path segment', () => {
    expect(() => fromHeader('')).toThrow(ConfigurationError);
    expect(() => fromPayloadField('a.')).toThrow(ConfigurationError);
  });
});

describe('completeDecodedEvent', () => {
  it('keeps every field a decoder set', () => {
    const decoded = {
      id: 'x',
      type: 'a.v1',
      payload: 1,
      source: 's',
      key: 'k',
      headers: { h: 'v' },
      correlationId: 'c',
      causationId: 'z',
      occurredAt: new Date(0),
    };
    expect(completeDecodedEvent(decoded, message('{}'))).toEqual(decoded);
  });

  it('fills the time of receipt when the message has no usable timestamp', () => {
    const decoded = { id: 'x', type: 'a.v1', payload: 1 };
    expect(
      completeDecodedEvent(decoded, message('{}', { timestamp: Number.NaN })).occurredAt,
    ).toEqual(NOW);
  });

  it.each([
    ['undefined', () => undefined],
    ['null', () => null],
    ['a number', () => 3],
    ['a promise', () => Promise.resolve({ id: 'x', type: 'a.v1', payload: 1 })],
    [
      'a rejected promise, observing its rejection',
      () => Promise.reject(new Error('async failure')),
    ],
  ])('rejects a decoder result that is %s', (_label, createResult: () => unknown) => {
    expect(malformedReason(() => completeDecodedEvent(createResult(), message('{}')))).toBe(
      'The decoder must return an event object synchronously',
    );
  });
});
