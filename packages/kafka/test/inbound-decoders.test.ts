import { ConfigurationError } from '@relaybus/core';
import { describe, expect, it } from 'vitest';
import {
  InvalidKafkaMessageError,
  decodeCloudEvent,
  decodeEnvelope,
  fromHeader,
  fromPayloadField,
  plainJsonDecoder,
  type KafkaInboundRecord,
  type KafkaIncomingHeaders,
} from '../src/index.js';
import { completeDecodedEvent } from '../src/inbound-decoders.js';
import { asIncoming, createEnvelope } from './fakes.js';

const RECORD_TIME = Date.UTC(2026, 4, 1, 12, 0, 0);

function record(
  value: string | Buffer | null,
  headers: KafkaIncomingHeaders = {},
  overrides: Partial<KafkaInboundRecord> = {},
): KafkaInboundRecord {
  return {
    topic: 'orders',
    partition: 2,
    offset: '41',
    key: Buffer.from('order-7'),
    value: typeof value === 'string' ? Buffer.from(value) : value,
    headers,
    timestamp: String(RECORD_TIME),
    ...overrides,
  };
}

function structured(body: unknown, headers: KafkaIncomingHeaders = {}): KafkaInboundRecord {
  return record(JSON.stringify(body), {
    'content-type': 'application/cloudevents+json; charset=utf-8',
    ...headers,
  });
}

const minimalStructured = {
  specversion: '1.0',
  id: 'evt-1',
  type: 'order.placed.v1',
  source: '//legacy/orders',
};

function invalidReason(decode: () => unknown): string {
  try {
    decode();
  } catch (error) {
    expect(error).toBeInstanceOf(InvalidKafkaMessageError);
    return (error as Error).message;
  }
  throw new Error('expected the record to be invalid');
}

describe('decodeCloudEvent', () => {
  it('decodes binary mode exactly like the default decoder', () => {
    const envelope = createEnvelope();
    expect(decodeCloudEvent(asIncoming(envelope))).toEqual(decodeEnvelope(asIncoming(envelope)));
    expect(decodeCloudEvent(asIncoming(envelope))).toEqual(envelope);
  });

  it('still rejects a plain JSON record, like the default decoder', () => {
    expect(invalidReason(() => decodeCloudEvent(record('{"a":1}')))).toContain('ce_specversion');
  });

  it('decodes structured mode with every attribute', () => {
    const decoded = decodeCloudEvent(
      structured(
        {
          ...minimalStructured,
          time: '2026-03-04T05:06:07.890Z',
          subject: 'order-9',
          datacontenttype: 'application/json',
          dataschema: 'https://example.com/schema',
          data: { orderId: '9' },
          correlationid: 'flow-1',
          causationid: 'cause-1',
          traceparent: '00-abc-def-01',
          sequence: 4,
        },
        { tenant: Buffer.from('acme'), 'x-relaybus-h-content-type': 'escaped' },
      ),
    );
    expect(decoded).toEqual({
      id: 'evt-1',
      type: 'order.placed.v1',
      source: '//legacy/orders',
      key: 'order-9',
      payload: { orderId: '9' },
      headers: { tenant: 'acme', 'content-type': 'escaped', traceparent: '00-abc-def-01' },
      correlationId: 'flow-1',
      causationId: 'cause-1',
      occurredAt: new Date('2026-03-04T05:06:07.890Z'),
    });
  });

  it('fills a structured event without optional attributes from the record', () => {
    expect(decodeCloudEvent(structured(minimalStructured))).toEqual({
      id: 'evt-1',
      type: 'order.placed.v1',
      source: '//legacy/orders',
      key: 'order-7',
      payload: null,
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date(RECORD_TIME),
    });
  });

  it.each(['application/json', 'text/json', 'application/vnd.orders+json; charset=utf-8'])(
    'accepts the JSON datacontenttype %s',
    (datacontenttype) => {
      const decoded = decodeCloudEvent(structured({ ...minimalStructured, datacontenttype }));
      expect(decoded.id).toBe('evt-1');
    },
  );

  it.each([
    [
      'a body that is not JSON',
      record('{nope', { 'content-type': 'application/cloudevents+json' }),
    ],
    ['a JSON array', structured([minimalStructured])],
    ['a JSON null', structured(null)],
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
    ['a non-string subject', structured({ ...minimalStructured, subject: 3 })],
  ])('rejects a structured event with %s', (_label, input) => {
    expect(() => decodeCloudEvent(input)).toThrow(InvalidKafkaMessageError);
  });

  it('names the structured attribute in the error', () => {
    expect(
      invalidReason(() => decodeCloudEvent(structured({ ...minimalStructured, time: 'x' }))),
    ).toBe("time is not an RFC 3339 timestamp: 'x'");
    expect(
      invalidReason(() => decodeCloudEvent(structured({ ...minimalStructured, type: 'a b' }))),
    ).toMatch(/^type is not a valid event type/);
  });

  it('rejects a structured event without time on a record without timestamp', () => {
    const input = { ...structured(minimalStructured), timestamp: undefined };
    expect(invalidReason(() => decodeCloudEvent(input))).toBe(
      'The event has no time and the record has no usable timestamp',
    );
  });

  it('rejects a structured event whose record key is not UTF-8 when it has no subject', () => {
    const input = { ...structured(minimalStructured), key: Buffer.from([0xff, 0xfe]) };
    expect(() => decodeCloudEvent(input)).toThrow(InvalidKafkaMessageError);
  });
});

describe('plainJsonDecoder', () => {
  it('maps a plain JSON record with a fixed type and defaults for everything else', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(decode(record('{"orderId":"7"}', { traceparent: Buffer.from('tp') }))).toEqual({
      id: 'kafka:orders:2:41',
      type: 'order.placed.v1',
      source: 'orders',
      key: 'order-7',
      payload: { orderId: '7' },
      headers: { traceparent: 'tp' },
      correlationId: null,
      causationId: null,
      occurredAt: new Date(RECORD_TIME),
    });
  });

  it('derives the same id for every delivery of a record, and different ids for different offsets', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    const first = decode(record('{"n":1}'));
    const redelivered = decode(record('{"n":1}'));
    const next = decode(record('{"n":1}', {}, { offset: '42' }));
    const otherPartition = decode(record('{"n":1}', {}, { partition: 3 }));
    const otherTopic = decode(record('{"n":1}', {}, { topic: 'payments' }));
    expect(redelivered.id).toBe(first.id);
    expect(new Set([first.id, next.id, otherPartition.id, otherTopic.id]).size).toBe(4);
  });

  it('reads type, id, key and source from headers or payload fields', () => {
    const decode = plainJsonDecoder({
      type: fromHeader('event-type'),
      id: fromPayloadField('meta.id'),
      key: fromPayloadField('customer'),
      source: fromHeader('producer'),
      occurredAt: (input) => new Date((input.payload as { at: string }).at),
    });
    const decoded = decode(
      record('{"meta":{"id":991},"customer":"c-1","at":"2026-02-02T00:00:00Z"}', {
        'event-type': 'order.shipped.v1',
        producer: Buffer.from('legacy-shipping'),
      }),
    );
    expect(decoded).toMatchObject({
      id: '991',
      type: 'order.shipped.v1',
      key: 'c-1',
      source: 'legacy-shipping',
      occurredAt: new Date('2026-02-02T00:00:00Z'),
    });
  });

  it('prefers a header type over the payload, or the payload when the header is absent', () => {
    const decode = plainJsonDecoder({
      type: (input) => fromHeader('event-type')(input) ?? fromPayloadField('type')(input),
    });
    expect(decode(record('{"type":"a.v1"}', { 'event-type': 'b.v1' })).type).toBe('b.v1');
    expect(decode(record('{"type":"a.v1"}')).type).toBe('a.v1');
  });

  it('falls back to the derived id and the topic when the extractors find nothing', () => {
    const decode = plainJsonDecoder({
      type: 'order.placed.v1',
      id: fromHeader('message-id'),
      source: () => '',
    });
    expect(decode(record('{}'))).toMatchObject({ id: 'kafka:orders:2:41', source: 'orders' });
  });

  it('uses a fixed source', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1', source: 'legacy' });
    expect(decode(record('{}')).source).toBe('legacy');
  });

  it('lets the key option drop the key', () => {
    expect(plainJsonDecoder({ type: 'a.v1', key: () => null })(record('{}')).key).toBeNull();
    expect(plainJsonDecoder({ type: 'a.v1', key: () => undefined })(record('{}')).key).toBeNull();
  });

  it('keeps a missing key as null and a binary key as base64', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(decode(record('{}', {}, { key: null })).key).toBeNull();
    const binaryKey = Buffer.from([0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xfe]);
    expect(decode(record('{}', {}, { key: binaryKey })).key).toBe(binaryKey.toString('base64'));
  });

  it('reads headers leniently: last value of a repeated header, invalid UTF-8 left out', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    const decoded = decode(
      record('{}', {
        repeated: [Buffer.from('first'), 'second'],
        binary: Buffer.from([0xff]),
        absent: undefined,
      }),
    );
    expect(decoded.headers).toEqual({ repeated: 'second' });
  });

  it('decodes a binary-mode CloudEvent on the same topic as a CloudEvent', () => {
    const envelope = createEnvelope();
    const decode = plainJsonDecoder({ type: 'something.else.v1' });
    expect(decode({ ...asIncoming(envelope), topic: 'orders', partition: 0 })).toEqual(envelope);
  });

  it('decodes a structured CloudEvent on the same topic as a CloudEvent', () => {
    const decode = plainJsonDecoder({ type: 'something.else.v1' });
    expect(decode(structured(minimalStructured))).toMatchObject({
      id: 'evt-1',
      type: 'order.placed.v1',
    });
  });

  it('rejects a CloudEvent that is malformed instead of reading it as plain JSON', () => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(() => decode(record('{}', { ce_specversion: '1.0' }))).toThrow(InvalidKafkaMessageError);
    expect(() => decode(record('{}', { ce_specversion: Buffer.from([0xff]) }))).toThrow(
      InvalidKafkaMessageError,
    );
  });

  it.each([
    ['no value (a tombstone)', null, 'Message has no value'],
    ['a value that is not JSON', 'order 7 placed', 'Message value is not valid JSON'],
    ['a value that is not UTF-8', Buffer.from([0xff]), 'Message value is not valid UTF-8'],
  ])('rejects a record with %s', (_label, value, reason) => {
    const decode = plainJsonDecoder({ type: 'order.placed.v1' });
    expect(invalidReason(() => decode(record(value)))).toBe(reason);
  });

  it('falls back to the derived id when the id header is not valid UTF-8, so ids never collide', () => {
    const decode = plainJsonDecoder({ type: 'a.v1', id: fromHeader('message-id') });
    const first = decode(record('{}', { 'message-id': Buffer.from([0xff]) }));
    const second = decode(record('{}', { 'message-id': Buffer.from([0xfe]) }, { offset: '42' }));
    expect([first.id, second.id]).toEqual(['kafka:orders:2:41', 'kafka:orders:2:42']);
  });

  it('keeps a leading byte order mark in a header, so it stays a different id', () => {
    const decode = plainJsonDecoder({ type: 'a.v1', id: fromHeader('message-id') });
    const withMark = Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from('event-1')]);
    expect(decode(record('{}', { 'message-id': withMark })).id).toBe('\uFEFFevent-1');
    expect(decode(record('{}', { 'message-id': Buffer.from('event-1') })).id).toBe('event-1');
  });

  it('rejects a record whose type cannot be found or is invalid', () => {
    const decode = plainJsonDecoder({ type: fromHeader('event-type') });
    expect(invalidReason(() => decode(record('{}')))).toBe(
      'Record has no event type: the type option found none',
    );
    expect(invalidReason(() => decode(record('{}', { 'event-type': 'not valid' })))).toMatch(
      /^The event type is not a valid event type/,
    );
  });

  it('rejects a record when an extractor returns something other than a string', () => {
    const badId = plainJsonDecoder({ type: 'a.v1', id: () => 42 as unknown as string });
    expect(invalidReason(() => badId(record('{}')))).toBe(
      'The id option returned number, expected a string',
    );
  });

  it.each([
    ['not a Date', () => '2026-01-01' as unknown as Date],
    ['an invalid Date', () => new Date('nope')],
  ])('rejects a record when occurredAt returns %s', (_label, occurredAt) => {
    const decode = plainJsonDecoder({ type: 'a.v1', occurredAt });
    expect(invalidReason(() => decode(record('{}')))).toBe(
      'The occurredAt option returned no valid Date',
    );
  });

  it('rejects a record without timestamp when occurredAt finds none', () => {
    const decode = plainJsonDecoder({ type: 'a.v1', occurredAt: () => undefined });
    expect(() => decode(record('{}', {}, { timestamp: undefined }))).toThrow(
      'The event has no time and the record has no usable timestamp',
    );
  });

  it('refuses async extractors and observes their rejections', () => {
    const asyncType = plainJsonDecoder({
      type: (() => Promise.reject(new Error('lookup failed'))) as unknown as () => string,
    });
    expect(() => asyncType(record('{}'))).toThrow(InvalidKafkaMessageError);
    const asyncTime = plainJsonDecoder({
      type: 'a.v1',
      occurredAt: (() => Promise.reject(new Error('lookup failed'))) as unknown as () => Date,
    });
    expect(() => asyncTime(record('{}'))).toThrow(InvalidKafkaMessageError);
  });

  it('refuses an invalid fixed type at construction', () => {
    expect(() => plainJsonDecoder({ type: 'not valid' })).toThrow(ConfigurationError);
  });
});

describe('field extractors', () => {
  it('reads a header by its exact name', () => {
    const read = fromHeader('Event-Type');
    expect(read({ headers: { 'Event-Type': 'a' } })).toBe('a');
    expect(read({ headers: { 'event-type': 'a' } })).toBeUndefined();
    expect(fromHeader('toString')({ headers: {} })).toBeUndefined();
  });

  it('reads a payload field by a dotted path, as text', () => {
    const payload = {
      a: { b: 'text', n: 12.5, int: 12, nan: Number.NaN, flag: true, nil: null },
      list: ['x'],
    };
    expect(fromPayloadField('a.b')({ payload })).toBe('text');
    expect(fromPayloadField('a.n')({ payload })).toBeUndefined();
    expect(fromPayloadField('a.int')({ payload })).toBe('12');
    expect(fromPayloadField('id')({ payload: JSON.parse('{"id":9007199254740993}') })).toBe(
      undefined,
    );
    expect(fromPayloadField('list.0')({ payload })).toBe('x');
    expect(fromPayloadField('a.nan')({ payload })).toBeUndefined();
    expect(fromPayloadField('a.flag')({ payload })).toBeUndefined();
    expect(fromPayloadField('a.nil.x')({ payload })).toBeUndefined();
    expect(fromPayloadField('a.missing')({ payload })).toBeUndefined();
    expect(fromPayloadField('a.b.length')({ payload })).toBeUndefined();
    expect(fromPayloadField('a')({ payload: 'scalar' })).toBeUndefined();
    expect(fromPayloadField('toString')({ payload: {} })).toBeUndefined();
  });

  it.each([[''], ['a..b'], ['.a']])('refuses the payload path %j', (path) => {
    expect(() => fromPayloadField(path)).toThrow(ConfigurationError);
  });

  it('refuses an empty header name', () => {
    expect(() => fromHeader('')).toThrow(ConfigurationError);
  });
});

describe('completeDecodedEvent', () => {
  it('fills the fields a decoder left out from the record', () => {
    expect(completeDecodedEvent({ id: 'x', type: 'a.v1', payload: 1 }, record('{}'))).toEqual({
      id: 'x',
      type: 'a.v1',
      source: 'orders',
      key: 'order-7',
      payload: 1,
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date(RECORD_TIME),
    });
  });

  it('keeps every field a decoder set, including a null key', () => {
    const decoded = {
      id: 'x',
      type: 'a.v1',
      payload: 1,
      source: 's',
      key: null,
      headers: { h: 'v' },
      correlationId: 'c',
      causationId: 'k',
      occurredAt: new Date(0),
    };
    expect(completeDecodedEvent(decoded, record('{}'))).toEqual(decoded);
  });

  it.each([
    ['undefined', () => undefined],
    ['null', () => null],
    ['a string', () => 'event'],
    ['a promise', () => Promise.resolve({ id: 'x', type: 'a.v1', payload: 1 })],
    [
      'a rejected promise, observing its rejection',
      () => Promise.reject(new Error('async failure')),
    ],
  ])('rejects a decoder result that is %s', (_label, createResult: () => unknown) => {
    expect(invalidReason(() => completeDecodedEvent(createResult(), record('{}')))).toBe(
      'The decoder must return an event object synchronously',
    );
  });
});
