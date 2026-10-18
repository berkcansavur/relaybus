import { NonRetryableError } from '@relaybus/core';
import { describe, expect, it } from 'vitest';
import {
  InvalidKafkaMessageError,
  UnencodableEventError,
  decodeEnvelope,
  encodeEnvelope,
  type KafkaIncomingHeaders,
} from '../src/index.js';
import { toOutgoingHeaders } from '../src/cloudevents-codec.js';
import { asIncoming, createEnvelope } from './fakes.js';

function incoming(headers: KafkaIncomingHeaders = {}, value: string | Buffer | null = '{"a":1}') {
  const valid = asIncoming(createEnvelope()).headers;
  return {
    key: Buffer.from('order-1'),
    value: typeof value === 'string' ? Buffer.from(value) : value,
    headers: { ...valid, ...headers },
  };
}

function omitSubject(headers: KafkaIncomingHeaders): KafkaIncomingHeaders {
  return Object.fromEntries(Object.entries(headers).filter(([name]) => name !== 'ce_subject'));
}

describe('encodeEnvelope', () => {
  it('writes CloudEvents binary mode: JSON value, ce_* headers, key as subject', () => {
    const encoded = encodeEnvelope(createEnvelope());
    expect(encoded.key).toBe('order-1');
    expect(encoded.value).toBe('{"orderId":"1"}');
    expect(encoded.headers).toEqual({
      traceparent: '00-abc-def-01',
      ce_specversion: '1.0',
      ce_id: '0190a000-0000-7000-8000-0000000000aa',
      ce_type: 'order.placed.v1',
      ce_source: 'orders',
      ce_time: '2026-01-01T00:00:00.000Z',
      'content-type': 'application/json',
      ce_subject: 'order-1',
      ce_correlationid: 'flow-1',
      ce_causationid: 'cause-1',
    });
  });

  it('omits subject, correlation and causation when absent', () => {
    const { key, headers } = encodeEnvelope(
      createEnvelope({ key: null, correlationId: null, causationId: null, headers: {} }),
    );
    expect(key).toBeNull();
    expect(Object.keys(headers).sort()).toEqual([
      'ce_id',
      'ce_source',
      'ce_specversion',
      'ce_time',
      'ce_type',
      'content-type',
    ]);
  });

  it('rejects a payload JSON cannot represent', () => {
    expect(() => encodeEnvelope(createEnvelope({ payload: undefined }))).toThrow(
      /payload that JSON cannot represent/,
    );
  });
});

describe('encodeEnvelope validation', () => {
  const circular: Record<string, unknown> = {};
  circular['self'] = circular;

  it.each([
    ['undefined', undefined],
    ['a function', () => 1],
    ['a symbol', Symbol('x')],
    ['a BigInt', 10n],
    ['a nested BigInt', { amount: 10n }],
    ['a circular structure', circular],
  ])('rejects %s as a payload with a non-retryable error', (_label, payload) => {
    const attempt = (): unknown => encodeEnvelope(createEnvelope({ payload }));
    expect(attempt).toThrow(UnencodableEventError);
    expect(attempt).toThrow(NonRetryableError);
    expect(attempt).toThrow(/payload that JSON cannot represent/);
  });

  it.each([
    ['an invalid date', new Date(Number.NaN)],
    ['a year beyond 9999', new Date('+010000-01-01T00:00:00.000Z')],
    ['a negative year', new Date('-000001-01-01T00:00:00.000Z')],
  ])('rejects occurredAt with %s instead of writing a non RFC 3339 ce_time', (_l, occurredAt) => {
    expect(() => encodeEnvelope(createEnvelope({ occurredAt }))).toThrow(/not an RFC 3339 instant/);
  });

  it('accepts the first and last representable instants', () => {
    for (const iso of ['0000-01-01T00:00:00.000Z', '9999-12-31T23:59:59.999Z']) {
      const envelope = createEnvelope({ occurredAt: new Date(iso) });
      expect(decodeEnvelope(asIncoming(envelope)).occurredAt.toISOString()).toBe(iso);
    }
  });

  it('keeps a header named __proto__ as data through encode, decode and re-emit', () => {
    const headers = Object.fromEntries([
      ['__proto__', 'p'],
      ['plain', 'v'],
    ]);
    const encoded = encodeEnvelope(createEnvelope({ headers }));
    expect(Object.getOwnPropertyDescriptor(encoded.headers, '__proto__')?.value).toBe('p');
    const decoded = decodeEnvelope(asIncoming(createEnvelope({ headers })));
    expect(Object.getOwnPropertyDescriptor(decoded.headers, '__proto__')?.value).toBe('p');
    expect(Object.getPrototypeOf(decoded.headers)).toBe(Object.prototype);
    const outgoing = toOutgoingHeaders(headers);
    expect(Object.getOwnPropertyDescriptor(outgoing, '__proto__')?.value).toBe('p');
  });
});

describe('ce_time fallback', () => {
  it('uses the Kafka record timestamp for a plain CloudEvents producer record without ce_time', () => {
    const plain = {
      key: Buffer.from('order-1'),
      value: Buffer.from('{"a":1}'),
      timestamp: '1767225600000',
      headers: {
        ce_specversion: Buffer.from('1.0'),
        ce_id: Buffer.from('plain-1'),
        ce_type: Buffer.from('order.placed.v1'),
        ce_source: Buffer.from('//legacy'),
        'content-type': Buffer.from('application/json'),
      },
    };
    const decoded = decodeEnvelope(plain);
    expect(decoded.occurredAt.toISOString()).toBe('2026-01-01T00:00:00.000Z');
    expect(decoded.id).toBe('plain-1');
  });

  it('prefers ce_time over the record timestamp', () => {
    const decoded = decodeEnvelope({ ...incoming(), timestamp: '1' });
    expect(decoded.occurredAt.toISOString()).toBe('2026-01-01T00:00:00.000Z');
  });

  it.each([
    ['no timestamp', undefined],
    ['an empty timestamp', ''],
    ['a non numeric timestamp', 'abc'],
  ])('rejects a missing ce_time with %s', (_label, timestamp) => {
    expect(() => decodeEnvelope({ ...incoming({ ce_time: undefined }), timestamp })).toThrow(
      /ce_time is missing/,
    );
  });

  it('treats an empty ce_time as absent', () => {
    const decoded = decodeEnvelope({ ...incoming({ ce_time: '' }), timestamp: '0' });
    expect(decoded.occurredAt.getTime()).toBe(0);
  });
});

describe('decodeEnvelope round trip', () => {
  it.each([
    ['a full envelope', createEnvelope()],
    [
      'no key, correlation or causation',
      createEnvelope({ key: null, correlationId: null, causationId: null, headers: {} }),
    ],
    ['an empty key', createEnvelope({ key: '' })],
    ['a null payload', createEnvelope({ payload: null })],
    ['a scalar payload', createEnvelope({ payload: 42 })],
    ['unicode content', createEnvelope({ payload: { name: 'Çağrı 🚀' }, key: 'ключ' })],
    [
      'user headers that collide with protocol names',
      createEnvelope({
        headers: {
          ce_id: 'user',
          CE_Type: 'user',
          'Content-Type': 'text/plain',
          'x-relaybus-h-ce_id': 'nested',
          'x-relaybus-other': 'x',
          plain: 'p',
        },
      }),
    ],
  ])('restores %s exactly', (_label, envelope) => {
    const decoded = decodeEnvelope(asIncoming(envelope));
    expect(decoded).toEqual(envelope);
    expect(decoded.occurredAt).toBeInstanceOf(Date);
    expect(decoded.occurredAt.getTime()).toBe(envelope.occurredAt.getTime());
  });

  it('accepts string header values and a content type with parameters', () => {
    const decoded = decodeEnvelope(
      incoming({
        ce_id: 'id-1',
        ce_time: '2026-01-01T01:00:00+01:00',
        'content-type': 'Application/JSON; charset=utf-8',
      }),
    );
    expect(decoded.id).toBe('id-1');
    expect(decoded.occurredAt.toISOString()).toBe('2026-01-01T00:00:00.000Z');
  });

  it('takes the key from ce_subject alone, or from the record key alone', () => {
    const base = incoming();
    expect(decodeEnvelope({ ...base, key: null }).key).toBe('order-1');
    const withoutSubject = omitSubject(base.headers);
    expect(decodeEnvelope({ ...base, headers: withoutSubject }).key).toBe('order-1');
    expect(decodeEnvelope({ ...base, key: null, headers: withoutSubject }).key).toBeNull();
  });

  it('keeps foreign ce_ attributes as headers and ignores undefined header values', () => {
    const decoded = decodeEnvelope(incoming({ ce_dataschema: 'urn:x', skipped: undefined }));
    expect(decoded.headers).toMatchObject({ ce_dataschema: 'urn:x' });
    expect(decoded.headers).not.toHaveProperty('skipped');
  });

  it('reads headers without a header map', () => {
    expect(() => decodeEnvelope({ key: null, value: Buffer.from('1') })).toThrow(
      InvalidKafkaMessageError,
    );
  });
});

describe('decodeEnvelope strictness', () => {
  it.each([
    ['wrong spec version', { ce_specversion: '0.3' }, /ce_specversion/],
    ['missing id', { ce_id: undefined }, /ce_id/],
    ['empty source', { ce_source: '' }, /ce_source/],
    ['missing type', { ce_type: undefined }, /ce_type/],
    ['invalid type', { ce_type: 'no spaces allowed' }, /not a valid event type/],
    ['time without a zone', { ce_time: '2026-01-01T00:00:00' }, /RFC 3339/],
    ['impossible date', { ce_time: '2026-13-45T00:00:00Z' }, /RFC 3339/],
    ['missing content type', { 'content-type': undefined }, /content-type/],
    ['non-JSON content type', { 'content-type': 'text/plain' }, /content-type/],
    ['repeated header', { ce_id: ['a', 'b'] }, /repeated/],
    ['non UTF-8 protocol header', { ce_id: Buffer.from([0xff, 0xfe]) }, /not valid UTF-8/],
    ['mismatched subject', { ce_subject: 'other' }, /does not match/],
  ])('rejects %s', (_label, override, message) => {
    expect(() => decodeEnvelope(incoming(override as KafkaIncomingHeaders))).toThrow(message);
  });

  it.each([
    ['a missing value', null, /no value/],
    ['a non UTF-8 value', Buffer.from([0xff, 0xfe]), /not valid UTF-8/],
    ['invalid JSON', '{nope', /not valid JSON/],
  ])('rejects %s', (_label, value, message) => {
    expect(() => decodeEnvelope(incoming({}, value))).toThrow(message);
  });

  it('rejects a non UTF-8 key', () => {
    const base = incoming();
    const headers = omitSubject(base.headers);
    expect(() => decodeEnvelope({ ...base, key: Buffer.from([0xff]), headers })).toThrow(
      /Message key is not valid UTF-8/,
    );
  });

  it('decodes foreign headers leniently so a binary trace header cannot poison a record', () => {
    const decoded = decodeEnvelope(incoming({ binary: Buffer.from([0xff, 0x41]) }));
    expect(decoded.headers['binary']).toBe('�A');
  });
});

describe('toOutgoingHeaders', () => {
  it('keeps strings, copies bytes into buffers, keeps lists and drops undefined', () => {
    const outgoing = toOutgoingHeaders({
      text: 'a',
      bytes: new Uint8Array([1, 2]),
      many: ['x', new Uint8Array([3])],
      gone: undefined,
    });
    expect(outgoing).toEqual({
      text: 'a',
      bytes: Buffer.from([1, 2]),
      many: ['x', Buffer.from([3])],
    });
    expect(Buffer.isBuffer(outgoing['bytes'])).toBe(true);
  });

  it('accepts missing headers', () => {
    expect(toOutgoingHeaders(undefined)).toEqual({});
  });
});
