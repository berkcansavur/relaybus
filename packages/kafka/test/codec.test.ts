import { describe, expect, it } from 'vitest';
import {
  InvalidKafkaMessageError,
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
    ['missing time', { ce_time: undefined }, /ce_time/],
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
