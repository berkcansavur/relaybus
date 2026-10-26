import { describe, expect, it } from 'vitest';
import { ConfigurationError, type EventEnvelope } from '@relaybus/core';
import { kafkaCodecContract } from '@relaybus/testing';
import { FakeProducer, createLogger } from './fakes.js';
import {
  KafkaTransport,
  decodeKafkaRecord,
  jsonEnvelope,
  type KafkaCodec,
  type KafkaJsonEnvelopeMapping,
} from '../src/index.js';

/**
 * A legacy flat JSON envelope: one topic carries many event types; existing producers write
 * no record key and no headers, and the value is
 * `{ id?, requestId?, provider, timestamp (epoch ms), createdDate (ISO), type, data, ...headers }`.
 */
const LEGACY_FLAT: KafkaJsonEnvelopeMapping = {
  type: { body: 'type' },
  id: { body: 'id', whenMissing: 'record-position' },
  payload: { body: 'data' },
  source: { body: 'provider' },
  occurredAt: { body: 'timestamp', format: 'epoch-millis' },
  correlationId: { body: 'requestId' },
  headers: 'top-level-fields',
  extra: { body: { createdDate: (event) => event.occurredAt.toISOString() } },
  bodyOrder: ['id', 'requestId', 'provider', 'timestamp', 'createdDate', 'type', 'data'],
  recordKey: 'none',
  contentTypeHeader: false,
};

const codec: KafkaCodec = jsonEnvelope(LEGACY_FLAT);

const EVENT: EventEnvelope = {
  id: 'evt-1',
  type: 'order.created',
  source: 'billing-api',
  key: 'order-1',
  payload: { orderId: 1, total: 10.5 },
  headers: { userId: 'u-7', channel: 'web' },
  correlationId: 'req-1',
  causationId: null,
  occurredAt: new Date('2026-10-25T14:00:00.123Z'),
};

kafkaCodecContract('a legacy flat JSON envelope', {
  encode: codec.encode,
  decode: (record) => decodeKafkaRecord(codec, record),
  cases: [
    {
      name: 'an event relaybus writes: no key, no headers, the flat value byte for byte',
      envelope: EVENT,
      record: {
        topic: 'orders',
        key: null,
        value:
          '{"id":"evt-1","requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.created","data":{"orderId":1,"total":10.5},"userId":"u-7","channel":"web"}',
      },
      // The wire has no key: the event comes back without one.
      decoded: { ...EVENT, key: null },
    },
    {
      name: 'a record of an existing producer: no id, no requestId, unknown top-level fields',
      only: 'decode',
      envelope: {
        ...EVENT,
        id: 'kafka:orders:3:812',
        key: null,
        correlationId: null,
        headers: { userId: 'u-9', region: 'eu' },
        type: 'order.cancelled',
        payload: { orderId: 2 },
      },
      record: {
        topic: 'orders',
        partition: 3,
        offset: '812',
        key: null,
        value:
          '{"provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.cancelled","data":{"orderId":2},"userId":"u-9","region":"eu","attempt":2,"meta":{"v":1}}',
      },
    },
  ],
});

/** What a transport with `codec` sends, captured by an injected producer. */
async function publishedRecord(envelope: EventEnvelope) {
  const producer = new FakeProducer();
  const transport = new KafkaTransport({ producer, codec, logger: createLogger() });
  await transport.publish(envelope, 'orders');
  await transport.close();
  return producer.sent[0]!.messages[0]!;
}

kafkaCodecContract('a legacy flat JSON envelope, as the transport publishes it', {
  encode: publishedRecord,
  decode: (record) => decodeKafkaRecord(codec, record),
  cases: [
    {
      name: 'an event relaybus writes',
      envelope: EVENT,
      record: {
        topic: 'orders',
        key: null,
        value:
          '{"id":"evt-1","requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.created","data":{"orderId":1,"total":10.5},"userId":"u-7","channel":"web"}',
      },
      decoded: { ...EVENT, key: null },
    },
  ],
});

/** The same envelope, keeping the top-level fields that are not text under `payload.wire`. */
const keeping: KafkaCodec = jsonEnvelope({ ...LEGACY_FLAT, otherFields: { payloadField: 'wire' } });

kafkaCodecContract('a legacy flat JSON envelope that keeps every top-level field', {
  encode: keeping.encode,
  decode: (record) => decodeKafkaRecord(keeping, record),
  cases: [
    {
      name: 'numbers, booleans, null, objects and arrays at the top level, byte for byte both ways',
      envelope: {
        ...EVENT,
        payload: { orderId: 1, wire: { attempt: 2, retried: true, meta: { v: 1, a: [null] } } },
      },
      record: {
        topic: 'orders',
        key: null,
        value:
          '{"id":"evt-1","requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.created","data":{"orderId":1},"userId":"u-7","channel":"web","attempt":2,"retried":true,"meta":{"v":1,"a":[null]}}',
      },
      decoded: {
        ...EVENT,
        key: null,
        payload: { orderId: 1, wire: { attempt: 2, retried: true, meta: { v: 1, a: [null] } } },
      },
    },
    {
      name: 'a record of an existing producer: the unknown fields that are not text',
      only: 'decode',
      envelope: {
        ...EVENT,
        id: 'kafka:orders:3:812',
        key: null,
        correlationId: null,
        headers: { userId: 'u-9', region: 'eu' },
        type: 'order.cancelled',
        payload: { orderId: 2, wire: { attempt: 2, meta: { v: 1 } } },
      },
      record: {
        topic: 'orders',
        partition: 3,
        offset: '812',
        key: null,
        value:
          '{"provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.cancelled","data":{"orderId":2},"userId":"u-9","region":"eu","attempt":2,"meta":{"v":1}}',
      },
    },
  ],
});

/** requestId travels as an event header spread over the top level, at its bodyOrder position. */
const headerFirst: KafkaCodec = jsonEnvelope({
  type: { body: 'type' },
  id: { body: 'id', whenMissing: 'record-position' },
  payload: { body: 'data' },
  source: { body: 'provider' },
  occurredAt: { body: 'timestamp', format: 'epoch-millis' },
  headers: 'top-level-fields',
  extra: { body: { createdDate: (event) => event.occurredAt.toISOString() } },
  bodyOrder: ['id', 'requestId', 'provider', 'timestamp', 'createdDate', 'type', 'data'],
  recordKey: 'none',
  contentTypeHeader: false,
});

kafkaCodecContract('a legacy flat JSON envelope whose bodyOrder names a spread header', {
  encode: headerFirst.encode,
  decode: (record) => decodeKafkaRecord(headerFirst, record),
  cases: [
    {
      name: 'the requestId header is written second, as bodyOrder says',
      envelope: { ...EVENT, correlationId: null, headers: { userId: 'u-7', requestId: 'req-1' } },
      record: {
        topic: 'orders',
        key: null,
        value:
          '{"id":"evt-1","requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order.created","data":{"orderId":1,"total":10.5},"userId":"u-7"}',
      },
      decoded: {
        ...EVENT,
        key: null,
        correlationId: null,
        headers: { userId: 'u-7', requestId: 'req-1' },
      },
    },
  ],
});

/** One route for many types, named on the wire as the existing producers name them. */
const named: KafkaCodec = jsonEnvelope({
  ...LEGACY_FLAT,
  wireTypes: { 'order.created': 'ORDER CREATED', 'order.cancelled': 'order_cancelled' },
});

kafkaCodecContract('a legacy flat JSON envelope with wire type names', {
  encode: named.encode,
  decode: (record) => decodeKafkaRecord(named, record),
  cases: [
    {
      name: 'a listed type travels under its wire name',
      envelope: EVENT,
      record: {
        topic: 'orders',
        key: null,
        value:
          '{"id":"evt-1","requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"ORDER CREATED","data":{"orderId":1,"total":10.5},"userId":"u-7","channel":"web"}',
      },
      decoded: { ...EVENT, key: null },
    },
    {
      name: 'a snake_case wire name reads back as its event type',
      envelope: { ...EVENT, type: 'order.cancelled' },
      record: {
        topic: 'orders',
        key: null,
        value:
          '{"id":"evt-1","requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"createdDate":"2026-10-25T14:00:00.123Z","type":"order_cancelled","data":{"orderId":1,"total":10.5},"userId":"u-7","channel":"web"}',
      },
      decoded: { ...EVENT, type: 'order.cancelled', key: null },
    },
  ],
});

describe('record key', () => {
  const base: KafkaJsonEnvelopeMapping = {
    type: { body: 'type' },
    id: { body: 'id' },
    payload: { body: 'data' },
  };

  it("keeps the event key by default and with 'event-key'", () => {
    expect(jsonEnvelope(base).encode(EVENT).key).toBe('order-1');
    expect(jsonEnvelope({ ...base, recordKey: 'event-key' }).encode(EVENT).key).toBe('order-1');
  });

  it("writes no key with 'none' and a derived one with a function", () => {
    expect(jsonEnvelope({ ...base, recordKey: 'none' }).encode(EVENT).key).toBeNull();
    const derived = jsonEnvelope({ ...base, recordKey: (event) => `${event.type}:${event.key}` });
    expect(derived.encode(EVENT).key).toBe('order.created:order-1');
    expect(jsonEnvelope({ ...base, recordKey: () => null }).encode(EVENT).key).toBeNull();
  });

  it('writes the content-type header unless told not to', () => {
    expect(jsonEnvelope(base).encode(EVENT).headers).toMatchObject({
      'content-type': 'application/json',
    });
    expect(
      jsonEnvelope({ ...base, contentTypeHeader: false }).encode(EVENT).headers,
    ).not.toHaveProperty('content-type');
  });

  it('refuses an unknown record key option', () => {
    expect(() => jsonEnvelope({ ...base, recordKey: 'random' as 'none' })).toThrow(
      ConfigurationError,
    );
  });
});
