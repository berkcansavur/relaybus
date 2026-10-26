import { describe, expect, it } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';
import { kafkaCodecContract } from '@relaybus/testing';
import {
  cloudEventsBinary,
  decodeKafkaRecord,
  jsonEnvelope,
  type KafkaCodec,
} from '../src/index.js';
import { createEnvelope } from './fakes.js';

/**
 * An in-house format as company services write it: type and id in headers, the payload and
 * metadata nested in the body, the time as epoch milliseconds, a constant producer name and a
 * schema-version header.
 */
const IN_HOUSE: KafkaCodec = jsonEnvelope({
  type: { header: 'x-event-type' },
  id: { header: 'x-message-id' },
  payload: { body: 'payload' },
  source: { body: 'meta.producer' },
  occurredAt: { body: 'meta.sentAt', format: 'epoch-millis' },
  correlationId: { body: 'meta.traceId' },
  headers: { body: 'meta.headers' },
  extra: {
    body: {
      'meta.schema': 'orders/v1',
      'meta.sentAtIso': (event) => event.occurredAt.toISOString(),
    },
    headers: { 'x-schema-version': '1' },
  },
  bodyOrder: ['meta', 'payload'],
});

const IN_HOUSE_EVENT: EventEnvelope = createEnvelope({
  causationId: null,
  headers: { tenant: 'tr' },
});

const decoder = (codec: KafkaCodec) => (record: Parameters<typeof decodeKafkaRecord>[1]) =>
  decodeKafkaRecord(codec, record);

kafkaCodecContract('an in-house JSON format', {
  encode: IN_HOUSE.encode,
  decode: decoder(IN_HOUSE),
  cases: [
    {
      name: 'order placed',
      envelope: IN_HOUSE_EVENT,
      record: {
        topic: 'orders',
        key: 'order-1',
        headers: {
          'x-event-type': 'order.placed.v1',
          'x-message-id': '0190a000-0000-7000-8000-0000000000aa',
          'x-schema-version': '1',
          'content-type': 'application/json',
        },
        value:
          '{"meta":{"producer":"orders","sentAt":1767225600000,"traceId":"flow-1","headers":{"tenant":"tr"},"schema":"orders/v1","sentAtIso":"2026-01-01T00:00:00.000Z"},"payload":{"orderId":"1"}}',
      },
    },
  ],
});

kafkaCodecContract('CloudEvents binary mode', {
  encode: cloudEventsBinary.encode,
  decode: decoder(cloudEventsBinary),
  compare: 'json',
  cases: [
    {
      name: 'order placed',
      envelope: createEnvelope({ causationId: null, headers: {} }),
      record: {
        topic: 'orders',
        key: 'order-1',
        headers: {
          ce_specversion: '1.0',
          ce_id: '0190a000-0000-7000-8000-0000000000aa',
          ce_type: 'order.placed.v1',
          ce_source: 'orders',
          ce_time: '2026-01-01T00:00:00.000Z',
          ce_subject: 'order-1',
          ce_correlationid: 'flow-1',
          'content-type': 'application/json',
        },
        value: '{ "orderId": "1" }',
      },
    },
  ],
});

describe('decodeKafkaRecord', () => {
  it('fills what a codec leaves out the way the transport does', () => {
    const plain = jsonEnvelope({
      type: { header: 'event-type' },
      id: 'record-position',
      payload: 'whole-body',
    });
    const event = decodeKafkaRecord(plain, {
      topic: 'orders',
      partition: 2,
      offset: '41',
      key: Buffer.from('order-9'),
      value: Buffer.from('{"orderId":"9"}'),
      headers: { 'event-type': Buffer.from('order.placed.v1') },
      timestamp: '1767225600000',
    });
    expect(event).toEqual({
      id: 'kafka:orders:2:41',
      type: 'order.placed.v1',
      source: 'orders',
      key: 'order-9',
      payload: { orderId: '9' },
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    });
  });
});
