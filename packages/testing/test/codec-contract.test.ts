import { describe, expect, it } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';
import {
  kafkaCodecContract,
  kafkaCodecMismatches,
  rabbitMqCodecContract,
  rabbitMqCodecMismatches,
  type KafkaCodecContractOptions,
  type KafkaCodecCase,
  type RabbitMqCodecCase,
  type RabbitMqCodecContractOptions,
} from '../src/index.js';
import { canonicalJson } from '../src/codec-contract/compare.js';

const ENVELOPE: EventEnvelope = {
  id: 'evt-1',
  type: 'order.placed.v1',
  source: 'orders',
  key: 'order-1',
  payload: { orderId: '1', total: 10 },
  headers: {},
  correlationId: null,
  causationId: null,
  occurredAt: new Date('2026-01-01T00:00:00.000Z'),
};

const KAFKA_CASE: KafkaCodecCase = {
  name: 'order placed',
  envelope: ENVELOPE,
  record: {
    topic: 'orders',
    key: 'order-1',
    headers: { 'event-type': 'order.placed.v1' },
    value: '{"orderId":"1","total":10}',
  },
};

/** A codec that writes the payload as the value and the type in a header, and reads it back. */
function kafkaOptions(
  overrides: Partial<KafkaCodecContractOptions> = {},
): KafkaCodecContractOptions {
  return {
    encode: (envelope) => ({
      key: envelope.key,
      value: JSON.stringify(envelope.payload),
      headers: { 'event-type': envelope.type },
    }),
    decode: (record) => ({
      ...ENVELOPE,
      key: record.key === null ? null : record.key.toString(),
      payload: JSON.parse(record.value!.toString()) as unknown,
      type: record.headers['event-type']!.toString(),
    }),
    cases: [KAFKA_CASE],
    ...overrides,
  };
}

describe('kafkaCodecMismatches', () => {
  it('finds none when both directions agree', async () => {
    expect(await kafkaCodecMismatches(kafkaOptions(), KAFKA_CASE)).toEqual([]);
  });

  it('compares the value byte for byte by default and as canonical JSON on request', async () => {
    const reordered: KafkaCodecCase = {
      ...KAFKA_CASE,
      record: { ...KAFKA_CASE.record, value: Buffer.from('{"total":10,"orderId":"1"}') },
    };
    expect(await kafkaCodecMismatches(kafkaOptions(), reordered)).toEqual([
      'value: expected {"total":10,"orderId":"1"}, encoded {"orderId":"1","total":10}',
      'decoded payload: expected {"orderId":"1","total":10}, decoded {"total":10,"orderId":"1"} (same values, other key order)',
    ]);
    expect(await kafkaCodecMismatches(kafkaOptions({ compare: 'json' }), reordered)).toEqual([]);
  });

  it('reports a different key, headers and timestamp', async () => {
    const options = kafkaOptions({
      encode: () => ({
        key: Buffer.from('other'),
        value: '{"orderId":"1","total":10}',
        headers: { 'event-type': ['order.placed.v1'], extra: Buffer.from('x') },
        timestamp: '5',
      }),
    });
    const mismatches = await kafkaCodecMismatches(options, {
      ...KAFKA_CASE,
      record: { ...KAFKA_CASE.record, timestamp: '6' },
    });
    expect(mismatches).toEqual([
      'key: expected order-1, encoded other',
      'headers: expected {"event-type":"order.placed.v1"}, encoded {"event-type":"order.placed.v1","extra":"x"}',
      'timestamp: expected 6, encoded 5',
    ]);
  });

  it('ignores the listed headers and a timestamp the codec does not set', async () => {
    const options = kafkaOptions({ ignoreHeaders: ['relaybus-event-id'] });
    const withExtraHeader: KafkaCodecCase = {
      ...KAFKA_CASE,
      record: {
        ...KAFKA_CASE.record,
        headers: { ...KAFKA_CASE.record.headers, 'relaybus-event-id': 'evt-1' },
        timestamp: '9',
      },
    };
    expect(await kafkaCodecMismatches(options, withExtraHeader)).toEqual([]);
  });

  it('reports a decoded event that differs, and compares with the expected decoded event', async () => {
    const options = kafkaOptions({ decode: () => ({ ...ENVELOPE, key: null }) });
    expect(await kafkaCodecMismatches(options, KAFKA_CASE)).toEqual([
      expect.stringMatching(/^decoded: expected \{.*"key":"order-1".*\}, decoded \{.*"key":null/),
    ]);
    expect(
      await kafkaCodecMismatches(options, { ...KAFKA_CASE, decoded: { ...ENVELOPE, key: null } }),
    ).toEqual([]);
  });

  it('hands the decoder bytes and defaults, and compares null values and keys', async () => {
    const seen: unknown[] = [];
    const options = kafkaOptions({
      encode: () => ({ key: null, value: null, headers: {} }),
      decode: (record) => {
        seen.push(record);
        return ENVELOPE;
      },
    });
    const empty: KafkaCodecCase = {
      name: 'tombstone',
      envelope: ENVELOPE,
      record: { topic: 'orders', key: null, value: null },
    };
    expect(await kafkaCodecMismatches(options, empty)).toEqual([]);
    expect(await kafkaCodecMismatches({ ...options, compare: 'json' }, empty)).toEqual([]);
    expect(
      await kafkaCodecMismatches(options, { ...empty, record: { ...empty.record, value: '{}' } }),
    ).toEqual(['value: expected {}, encoded null']);
    expect(seen[0]).toEqual({
      topic: 'orders',
      partition: 0,
      offset: '0',
      key: null,
      value: null,
      headers: {},
    });
  });

  it('reports what encode and decode throw', async () => {
    const options = kafkaOptions({
      encode: () => {
        throw new Error('cannot encode');
      },
      decode: async () => Promise.reject(new Error('cannot decode')),
    });
    expect(await kafkaCodecMismatches(options, KAFKA_CASE)).toEqual([
      'encode threw: cannot encode',
      'decode threw: cannot decode',
    ]);
  });
});

describe('one-way cases', () => {
  const broken = kafkaOptions({
    encode: () => ({ key: 'x', value: 'x', headers: {} }),
    decode: () => ({ ...ENVELOPE, id: 'other' }),
  });

  it("checks only decoding for 'decode' and only encoding for 'encode'", async () => {
    expect(await kafkaCodecMismatches(broken, { ...KAFKA_CASE, only: 'decode' })).toEqual([
      expect.stringMatching(/^decoded: /),
    ]);
    expect(await kafkaCodecMismatches(broken, { ...KAFKA_CASE, only: 'encode' })).toHaveLength(3);
    const rabbit = rabbitOptions({ decode: () => ({ ...ENVELOPE, id: 'other' }) });
    expect(await rabbitMqCodecMismatches(rabbit, { ...RABBIT_CASE, only: 'encode' })).toEqual([]);
    expect(await rabbitMqCodecMismatches(rabbit, { ...RABBIT_CASE, only: 'decode' })).toHaveLength(
      1,
    );
  });
});

kafkaCodecContract('the test codec', kafkaOptions());

const RABBIT_CASE: RabbitMqCodecCase = {
  name: 'order placed',
  envelope: ENVELOPE,
  message: {
    routingKey: 'orders',
    properties: { messageId: 'evt-1', contentType: 'application/json' },
    headers: { 'event-type': 'order.placed.v1' },
    body: '{"orderId":"1","total":10}',
  },
};

function rabbitOptions(
  overrides: Partial<RabbitMqCodecContractOptions> = {},
): RabbitMqCodecContractOptions {
  return {
    encode: (envelope) => ({
      content: Buffer.from(JSON.stringify(envelope.payload)),
      options: {
        messageId: envelope.id,
        contentType: 'application/json',
        headers: { 'event-type': envelope.type },
      },
    }),
    decode: (message) => ({
      ...ENVELOPE,
      source: message.fields.routingKey,
      payload: JSON.parse(message.content.toString()) as unknown,
      type: String(message.properties.headers['event-type']),
    }),
    cases: [RABBIT_CASE],
    ...overrides,
  };
}

describe('rabbitMqCodecMismatches', () => {
  it('finds none when both directions agree', async () => {
    expect(await rabbitMqCodecMismatches(rabbitOptions(), RABBIT_CASE)).toEqual([]);
  });

  it('reports different properties, headers and body', async () => {
    const options = rabbitOptions({
      encode: () => ({
        content: Buffer.from('{"total":10,"orderId":"1"}'),
        options: { messageId: 'other', contentType: 'application/json', persistent: true },
      }),
    });
    expect(await rabbitMqCodecMismatches(options, RABBIT_CASE)).toEqual([
      'properties: expected {"contentType":"application/json","messageId":"evt-1"}, encoded {"contentType":"application/json","messageId":"other","persistent":true}',
      'headers: expected {"event-type":"order.placed.v1"}, encoded {}',
      'body: expected {"orderId":"1","total":10}, encoded {"total":10,"orderId":"1"}',
    ]);
  });

  it('ignores the listed properties and headers and compares JSON bodies canonically', async () => {
    const options = rabbitOptions({
      compare: 'json',
      ignoreProperties: ['persistent', 'timestamp'],
      ignoreHeaders: ['x-relaybus-attempt'],
      encode: () => ({
        content: Buffer.from('{"total":10,"orderId":"1"}'),
        options: {
          messageId: 'evt-1',
          contentType: 'application/json',
          persistent: true,
          timestamp: 1,
          headers: { 'event-type': 'order.placed.v1', 'x-relaybus-attempt': 1 },
        },
      }),
    });
    expect(await rabbitMqCodecMismatches(options, RABBIT_CASE)).toEqual([]);
  });

  it('hands the decoder the message as a consumer receives it, and reports a throwing decode', async () => {
    const seen: unknown[] = [];
    const options = rabbitOptions({
      decode: (message) => {
        seen.push(message);
        throw new Error('cannot decode');
      },
    });
    const bare: RabbitMqCodecCase = {
      name: 'bare',
      envelope: ENVELOPE,
      message: { routingKey: 'orders', body: Buffer.from('{}') },
    };
    expect(await rabbitMqCodecMismatches(options, bare)).toContain('decode threw: cannot decode');
    expect(seen[0]).toEqual({
      content: Buffer.from('{}'),
      fields: { routingKey: 'orders' },
      properties: { headers: {} },
    });
  });

  it('reports what encode throws, and a decoded event that differs', async () => {
    const options = rabbitOptions({
      encode: () => {
        throw new Error('cannot encode');
      },
      decode: () => ({ ...ENVELOPE, id: 'evt-2' }),
    });
    const mismatches = await rabbitMqCodecMismatches(options, RABBIT_CASE);
    expect(mismatches[0]).toBe('encode threw: cannot encode');
    expect(mismatches[1]).toMatch(/^decoded: expected .*"id":"evt-1".*, decoded .*"id":"evt-2"/);
  });
});

rabbitMqCodecContract('the test codec', rabbitOptions());

describe('canonicalJson', () => {
  it('sorts keys at every level, inside arrays too, and writes undefined as null', () => {
    expect(canonicalJson({ b: [{ d: 1, c: 2 }], a: 1 })).toBe('{"a":1,"b":[{"c":2,"d":1}]}');
    expect(canonicalJson(undefined)).toBe('null');
  });
});
