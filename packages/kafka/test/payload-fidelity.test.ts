import { describe, expect, it } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';
import {
  KafkaTransport,
  cloudEventsBinary,
  cloudEventsStructured,
  decodeKafkaRecord,
  jsonEnvelope,
  type KafkaCodec,
  type KafkaEncodedRecord,
} from '../src/index.js';
import { FakeProducer, createEnvelope, createLogger } from './fakes.js';

/** Keys out of sorted order at every depth, unicode, exponent numbers and empty containers. */
const PAYLOAD = {
  zulu: 1,
  b: { yankee: [{ zz: 1, a: { q: null, c: true } }, [], {}], alpha: {} },
  a: 'çğıöşü 🚀 "quoted"',
  exponent: 1e21,
  maxSafe: Number.MAX_SAFE_INTEGER,
  fraction: 0.1 + 0.2,
  '7': 'integer-like key',
};

const CODECS: readonly [string, KafkaCodec][] = [
  ['cloudEventsBinary', cloudEventsBinary],
  ['cloudEventsStructured', cloudEventsStructured],
  [
    'jsonEnvelope',
    jsonEnvelope({
      type: { body: 'eventName' },
      id: { body: 'messageId' },
      payload: { body: 'data' },
    }),
  ],
  [
    'jsonEnvelope with the whole body',
    jsonEnvelope({
      type: { header: 'event-type' },
      id: { header: 'message-id' },
      payload: 'whole-body',
    }),
  ],
];

/** The record as a consumer receives it. */
function delivered(record: KafkaEncodedRecord) {
  return {
    topic: 'orders',
    partition: 0,
    offset: '1',
    key: record.key === null ? null : Buffer.from(record.key),
    value: Buffer.from(record.value as string),
    headers: Object.fromEntries(
      Object.entries(record.headers).map(([name, value]) => [name, Buffer.from(value as string)]),
    ),
    timestamp: '1767225600000',
  };
}

describe.each(CODECS)('payload fidelity of %s on Kafka', (_name, codec) => {
  const event: EventEnvelope = createEnvelope({
    payload: PAYLOAD,
    headers: { zz: 'last', a: 'first' },
  });

  it('writes the payload as JSON.stringify writes it, key order included', () => {
    expect(String(codec.encode(event).value)).toContain(JSON.stringify(PAYLOAD));
  });

  it('reads it back with the same order, and writes the read event byte for byte again', () => {
    const record = codec.encode(event);
    const decoded = decodeKafkaRecord(codec, delivered(record));
    expect(JSON.stringify(decoded.payload)).toBe(JSON.stringify(PAYLOAD));
    expect(String(codec.encode({ ...decoded, occurredAt: event.occurredAt }).value)).toBe(
      String(record.value),
    );
  });

  it('sends the same bytes through the transport as the codec writes', async () => {
    const producer = new FakeProducer();
    const transport = new KafkaTransport({ producer, codec, logger: createLogger() });
    await transport.publish(event, 'orders');
    await transport.close();
    expect(String(producer.sent[0]!.messages[0]!.value)).toBe(String(codec.encode(event).value));
  });
});
