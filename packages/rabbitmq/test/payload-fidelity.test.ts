import { describe, expect, it } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';
import {
  cloudEventsBinary,
  cloudEventsStructured,
  decodeRabbitMqMessage,
  jsonEnvelope,
  type RabbitMqCodec,
} from '../src/index.js';
import { createEnvelope } from './fakes.js';
import { createTransport } from './support/transport-setup.js';

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

const CODECS: readonly [string, RabbitMqCodec][] = [
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
      id: 'message-id-property',
      payload: 'whole-body',
    }),
  ],
];

describe.each(CODECS)('payload fidelity of %s on RabbitMQ', (_name, codec) => {
  const event: EventEnvelope = createEnvelope({
    payload: PAYLOAD,
    headers: { zz: 'last', a: 'first' },
  });

  /** The message as a consumer receives it. */
  function delivered(encoded: ReturnType<RabbitMqCodec['encode']>) {
    return {
      content: Buffer.from(encoded.content),
      fields: { routingKey: 'orders' },
      properties: { ...encoded.options, headers: { ...encoded.options.headers } },
    };
  }

  it('writes the payload as JSON.stringify writes it, key order included', () => {
    expect(Buffer.from(codec.encode(event).content).toString()).toContain(JSON.stringify(PAYLOAD));
  });

  it('reads it back with the same order, and writes the read event byte for byte again', () => {
    const encoded = codec.encode(event);
    const decoded = decodeRabbitMqMessage(codec, delivered(encoded));
    expect(JSON.stringify(decoded.payload)).toBe(JSON.stringify(PAYLOAD));
    expect(Buffer.from(codec.encode({ ...decoded, occurredAt: event.occurredAt }).content)).toEqual(
      Buffer.from(encoded.content),
    );
  });

  it('publishes the same bytes through the transport as the codec writes', async () => {
    const { transport, connections } = createTransport({ codec });
    await transport.connect();
    await transport.publish(event, 'orders');
    expect(connections[0]!.publisher.published[0]!.content).toEqual(
      Buffer.from(codec.encode(event).content),
    );
    await transport.close();
  });
});
