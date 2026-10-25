import { describe, expect, it } from 'vitest';
import { type EventEnvelope } from '@relaybus/core';
import { createJsonEnvelopeFormat, type JsonEnvelopeMapping } from '@relaybus/core/internal';

// Golden output of the jsonEnvelope format, recorded before the 0.3.1 readability refactor: every
// write, read and refusal below must stay byte for byte the same.

type Mapping = JsonEnvelopeMapping<'transport-id'>;

const failures = {
  invalid: (reason: string) => new Error(`invalid: ${reason}`),
  unencodable: (reason: string) => new Error(`unencodable: ${reason}`),
};

const MAPPINGS: Record<string, Mapping> = {
  bodyEnvelope: {
    type: { body: 'eventName' },
    id: { body: 'messageId' },
    payload: { body: 'data' },
    key: { body: 'meta.key' },
    source: { body: 'meta.source' },
    occurredAt: { body: 'meta.occurredAt' },
    correlationId: { body: 'meta.correlationId' },
    causationId: { body: 'meta.causationId' },
    headers: { body: 'meta.headers' },
  },
  arrayIndexes: {
    type: { body: 'messageType.0' },
    id: { body: 'ids.0' },
    payload: { body: 'message' },
    key: { body: 'ids.1' },
  },
  headerFields: {
    type: { header: 'x-type' },
    id: 'transport-id',
    payload: { body: 'payload' },
    source: { header: 'x-source' },
    occurredAt: { header: 'x-time' },
    correlationId: { header: 'x-correlation' },
  },
  payloadOnly: {
    type: { fixed: 'order.placed.v1' },
    id: { header: 'x-id' },
    payload: 'whole-body',
    key: { header: 'x-key' },
    causationId: { header: 'x-cause' },
  },
};

function envelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: 'evt-1',
    type: 'order.placed.v1',
    source: 'orders',
    key: 'order-1',
    payload: { orderId: '1', lines: [{ sku: 'a', quantity: 2 }] },
    headers: { traceparent: '00-abc-def-01', tenant: 't-1' },
    correlationId: 'flow-1',
    causationId: 'cause-1',
    occurredAt: new Date('2026-01-01T10:20:30.456Z'),
    ...overrides,
  };
}

const ENVELOPES: Record<string, EventEnvelope> = {
  full: envelope(),
  unkeyed: envelope({ key: null, causationId: null, headers: {} }),
  protoField: envelope({ payload: { ['__proto__']: { polluted: true } } }),
  otherType: envelope({ type: 'order.shipped.v1' }),
  headerClash: envelope({ headers: { 'x-type': 'clash', 'x-id': 'clash' } }),
  badTime: envelope({ occurredAt: new Date(Number.NaN) }),
};

function attempt(work: () => unknown): unknown {
  try {
    return { value: work() };
  } catch (error) {
    return { error: (error as Error).message };
  }
}

describe('jsonEnvelope (characterisation)', () => {
  it('writes and reads every envelope with every mapping the same way', () => {
    const golden: Record<string, unknown> = {};
    for (const [mappingName, mapping] of Object.entries(MAPPINGS)) {
      const format = createJsonEnvelopeFormat(mapping, {
        transportIds: ['transport-id'],
        failures,
      });
      for (const [envelopeName, event] of Object.entries(ENVELOPES)) {
        const written = attempt(() => format.write(event));
        const read =
          'value' in (written as object)
            ? attempt(() => format.read((written as { value: never }).value))
            : undefined;
        golden[`${mappingName} / ${envelopeName}`] = {
          written: JSON.stringify(written),
          read: JSON.stringify(read),
        };
      }
    }
    expect(golden).toMatchSnapshot();
  });

  it('reads malformed messages with the same refusals', () => {
    const messages: Record<string, { body: unknown; headers: Record<string, string> }> = {
      notAnObject: { body: [1, 2], headers: {} },
      noType: { body: { messageId: 'm', data: {} }, headers: {} },
      badType: { body: { eventName: 'Not A Type', messageId: 'm', data: {} }, headers: {} },
      numericId: { body: { eventName: 'a.b', messageId: 42, data: {} }, headers: {} },
      objectId: { body: { eventName: 'a.b', messageId: { x: 1 }, data: {} }, headers: {} },
      noPayload: { body: { eventName: 'a.b', messageId: 'm' }, headers: {} },
      badTime: {
        body: { eventName: 'a.b', messageId: 'm', data: {}, meta: { occurredAt: 'yesterday' } },
        headers: {},
      },
      emptyKey: {
        body: { eventName: 'a.b', messageId: 'm', data: {}, meta: { key: '', headers: [] } },
        headers: { other: 'x' },
      },
    };
    const format = createJsonEnvelopeFormat(MAPPINGS['bodyEnvelope']!, {
      transportIds: ['transport-id'],
      failures,
    });
    const golden = Object.fromEntries(
      Object.entries(messages).map(([name, message]) => [
        name,
        JSON.stringify(attempt(() => format.read(message))),
      ]),
    );
    expect(golden).toMatchSnapshot();
  });

  it('refuses the same mappings with the same messages', () => {
    const mappings: Record<string, unknown> = {
      notAnObject: 'mapping',
      noType: { id: 'transport-id', payload: 'whole-body' },
      badFixedType: { type: { fixed: 'Bad Type' }, id: 'transport-id', payload: 'whole-body' },
      noId: { type: { fixed: 'a.b' }, payload: 'whole-body' },
      noPayload: { type: { fixed: 'a.b' }, id: 'transport-id' },
      badHeaders: {
        type: { fixed: 'a.b' },
        id: 'transport-id',
        payload: { body: 'd' },
        headers: 1,
      },
      badLocation: { type: { nowhere: 1 }, id: 'transport-id', payload: 'whole-body' },
      emptySegment: { type: { body: 'a..b' }, id: 'transport-id', payload: { body: 'd' } },
      headerTwice: { type: { header: 'h' }, id: { header: 'h' }, payload: 'whole-body' },
      nested: { type: { body: 'meta' }, id: { body: 'meta.id' }, payload: { body: 'd' } },
      mixed: { type: { body: 'm.0' }, id: { body: 'm.name' }, payload: { body: 'd' } },
      same: { type: { body: 'd' }, id: 'transport-id', payload: { body: 'd' } },
    };
    const golden = Object.fromEntries(
      Object.entries(mappings).map(([name, mapping]) => [
        name,
        JSON.stringify(
          attempt(() =>
            createJsonEnvelopeFormat(mapping as Mapping, {
              transportIds: ['transport-id', 'other-id'],
              failures,
            }),
          ),
        ),
      ]),
    );
    expect(golden).toMatchSnapshot();
  });
});
