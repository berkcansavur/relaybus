import { describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  WireTypeCollisionError,
  WireTypesUnsupportedError,
  defineCatalog,
  defineEvent,
  produce,
  route,
  type EventEnvelope,
  type InMemoryTransaction,
} from '@relaybus/core';
import { kafkaCodecContract, type KafkaCodecCase } from '@relaybus/testing';
import {
  cloudEventsStructured,
  decodeKafkaRecord,
  jsonEnvelope,
  type KafkaCodec,
  type KafkaJsonEnvelopeMapping,
} from '../src/index.js';
import { createEnvelope } from './fakes.js';
import { setup } from './support/transport-setup.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

const MAPPING: KafkaJsonEnvelopeMapping = {
  type: { body: 'eventName' },
  id: { body: 'messageId' },
  payload: { body: 'data' },
};

const Events = defineCatalog(
  { OrderPlaced, OrderShipped },
  { name: 'Events', wireTypes: { OrderPlaced: 'Order Placed' } },
);

/** The wire names as `jsonEnvelope` has always taken them. */
const EXPLICIT = jsonEnvelope({
  ...MAPPING,
  wireTypes: { 'order.placed.v1': 'Order Placed', 'order.shipped.v1': 'Order Shipped' },
});

const SHIPPED = createEnvelope({
  type: 'order.shipped.v1',
  correlationId: null,
  causationId: null,
  headers: {},
});
const PLACED = createEnvelope({ correlationId: null, causationId: null, headers: {} });

const CASES: KafkaCodecCase[] = [PLACED, SHIPPED].map((envelope) => ({
  name: envelope.type,
  envelope,
  record: {
    topic: 'orders',
    key: 'order-1',
    timestamp: '1767225600000',
    headers: { 'content-type': 'application/json' },
    value: `{"eventName":"${envelope.type === 'order.placed.v1' ? 'Order Placed' : 'Order Shipped'}","messageId":"0190a000-0000-7000-8000-0000000000aa","data":{"orderId":"1"}}`,
  },
}));

/** A bus with a catalog route over a codec with no wire names of its own; the fake producer records the sends. */
function catalogRouteBus(codec: KafkaCodec) {
  const context = setup({ codec });
  const bus = new RelayBus<InMemoryTransaction>({
    store: new InMemoryEventStore(),
    transactionMode: 'optional',
    transports: [context.transport],
    routes: [
      route(produce(Events), {
        transport: 'kafka',
        destination: 'orders',
        wireTypes: { OrderShipped: 'Order Shipped' },
      }),
    ],
    logger: context.logger,
  });
  return { ...context, bus };
}

async function encodeThroughBus(envelope: EventEnvelope) {
  const { bus, producer } = catalogRouteBus(jsonEnvelope(MAPPING));
  await bus.publish(
    new (envelope.type === 'order.placed.v1' ? OrderPlaced : OrderShipped)({ orderId: '1' }),
    {
      key: 'order-1',
      id: envelope.id,
    },
  );
  await bus.drain();
  const [record] = producer.sent;
  return record!.messages[0]!;
}

kafkaCodecContract('wireTypes of jsonEnvelope', {
  encode: EXPLICIT.encode,
  decode: (record) => decodeKafkaRecord(EXPLICIT, record),
  cases: CASES,
});

const DECLARED = jsonEnvelope(MAPPING).withWireTypes!({
  'order.placed.v1': 'Order Placed',
  'order.shipped.v1': 'Order Shipped',
});

kafkaCodecContract('wire names declared by a catalog route (codec)', {
  encode: DECLARED.encode,
  decode: (record) => decodeKafkaRecord(DECLARED, record),
  cases: CASES,
});

describe('catalog routes over a Kafka transport', () => {
  it('writes the bytes wireTypes writes, for what a bus publishes', async () => {
    for (const envelope of [PLACED, SHIPPED]) {
      const sent = await encodeThroughBus(envelope);
      const expected = EXPLICIT.encode(envelope);

      expect(Buffer.compare(Buffer.from(sent.value!), Buffer.from(expected.value as string))).toBe(
        0,
      );
      expect(sent.key).toBe(expected.key);
      expect(sent.headers).toEqual(expected.headers);
    }
  });

  it('reads records under the declared wire names back as event types', async () => {
    const { transport, consumer } = setup({ codec: jsonEnvelope(MAPPING) });
    transport.declareWireTypes({ 'order.placed.v1': 'Order Placed' })();
    const received: EventEnvelope[] = [];
    await transport.subscribe(['orders'], async (envelope) => {
      received.push(envelope);
    });

    await consumer.deliver({
      key: Buffer.from('order-1'),
      value: Buffer.from(CASES[0]!.record.value as string),
      headers: { 'content-type': Buffer.from('application/json') },
      timestamp: '1767225600000',
    });

    expect(received.map(({ type, id }) => `${type} ${id}`)).toEqual([
      'order.placed.v1 0190a000-0000-7000-8000-0000000000aa',
    ]);
  });

  it('keeps the wire names the codec already has', () => {
    const codec = EXPLICIT.withWireTypes!({ 'order.placed.v1': 'Order Placed' });

    expect(codec.encode(PLACED)).toEqual(EXPLICIT.encode(PLACED));
    expect(codec.encode(SHIPPED)).toEqual(EXPLICIT.encode(SHIPPED));
  });

  it('refuses a wire name that clashes with the codec', () => {
    expect(() => EXPLICIT.withWireTypes!({ 'order.placed.v1': 'Placed' })).toThrow(
      WireTypeCollisionError,
    );
    expect(() => EXPLICIT.withWireTypes!({ 'order.audited.v1': 'Order Shipped' })).toThrow(
      "Event types 'order.shipped.v1' and 'order.audited.v1' share the wire name 'Order Shipped' in the codec and a route or catalog",
    );
  });

  it('refuses a codec that has no type to map', () => {
    const fixed = jsonEnvelope({ ...MAPPING, type: { fixed: 'order.placed.v1' } });

    expect(() => fixed.withWireTypes!({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
    expect(() =>
      setup({ codec: fixed }).transport.declareWireTypes({ 'order.placed.v1': 'x' }),
    ).toThrow(WireTypesUnsupportedError);
  });

  it.each([
    ['the default codec', undefined],
    ['cloudEventsStructured', cloudEventsStructured],
  ])('refuses wire names for %s at boot', (_label, codec) => {
    const error = () => catalogRouteBus(codec as KafkaCodec);

    expect(error).toThrow(WireTypesUnsupportedError);
    expect(error).toThrow(
      "The codec of transport 'kafka' cannot map event types to wire names; use jsonEnvelope with a type at { body } or { header }",
    );
  });

  it('checks the wire names without applying them, and refuses once the transport started', async () => {
    const { transport, producer } = setup({ codec: jsonEnvelope(MAPPING) });

    const apply = transport.declareWireTypes({ 'order.placed.v1': 'Order Placed' });
    await transport.publish(PLACED, 'orders');
    expect(JSON.parse(String(producer.sent[0]!.messages[0]!.value)).eventName).toBe(
      'order.placed.v1',
    );
    apply();
    await transport.publish(PLACED, 'orders');
    expect(JSON.parse(String(producer.sent[1]!.messages[0]!.value)).eventName).toBe('Order Placed');

    expect(() => transport.declareWireTypes({ 'order.placed.v1': 'Order Placed' })).toThrow(
      "Transport 'kafka' already started; declare wire names before it connects or subscribes",
    );
  });

  it('refuses once the transport subscribed', async () => {
    const { transport } = setup({ codec: jsonEnvelope(MAPPING) });
    await transport.subscribe(['orders'], async () => undefined);

    expect(() => transport.declareWireTypes({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
  });

  it('refuses once a connection is pending', () => {
    const { transport } = setup({ codec: jsonEnvelope(MAPPING) });
    void transport.connect();

    expect(() => transport.declareWireTypes({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
  });
});
