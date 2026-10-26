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
import { rabbitMqCodecContract, type RabbitMqCodecCase } from '@relaybus/testing';
import {
  cloudEventsStructured,
  decodeRabbitMqMessage,
  jsonEnvelope,
  type RabbitMqCodec,
  type RabbitMqJsonEnvelopeMapping,
} from '../src/index.js';
import { createEnvelope, waitFor } from './fakes.js';
import { createTransport } from './support/transport-setup.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

const MAPPING: RabbitMqJsonEnvelopeMapping = {
  type: { body: 'eventName' },
  id: 'message-id-property',
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

const PLACED = createEnvelope({
  key: null,
  source: 'orders',
  correlationId: null,
  causationId: null,
  headers: {},
  occurredAt: new Date('2026-01-01T10:20:30.000Z'),
});
const SHIPPED = { ...PLACED, type: 'order.shipped.v1' };

const CASES: RabbitMqCodecCase[] = [PLACED, SHIPPED].map((envelope) => ({
  name: envelope.type,
  envelope,
  message: {
    routingKey: 'orders',
    properties: {
      persistent: true,
      messageId: envelope.id,
      timestamp: 1767262830,
      type: envelope.type,
      appId: 'orders',
      contentType: 'application/json',
    },
    headers: {},
    body: `{"eventName":"${envelope.type === 'order.placed.v1' ? 'Order Placed' : 'Order Shipped'}","data":{"orderId":"1"}}`,
  },
}));

/** A bus with a catalog route over a codec with no wire names of its own. */
function catalogRouteBus(codec: RabbitMqCodec | undefined) {
  const context = createTransport({ ...(codec === undefined ? {} : { codec }) });
  const bus = new RelayBus<InMemoryTransaction>({
    store: new InMemoryEventStore(),
    transactionMode: 'optional',
    transports: [context.transport],
    routes: [
      route(produce(Events), {
        transport: 'rabbitmq',
        destination: 'orders',
        wireTypes: { OrderShipped: 'Order Shipped' },
      }),
    ],
    logger: context.logger,
  });
  return { ...context, bus };
}

rabbitMqCodecContract('wireTypes of jsonEnvelope', {
  encode: EXPLICIT.encode,
  decode: (message) => decodeRabbitMqMessage(EXPLICIT, message),
  cases: CASES,
});

const DECLARED = jsonEnvelope(MAPPING).withWireTypes!({
  'order.placed.v1': 'Order Placed',
  'order.shipped.v1': 'Order Shipped',
});

rabbitMqCodecContract('wire names declared by a catalog route (codec)', {
  encode: DECLARED.encode,
  decode: (message) => decodeRabbitMqMessage(DECLARED, message),
  cases: CASES,
});

describe('catalog routes over a RabbitMQ transport', () => {
  it('writes the bytes wireTypes writes, for what a bus publishes', async () => {
    for (const event of [new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })]) {
      const { bus, connections } = catalogRouteBus(jsonEnvelope(MAPPING));
      await bus.start();
      await bus.publish(event, { id: PLACED.id });
      await bus.drain();
      const [sent] = connections[0]!.publisher.published;
      const expected = EXPLICIT.encode({ ...PLACED, type: event.type, key: null });

      expect(sent!.routingKey).toBe('orders');
      expect(Buffer.compare(sent!.content, expected.content)).toBe(0);
      expect(sent!.options.headers).toEqual(expected.options.headers);
      await bus.stop();
    }
  });

  it('reads messages under the declared wire names back as event types', async () => {
    const { transport, connections } = createTransport({ codec: jsonEnvelope(MAPPING) });
    transport.declareWireTypes({ 'order.placed.v1': 'Order Placed' })();
    const received: EventEnvelope[] = [];
    await transport.connect();
    await transport.subscribe(['orders'], async (envelope) => {
      received.push(envelope);
    });
    const { body, properties, headers } = CASES[0]!.message;

    connections[0]!.consumerChannel.deliver!({
      content: Buffer.from(body as string),
      fields: { routingKey: 'orders' },
      properties: { ...properties, headers },
    });

    await waitFor(() => received.length === 1);
    expect(received[0]!.type).toBe('order.placed.v1');
    await transport.close();
  });

  it('keeps the wire names the codec already has, and refuses clashes', () => {
    const codec = EXPLICIT.withWireTypes!({ 'order.placed.v1': 'Order Placed' });

    expect(codec.encode(SHIPPED).content).toEqual(EXPLICIT.encode(SHIPPED).content);
    expect(() => EXPLICIT.withWireTypes!({ 'order.placed.v1': 'Placed' })).toThrow(
      WireTypeCollisionError,
    );
  });

  it('refuses a codec that has no type to map', () => {
    const fixed = jsonEnvelope({ ...MAPPING, type: { fixed: 'order.placed.v1' } });

    expect(() => fixed.withWireTypes!({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
  });

  it.each([
    ['the default codec', undefined],
    ['cloudEventsStructured', cloudEventsStructured],
    ['the deprecated decode option', 'decode'],
  ])('refuses wire names for %s at boot', (_label, codec) => {
    const build = () =>
      codec === 'decode'
        ? createTransport({ decode: cloudEventsStructured.decode }).transport.declareWireTypes({
            'order.placed.v1': 'x',
          })
        : catalogRouteBus(codec as RabbitMqCodec | undefined);

    expect(build).toThrow(WireTypesUnsupportedError);
    expect(build).toThrow(
      "The codec of transport 'rabbitmq' cannot map event types to wire names; use jsonEnvelope with a type at { body } or { header }",
    );
  });

  it('checks the wire names without applying them, and refuses once the transport started', async () => {
    const { transport, connections } = createTransport({ codec: jsonEnvelope(MAPPING) });

    const apply = transport.declareWireTypes({ 'order.placed.v1': 'Order Placed' });
    await transport.connect();
    expect(() => transport.declareWireTypes({ 'order.placed.v1': 'Order Placed' })).toThrow(
      "Transport 'rabbitmq' already started; declare wire names before it connects or subscribes",
    );
    expect(apply).toBeTypeOf('function');
    expect(connections).toHaveLength(1);
    await transport.close();
  });

  it('refuses once the transport closed', async () => {
    const { transport } = createTransport({ codec: jsonEnvelope(MAPPING) });
    await transport.close();

    expect(() => transport.declareWireTypes({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
  });

  it('refuses once a connection or a subscription is pending', async () => {
    const connecting = createTransport({ codec: jsonEnvelope(MAPPING) }).transport;
    const done = connecting.connect();
    expect(() => connecting.declareWireTypes({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
    await done;
    await connecting.close();

    const subscribing = createTransport({ codec: jsonEnvelope(MAPPING) }).transport;
    const pending = subscribing.subscribe(['orders'], async () => undefined);
    expect(() => subscribing.declareWireTypes({ 'order.placed.v1': 'x' })).toThrow(
      WireTypesUnsupportedError,
    );
    await pending.catch(() => undefined);
    await subscribing.close();
  });
});
