import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  CatalogPublisher,
  InMemoryEventStore,
  RelayBus,
  consume,
  defineCatalog,
  defineEvent,
  defineHandlers,
  describeCatalog,
  produce,
  type EventEnvelope,
  type HandlerContext,
  type HandlersOf,
  type InMemoryTransaction,
  type StandardSchemaV1,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { OrderPlaced, OrderShipped, createTestBus } from './helpers.js';

class OrderCancelled extends defineEvent<{ orderId: string; reason: string }>(
  'order.cancelled.v1',
) {}

/** A shared contracts module: both services import this one catalog. */
const OrderEvents = defineCatalog(
  { OrderPlaced, OrderShipped, OrderCancelled },
  { name: 'OrderEvents' },
);
const UnnamedEvents = defineCatalog({ OrderPlaced, OrderShipped });

function routeTo(transport: string, ...events: (typeof OrderPlaced)[]) {
  return events.map((event) => ({ event, transport }));
}

function failureOf(promise: Promise<unknown>): Promise<Error> {
  return promise.then(
    () => {
      throw new Error('expected a failure');
    },
    (error: unknown) => error as Error,
  );
}

describe('catalog event selection', () => {
  it('declares every event of the catalog by default, in catalog order', () => {
    const consumed = consume(OrderEvents);
    const produced = produce(OrderEvents, { events: ['OrderCancelled', 'OrderPlaced'] });

    expect(consumed.role).toBe('consume');
    expect(consumed.catalog).toBe(OrderEvents);
    expect(consumed.events).toEqual(['OrderPlaced', 'OrderShipped', 'OrderCancelled']);
    expect(produced.role).toBe('produce');
    expect(produced.events).toEqual(['OrderPlaced', 'OrderCancelled']);
    expect(produced.entries.map((entry) => entry.handlerName)).toEqual([
      'onOrderPlaced',
      'onOrderCancelled',
    ]);
    expect(Object.isFrozen(produced)).toBe(true);
    expect(Object.isFrozen(produced.events)).toBe(true);
  });

  it.each<[string, () => unknown, string]>([
    [
      'an empty list',
      () => consume(OrderEvents, { events: [] }),
      "The events selected from catalog 'OrderEvents' must name at least one event; omit events to select all of them",
    ],
    [
      'an unknown name',
      () => produce(OrderEvents, { events: ['OrderRefunded' as never] }),
      "Catalog 'OrderEvents' has no event named 'OrderRefunded'; its events are OrderPlaced, OrderShipped, OrderCancelled",
    ],
    [
      'an inherited property name',
      () => consume(UnnamedEvents, { events: ['toString' as never] }),
      "The catalog has no event named 'toString'; its events are OrderPlaced, OrderShipped",
    ],
    [
      'a duplicate name',
      () => consume(OrderEvents, { events: ['OrderPlaced', 'OrderPlaced'] }),
      "Event 'OrderPlaced' of catalog 'OrderEvents' is selected twice",
    ],
    [
      'a record that is not a catalog',
      () => consume({ OrderPlaced }),
      'Expected a catalog created with defineCatalog',
    ],
  ])('rejects %s', (_label, select, message) => {
    expect(select).toThrow(new ConfigurationError(message));
  });

  it('rejects anything but a catalog or a declaration as a bus catalog', () => {
    const forged = Object.freeze({ catalog: OrderEvents, role: 'consume', events: [] });
    expect(() => createTestBus({ catalogs: [forged as never] })).toThrow(
      new ConfigurationError('Expected a catalog created with defineCatalog'),
    );
  });
});

describe('catalog entries cannot be tampered with', () => {
  it('freezes every entry so a changed type cannot pass the checks', () => {
    const [placed] = describeCatalog(OrderEvents).entries;
    expect(Object.isFrozen(placed)).toBe(true);
    expect(Object.isFrozen(describeCatalog(OrderEvents).entries)).toBe(true);
    expect(() => {
      (placed as { type: string }).type = 'order.shipped.v1';
    }).toThrow(TypeError);
    expect(describeCatalog(OrderEvents).entries[0]!.type).toBe('order.placed.v1');
  });

  it('rejects a name with $, which no handler id accepts', () => {
    expect(() => defineCatalog({ $Placed: OrderPlaced }, { name: 'Dollar' })).toThrow(
      new ConfigurationError(
        "Catalog 'Dollar' entry '$Placed' must be a valid identifier made of letters, digits and " +
          'underscores because it names the handler method on$Placed and its handler id',
      ),
    );
    expect(() => defineCatalog({ Order$Placed: OrderPlaced })).toThrow(
      /entry 'Order\$Placed' must be a valid identifier/,
    );
  });

  it('keeps underscores, which handler ids accept', () => {
    const { bus } = createTestBus();
    bus.subscribeCatalog(defineCatalog({ _Order_Placed1: OrderPlaced }, { name: 'Underscored' }), {
      on_Order_Placed1: () => undefined,
    });
    expect(bus.handlers().map((handler) => handler.id)).toEqual(['Underscored.on_Order_Placed1']);
  });
});

describe('boot check with consume and produce', () => {
  it('passes when consumed events have handlers and produced events have routes', async () => {
    const { bus } = createTestBus({
      transports: [new InMemoryTransport('broker')],
      routes: routeTo('broker', OrderPlaced),
      catalogs: [
        produce(OrderEvents, { events: ['OrderPlaced'] }),
        consume(OrderEvents, { events: ['OrderShipped'] }),
      ],
    });
    bus.subscribeCatalog(
      OrderEvents,
      { onOrderShipped: () => undefined },
      {
        events: ['OrderShipped'],
      },
    );

    await bus.start();
    await bus.stop();
  });

  it('ignores events of the catalog the service does not declare', async () => {
    const { bus } = createTestBus({
      catalogs: [consume(OrderEvents, { events: ['OrderCancelled'] })],
    });
    bus.subscribe(OrderCancelled, () => undefined, { id: 'cancelled' });

    await bus.start();
    await bus.stop();
  });

  it('fails a consumer whose consumed event has no handler, even with a route', async () => {
    const { bus } = createTestBus({
      transports: [new InMemoryTransport('broker')],
      routes: routeTo('broker', OrderShipped),
      catalogs: [consume(OrderEvents, { events: ['OrderPlaced', 'OrderShipped'] })],
    });

    const failure = await failureOf(bus.start());

    expect(failure).toBeInstanceOf(ConfigurationError);
    expect(failure.message).toBe(
      [
        'Catalog events this service does not cover:',
        '  - OrderEvents.OrderPlaced (order.placed.v1): consumed here, needs handler method onOrderPlaced',
        '  - OrderEvents.OrderShipped (order.shipped.v1): consumed here, needs handler method onOrderShipped; ' +
          'its route to a transport does not handle it here',
      ].join('\n'),
    );
    expect(bus.isRunning).toBe(false);
  });

  it('fails a producer whose produced event has no route, even with a local handler', async () => {
    const { bus } = createTestBus({ catalogs: [produce(UnnamedEvents)] });
    bus.subscribe(OrderShipped, () => undefined, { id: 'local' });

    await expect(bus.start()).rejects.toThrow(
      new ConfigurationError(
        [
          'Catalog events this service does not cover:',
          '  - OrderPlaced (order.placed.v1): produced here, needs a route to a transport',
          '  - OrderShipped (order.shipped.v1): produced here, needs a route to a transport; ' +
            'a local handler does not deliver it to other services',
        ].join('\n'),
      ),
    );
  });

  it('requires both a handler and a route when an event is consumed and produced', async () => {
    const transport = new InMemoryTransport('broker');
    const { bus } = createTestBus({
      transports: [transport],
      catalogs: [
        consume(OrderEvents, { events: ['OrderPlaced'] }),
        produce(OrderEvents, { events: ['OrderPlaced'] }),
      ],
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'placed' });

    await expect(bus.start()).rejects.toThrow(
      'OrderEvents.OrderPlaced (order.placed.v1): produced here, needs a route to a transport; ' +
        'a local handler does not deliver it to other services',
    );

    const routed = createTestBus({
      transports: [new InMemoryTransport('broker')],
      routes: routeTo('broker', OrderPlaced),
      catalogs: [
        consume(OrderEvents, { events: ['OrderPlaced'] }),
        produce(OrderEvents, { events: ['OrderPlaced'] }),
      ],
    }).bus;
    routed.subscribe(OrderPlaced, () => undefined, { id: 'placed' });
    await routed.start();
    await routed.stop();
  });

  it('lists each gap once when declarations repeat, next to bare catalogs', async () => {
    const { bus } = createTestBus({
      catalogs: [consume(OrderEvents, { events: ['OrderPlaced'] }), UnnamedEvents],
    });
    bus.registerCatalog(consume(OrderEvents, { events: ['OrderPlaced'] }));
    bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });

    await expect(bus.start()).rejects.toThrow(
      new ConfigurationError(
        [
          'Catalog events this service does not cover:',
          '  - OrderEvents.OrderPlaced (order.placed.v1): consumed here, needs handler method onOrderPlaced',
          '  - OrderPlaced (order.placed.v1): expected handler method onOrderPlaced',
        ].join('\n'),
      ),
    );
  });

  it('keeps the bare-catalog heading when only bare catalogs have gaps', async () => {
    const { bus } = createTestBus({
      catalogs: [consume(OrderEvents, { events: ['OrderShipped'] }), UnnamedEvents],
    });
    bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });

    await expect(bus.start()).rejects.toThrow(
      new ConfigurationError(
        [
          'Catalog events without a handler or route:',
          '  - OrderPlaced (order.placed.v1): expected handler method onOrderPlaced',
        ].join('\n'),
      ),
    );
  });
});

describe('RelayBus.subscribeCatalog with selected events', () => {
  class EveryOrderHandler implements HandlersOf<typeof OrderEvents> {
    readonly seen: string[] = [];
    onOrderPlaced(event: EventEnvelope<{ orderId: string }>): void {
      this.seen.push(`placed:${event.payload.orderId}`);
    }
    onOrderShipped(event: EventEnvelope<{ orderId: string }>): void {
      this.seen.push(`shipped:${event.payload.orderId}`);
    }
    onOrderCancelled(): void {
      this.seen.push('cancelled');
    }
  }

  it('registers only the selected methods and ignores the others', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    const handlers = new EveryOrderHandler();
    bus.subscribeCatalog(OrderEvents, handlers, {
      events: ['OrderShipped', 'OrderPlaced'],
      concurrency: 2,
    });

    await bus.publishAll([
      new OrderPlaced({ orderId: '1' }),
      new OrderCancelled({ orderId: '1', reason: 'late' }),
    ]);
    await bus.drain();

    expect(bus.handlers().map((handler) => [handler.id, handler.concurrency])).toEqual([
      ['OrderEvents.onOrderPlaced', 2],
      ['OrderEvents.onOrderShipped', 2],
    ]);
    expect(handlers.seen).toEqual(['placed:1']);
  });

  it('lists only the missing methods of the selected events', () => {
    const { bus } = createTestBus();
    expect(() =>
      bus.subscribeCatalog(OrderEvents, { onOrderPlaced: () => undefined } as never, {
        events: ['OrderPlaced', 'OrderShipped', 'OrderCancelled'],
      }),
    ).toThrow(
      new ConfigurationError(
        "Handlers for catalog 'OrderEvents' lack methods: onOrderShipped, onOrderCancelled",
      ),
    );
    expect(bus.handlers()).toEqual([]);
  });

  it('rejects an invalid selection before registering anything', () => {
    const { bus } = createTestBus();
    expect(() =>
      bus.subscribeCatalog(OrderEvents, new EveryOrderHandler(), { events: [] }),
    ).toThrow(/must name at least one event/);
    expect(bus.handlers()).toEqual([]);
  });
});

describe('defineHandlers with selected events', () => {
  it('returns handlers that match the selection exactly', () => {
    const handlers = defineHandlers(
      OrderEvents,
      { onOrderShipped: () => undefined },
      { events: ['OrderShipped'] },
    );
    expect(Object.keys(handlers)).toEqual(['onOrderShipped']);
  });

  it('rejects handlers of events that are not selected and missing selected ones', () => {
    expect(() =>
      defineHandlers(
        OrderEvents,
        { onOrderPlaced: () => undefined, onOrderShipped: () => undefined } as never,
        { events: ['OrderPlaced', 'OrderCancelled'] },
      ),
    ).toThrow(
      new ConfigurationError(
        "Handlers do not match catalog 'OrderEvents': missing onOrderCancelled, unexpected onOrderShipped",
      ),
    );
  });

  it('accepts handlers typed with the transaction of the bus', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    const transactions: unknown[] = [];
    const handlers = defineHandlers(
      OrderEvents,
      {
        onOrderPlaced: (_event, context: HandlerContext<InMemoryTransaction>) => {
          transactions.push(context.transaction);
        },
      },
      { events: ['OrderPlaced'] },
    );
    bus.subscribeCatalog(OrderEvents, handlers, { events: ['OrderPlaced'], transactional: true });

    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();

    expect(transactions).toHaveLength(1);
    expect(transactions[0]).toBeDefined();
  });
});

describe('RelayBus.forCatalog with selected events', () => {
  it('publishes the produced events and rejects the others before storing anything', async () => {
    const { bus, store } = createTestBus({ transactionMode: 'optional' });
    bus.subscribeCatalog(
      OrderEvents,
      new (class Sink {
        onOrderPlaced(): void {}
        onOrderShipped(): void {}
        onOrderCancelled(): void {}
      })(),
    );
    const placedOnly = bus.forCatalog(OrderEvents, { events: ['OrderPlaced'] });
    // Structurally an OrderPlaced, so only the runtime check can reject it.
    const shipped = new OrderShipped({ orderId: '1' });

    await expect(placedOnly.publish(new OrderPlaced({ orderId: '1' }))).resolves.toMatchObject({
      type: 'order.placed.v1',
    });
    await expect(placedOnly.publish(shipped)).rejects.toThrow(
      new ConfigurationError(
        "Event type 'order.shipped.v1' is not one of the events this view of catalog 'OrderEvents' publishes (OrderPlaced)",
      ),
    );
    await expect(
      placedOnly.publishAll([new OrderPlaced({ orderId: '2' }), shipped]),
    ).rejects.toThrow(/not one of the events this view/);
    await expect(
      placedOnly.publish(new OrderCancelled({ orderId: '1', reason: 'x' }) as never),
    ).rejects.toThrow(/not one of the events this view of catalog 'OrderEvents'/);
    expect((await store.statistics(new Date())).pending).toBe(1);
  });

  it('names an unnamed catalog generically and validates the selection', () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    const view = bus.forCatalog(UnnamedEvents, { events: ['OrderShipped'] });
    expect(view).toBeInstanceOf(CatalogPublisher);
    expect(() => bus.forCatalog(UnnamedEvents, { events: [] })).toThrow(
      'The events selected from the catalog must name at least one event',
    );
    return expect(view.publish(new OrderPlaced({ orderId: '1' }))).rejects.toThrow(
      "Event type 'order.placed.v1' is not one of the events this view of the catalog publishes (OrderShipped)",
    );
  });
});

/** A minimal Standard Schema that requires a string orderId. */
const orderIdSchema: StandardSchemaV1<unknown, { orderId: string }> = {
  '~standard': {
    version: 1,
    vendor: 'test',
    validate: (value) =>
      typeof (value as { orderId?: unknown }).orderId === 'string'
        ? { value: value as { orderId: string } }
        : { issues: [{ message: 'orderId must be a string' }] },
  },
};

class PaymentRequested extends defineEvent('payment.requested.v1', { schema: orderIdSchema }) {}
class PaymentCaptured extends defineEvent<{ orderId: string }>('payment.captured.v1') {}

/** The contracts package both in-process services import. */
const PaymentEvents = defineCatalog(
  { PaymentRequested, PaymentCaptured },
  { name: 'PaymentEvents' },
);

interface InProcessService {
  readonly bus: RelayBus<InMemoryTransaction>;
  readonly transport: InMemoryTransport;
  readonly seen: string[];
}

/**
 * Two services in one process: orders produces PaymentRequested and consumes
 * PaymentCaptured; payments does the opposite. Each has its own store and
 * transport; `forward` plays the broker between them.
 */
function createServices(): { orders: InProcessService; payments: InProcessService } {
  const orders = createService('orders', {
    produces: 'PaymentRequested',
    consumes: 'PaymentCaptured',
  });
  const payments = createService('payments', {
    produces: 'PaymentCaptured',
    consumes: 'PaymentRequested',
  });
  return { orders, payments };
}

function createService(
  source: string,
  roles: { produces: keyof typeof PaymentEvents; consumes: keyof typeof PaymentEvents },
): InProcessService {
  const transport = new InMemoryTransport('broker');
  const seen: string[] = [];
  const bus = new RelayBus<InMemoryTransaction>({
    store: new InMemoryEventStore(),
    source,
    transactionMode: 'optional',
    dispatcher: { enabled: false },
    logger: { log: () => undefined, warn: () => undefined, error: () => undefined },
    transports: [transport],
    routes: [
      { event: PaymentEvents[roles.produces], transport: 'broker', destination: 'payments' },
    ],
    subscriptions: [{ transport: 'broker', destinations: ['payments'] }],
    catalogs: [
      produce(PaymentEvents, { events: [roles.produces] }),
      consume(PaymentEvents, { events: [roles.consumes] }),
    ],
  });
  bus.subscribeCatalog(
    PaymentEvents,
    {
      onPaymentRequested: (event) => {
        seen.push(`requested:${event.payload.orderId}`);
      },
      onPaymentCaptured: (event) => {
        seen.push(`captured:${event.payload.orderId}`);
      },
    },
    { events: [roles.consumes], idPrefix: source },
  );
  return { bus, transport, seen };
}

/** Moves everything `from` published to the subscribers of `to`, like a broker. */
async function forward(from: InProcessService, to: InProcessService): Promise<void> {
  for (const message of from.transport.published.splice(0)) {
    await to.transport.deliver(message.destination, message.envelope);
  }
}

describe('two services sharing one catalog in one process', () => {
  it('carries each event from the producing service to the consuming one', async () => {
    const { orders, payments } = createServices();
    await orders.bus.start();
    await payments.bus.start();

    await orders.bus
      .forCatalog(PaymentEvents, { events: ['PaymentRequested'] })
      .publish(new PaymentRequested({ orderId: 'o-1' }), { key: 'o-1' });
    await orders.bus.drain();
    await forward(orders, payments);
    await payments.bus.drain();

    await payments.bus
      .forCatalog(PaymentEvents, { events: ['PaymentCaptured'] })
      .publish(new PaymentCaptured({ orderId: 'o-1' }), { key: 'o-1' });
    await payments.bus.drain();
    await forward(payments, orders);
    await orders.bus.drain();

    expect(payments.seen).toEqual(['requested:o-1']);
    expect(orders.seen).toEqual(['captured:o-1']);
    expect(payments.bus.handlers().map((handler) => handler.id)).toEqual([
      'relaybus.transport:broker:payments',
      'payments.onPaymentRequested',
    ]);
    await orders.bus.stop();
    await payments.bus.stop();
  });

  it('handles a redelivered event once', async () => {
    const { orders, payments } = createServices();
    await payments.bus.start();
    await orders.bus.forCatalog(PaymentEvents).publish(new PaymentRequested({ orderId: 'o-2' }));
    await orders.bus.drain();
    const [message] = orders.transport.published;

    await payments.transport.deliver(message!.destination, message!.envelope);
    await payments.transport.deliver(message!.destination, message!.envelope);
    await payments.bus.drain();

    expect(payments.seen).toEqual(['requested:o-2']);
    await payments.bus.stop();
  });

  it('validates a received payload against the catalog event schema', async () => {
    const { orders, payments } = createServices();
    await payments.bus.start();
    await orders.bus.publish(new PaymentRequested({ orderId: 'o-3' }));
    await orders.bus.drain();
    const [message] = orders.transport.published;

    await payments.transport.deliver('payments', {
      ...message!.envelope,
      id: 'malformed',
      payload: { orderId: 3 },
    });
    await payments.bus.drain();

    expect(payments.seen).toEqual([]);
    expect(await payments.bus.listDeliveries({ status: 'dead' })).toEqual([
      expect.objectContaining({
        handler: 'payments.onPaymentRequested',
        lastError: expect.objectContaining({
          message: "Payload of 'payment.requested.v1' failed validation: orderId must be a string",
        }),
      }),
    ]);
    await payments.bus.stop();
  });

  it('refuses to start a service whose declarations it does not fulfil', async () => {
    const consumerWithoutHandler = new RelayBus({
      store: new InMemoryEventStore(),
      catalogs: [consume(PaymentEvents, { events: ['PaymentRequested'] })],
    });
    const producerWithoutRoute = new RelayBus({
      store: new InMemoryEventStore(),
      catalogs: [produce(PaymentEvents, { events: ['PaymentCaptured'] })],
    });

    await expect(consumerWithoutHandler.start()).rejects.toThrow(
      'PaymentEvents.PaymentRequested (payment.requested.v1): consumed here, needs handler method onPaymentRequested',
    );
    await expect(producerWithoutRoute.start()).rejects.toThrow(
      'PaymentEvents.PaymentCaptured (payment.captured.v1): produced here, needs a route to a transport',
    );
  });
});
