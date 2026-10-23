import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  InMemoryEventStore,
  defineCatalog,
  defineEvent,
  defineHandlers,
  describeCatalog,
  type Catalog,
  type DomainEvent,
  type EventEnvelope,
  type HandlerContext,
  type HandlersOf,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { OrderPlaced, OrderShipped, createTestBus } from './helpers.js';

class UserRegistered extends defineEvent<{ userId: number }>('user.registered.v1') {}

const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });
const unnamedCatalog = defineCatalog({ OrderPlaced });

/** Handlers that record what they received. */
class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
  readonly seen: string[] = [];
  onOrderPlaced(event: EventEnvelope<{ orderId: string }>, context: HandlerContext): void {
    this.seen.push(`placed:${event.payload.orderId}:${context.handler}`);
  }
  onOrderShipped(event: EventEnvelope<{ orderId: string }>, context: HandlerContext): void {
    this.seen.push(`shipped:${event.payload.orderId}:${context.handler}`);
  }
}

describe('defineCatalog', () => {
  it('freezes the catalog and keeps the event classes under their names', () => {
    expect(Object.isFrozen(OrderEvents)).toBe(true);
    expect(OrderEvents.OrderPlaced).toBe(OrderPlaced);
    expect(Object.keys(OrderEvents)).toEqual(['OrderPlaced', 'OrderShipped']);
  });

  it('is not affected by later changes to the object it was built from', () => {
    const source: Record<string, typeof OrderPlaced> = { OrderPlaced };
    const catalog = defineCatalog(source);
    source['OrderShipped'] = OrderPlaced;
    expect(Object.keys(catalog)).toEqual(['OrderPlaced']);
  });

  it('describes entries with the type and the expected handler method', () => {
    expect(describeCatalog(OrderEvents)).toMatchObject({
      name: 'OrderEvents',
      entries: [
        { name: 'OrderPlaced', type: 'order.placed.v1', handlerName: 'onOrderPlaced' },
        { name: 'OrderShipped', type: 'order.shipped.v1', handlerName: 'onOrderShipped' },
      ],
    });
    expect(describeCatalog(unnamedCatalog).name).toBeUndefined();
  });

  it.each([
    ['an empty catalog', () => defineCatalog({}), /Catalog must contain at least one event/],
    [
      'an empty named catalog',
      () => defineCatalog({}, { name: 'Empty' }),
      /Catalog 'Empty' must contain at least one event/,
    ],
    [
      'a name that cannot be part of a method name',
      () => defineCatalog({ 'order-placed': OrderPlaced }),
      /entry 'order-placed' must be a valid identifier/,
    ],
    [
      'an entry that is not an event class',
      () => defineCatalog({ Plain: class {} } as unknown as Catalog),
      /entry 'Plain' must be an event class created with defineEvent/,
    ],
    [
      'a value that is not a function',
      () => defineCatalog({ Plain: 'order.placed.v1' } as unknown as Catalog),
      /entry 'Plain' must be an event class/,
    ],
    [
      'an event type that is not valid',
      () =>
        defineCatalog({
          Bad: class {
            static readonly type = ' bad';
          },
        } as unknown as Catalog),
      /Event type ' bad' must be/,
    ],
    [
      'two names for one event type',
      () => defineCatalog({ First: OrderPlaced, Second: OrderPlaced }, { name: 'Dup' }),
      /Catalog 'Dup' entries 'First' and 'Second' share the event type 'order.placed.v1'/,
    ],
  ])('rejects %s', (_label, build, message) => {
    expect(build).toThrow(ConfigurationError);
    expect(build).toThrow(message);
  });

  it('refuses objects that were not created by defineCatalog', () => {
    expect(() => describeCatalog({ OrderPlaced })).toThrow(
      new ConfigurationError('Expected a catalog created with defineCatalog'),
    );
  });
});

describe('defineHandlers', () => {
  it('returns the handlers when they match the catalog exactly', () => {
    const handlers = defineHandlers(OrderEvents, {
      onOrderPlaced: () => undefined,
      onOrderShipped: () => undefined,
    });
    expect(Object.keys(handlers)).toEqual(['onOrderPlaced', 'onOrderShipped']);
  });

  it.each<[string, Catalog, object, string]>([
    [
      'a missing handler',
      OrderEvents,
      { onOrderPlaced: () => undefined },
      "Handlers do not match catalog 'OrderEvents': missing onOrderShipped",
    ],
    [
      'an unexpected key',
      unnamedCatalog,
      { onOrderPlaced: () => undefined, onOrderRefunded: () => undefined },
      'Handlers do not match the catalog: unexpected onOrderRefunded',
    ],
    [
      'a handler that is not a function',
      OrderEvents,
      { onOrderPlaced: () => undefined, onOrderShipped: 'nope' },
      "Handlers do not match catalog 'OrderEvents': onOrderShipped is not a function",
    ],
    [
      'every kind of mismatch together',
      OrderEvents,
      { onOrderPlaced: 1, onOrderRefunded: () => undefined },
      "Handlers do not match catalog 'OrderEvents': missing onOrderShipped, unexpected onOrderRefunded, onOrderPlaced is not a function",
    ],
  ])('rejects %s at runtime', (_label, catalog, handlers, message) => {
    expect(() => defineHandlers(catalog, handlers as never)).toThrow(
      new ConfigurationError(message),
    );
  });
});

describe('RelayBus.subscribeCatalog', () => {
  it('registers every on<Key> method under ${catalog name}.${method} and runs it with this bound', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    const handlers = new ShippingHandlers();

    bus.subscribeCatalog(OrderEvents, handlers);
    await bus.publishAll([new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })]);
    await bus.drain();

    expect(bus.handlers().map((handler) => [handler.id, handler.eventType])).toEqual([
      ['OrderEvents.onOrderPlaced', 'order.placed.v1'],
      ['OrderEvents.onOrderShipped', 'order.shipped.v1'],
    ]);
    expect(handlers.seen.sort()).toEqual([
      'placed:1:OrderEvents.onOrderPlaced',
      'shipped:1:OrderEvents.onOrderShipped',
    ]);
  });

  it('uses idPrefix first, then the class name of unnamed catalogs', () => {
    const { bus } = createTestBus();
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers(), { idPrefix: 'fulfilment' });
    bus.subscribeCatalog(unnamedCatalog, new ShippingHandlers());

    expect(bus.handlers().map((handler) => handler.id)).toEqual([
      'fulfilment.onOrderPlaced',
      'fulfilment.onOrderShipped',
      'ShippingHandlers.onOrderPlaced',
    ]);
  });

  it('applies the subscribe options to every handler', () => {
    const { bus } = createTestBus();
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers(), {
      concurrency: 3,
      transactional: true,
    });

    expect(bus.handlers().map((handler) => [handler.concurrency, handler.transactional])).toEqual([
      [3, true],
      [3, true],
    ]);
  });

  it('accepts handlers declared with defineHandlers', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    const seen: string[] = [];
    const handlers = defineHandlers(OrderEvents, {
      onOrderPlaced: (event) => void seen.push(event.payload.orderId),
      onOrderShipped: () => undefined,
    });

    bus.subscribeCatalog(OrderEvents, handlers);
    await bus.publish(new OrderPlaced({ orderId: 'o-9' }));
    await bus.drain();

    expect(seen).toEqual(['o-9']);
  });

  it('lists every missing method and registers nothing', () => {
    const { bus } = createTestBus();
    const partial = { onOrderPlaced: () => undefined } as unknown as HandlersOf<typeof OrderEvents>;
    const none = {} as unknown as HandlersOf<typeof unnamedCatalog>;

    expect(() => bus.subscribeCatalog(OrderEvents, partial)).toThrow(
      new ConfigurationError("Handlers for catalog 'OrderEvents' lack methods: onOrderShipped"),
    );
    expect(() => bus.subscribeCatalog(unnamedCatalog, none)).toThrow(
      new ConfigurationError('Handlers for catalog events lack methods: onOrderPlaced'),
    );
    expect(bus.handlers()).toEqual([]);
  });

  it('needs a catalog name, an idPrefix or a class instance to derive ids', () => {
    const { bus } = createTestBus();
    const literal = { onOrderPlaced: () => undefined };
    const anonymous = new (class {
      onOrderPlaced = () => undefined;
    })();
    Object.defineProperty(anonymous.constructor, 'name', { value: '' });

    expect(() => bus.subscribeCatalog(unnamedCatalog, literal)).toThrow(
      /Cannot derive handler ids/,
    );
    expect(() => bus.subscribeCatalog(unnamedCatalog, anonymous)).toThrow(
      /Cannot derive handler ids/,
    );
    bus.subscribeCatalog(unnamedCatalog, literal, { idPrefix: 'inline' });
    expect(bus.handlers().map((handler) => handler.id)).toEqual(['inline.onOrderPlaced']);
  });

  it('rejects a second registration of the same ids', () => {
    const { bus } = createTestBus();
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers());

    expect(() => bus.subscribeCatalog(OrderEvents, new ShippingHandlers())).toThrow(
      /Handler id 'OrderEvents.onOrderPlaced' is already registered/,
    );
  });

  it('refuses a plain record instead of a catalog', () => {
    const { bus } = createTestBus();
    expect(() =>
      bus.subscribeCatalog({ OrderPlaced } as never, new ShippingHandlers() as never),
    ).toThrow(/created with defineCatalog/);
  });
});

describe('catalog boot check', () => {
  it('fails start listing every event without a handler or route and the method it expects', async () => {
    const store = new InMemoryEventStore();
    let setupCalls = 0;
    Object.assign(store, {
      setup: async () => {
        setupCalls += 1;
      },
    });
    const { bus } = createTestBus({ store, catalogs: [OrderEvents, unnamedCatalog] });

    const failure = await bus.start().catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ConfigurationError);
    expect((failure as Error).message).toBe(
      [
        'Catalog events without a handler or route:',
        '  - OrderEvents.OrderPlaced (order.placed.v1): expected handler method onOrderPlaced',
        '  - OrderEvents.OrderShipped (order.shipped.v1): expected handler method onOrderShipped',
        '  - OrderPlaced (order.placed.v1): expected handler method onOrderPlaced',
      ].join('\n'),
    );
    expect(setupCalls).toBe(0);
  });

  it('leaves the bus idle so it starts once the handlers are registered', async () => {
    const { bus } = createTestBus({ catalogs: [OrderEvents] });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'placed' });

    await expect(bus.start()).rejects.toThrow(/OrderEvents.OrderShipped/);
    expect(bus.isRunning).toBe(false);

    bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    await bus.start();
    expect(bus.isRunning).toBe(true);
    await bus.stop();
  });

  it('passes when every event has a handler, through subscribeCatalog or individually', async () => {
    const { bus } = createTestBus({ catalogs: [OrderEvents, unnamedCatalog] });
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers());

    await bus.start();
    await bus.stop();
  });

  it('counts a route to a transport as covering an event', async () => {
    const { bus } = createTestBus({
      transports: [new InMemoryTransport('broker')],
      routes: [
        { event: OrderPlaced, transport: 'broker' },
        { event: OrderShipped, transport: 'broker' },
      ],
    });
    bus.registerCatalog(OrderEvents);
    bus.registerCatalog(OrderEvents);

    await bus.start();
    await bus.stop();
  });

  it('does nothing when no catalog is registered', async () => {
    const { bus } = createTestBus();
    await bus.start();
    await bus.stop();
  });

  it('rejects options that are not catalogs when the bus is created', () => {
    expect(() => createTestBus({ catalogs: [{ OrderPlaced }] })).toThrow(
      /created with defineCatalog/,
    );
  });
});

describe('RelayBus.forCatalog', () => {
  it('publishes the events of the catalog with the usual options', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers());
    const orders = bus.forCatalog(OrderEvents);

    const single = await orders.publish(new OrderPlaced({ orderId: '1' }), {
      key: 'order-1',
      id: 'fixed-id',
    });
    const many = await orders.publishAll(
      [new OrderPlaced({ orderId: '2' }), new OrderShipped({ orderId: '2' })],
      { key: 'order-2' },
    );

    expect(single).toMatchObject({ id: 'fixed-id', key: 'order-1', type: 'order.placed.v1' });
    expect(many.map((envelope) => [envelope.type, envelope.key])).toEqual([
      ['order.placed.v1', 'order-2'],
      ['order.shipped.v1', 'order-2'],
    ]);
    expect(await bus.drain()).toBe(3);
  });

  it('publishes without options', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers());
    const orders = bus.forCatalog(OrderEvents);

    await orders.publish(new OrderPlaced({ orderId: '1' }));
    await orders.publishAll([new OrderShipped({ orderId: '1' })]);

    expect(await bus.drain()).toBe(2);
  });

  it('rejects an event of another catalog with a rejected promise and stores nothing', async () => {
    const { bus, store } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(UserRegistered, () => undefined, { id: 'users' });
    bus.subscribeCatalog(OrderEvents, new ShippingHandlers());
    const orders = bus.forCatalog(OrderEvents);
    const foreign = new UserRegistered({ userId: 1 }) as unknown as OrderPlaced;

    await expect(orders.publish(foreign)).rejects.toThrow(
      new ConfigurationError(
        "Event type 'user.registered.v1' is not part of catalog 'OrderEvents'",
      ),
    );
    await expect(orders.publishAll([new OrderPlaced({ orderId: '1' }), foreign])).rejects.toThrow(
      /not part of catalog 'OrderEvents'/,
    );
    expect((await store.statistics(new Date())).pending).toBe(0);
  });

  it('names an unnamed catalog generically and rejects values that are not events', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    const orders = bus.forCatalog(unnamedCatalog);
    // Structurally identical to OrderPlaced, so only the runtime check can tell them apart.
    const foreign = new OrderShipped({ orderId: '1' });
    const notAnEvent = {} as unknown as DomainEvent as OrderPlaced;

    await expect(orders.publish(foreign)).rejects.toThrow(
      new ConfigurationError("Event type 'order.shipped.v1' is not part of the catalog"),
    );
    await expect(orders.publish(notAnEvent)).rejects.toThrow(/Event type 'undefined' is not part/);
  });

  it('refuses a plain record instead of a catalog', () => {
    const { bus } = createTestBus();
    expect(() => bus.forCatalog({ OrderPlaced } as never)).toThrow(/created with defineCatalog/);
  });
});
