import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  DuplicateRouteEventError,
  UnknownCatalogEventError,
  WireTypeCollisionError,
  WireTypesUnsupportedError,
  catalogWireTypes,
  consume,
  defineCatalog,
  describeCatalog,
  defineEvent,
  produce,
  route,
  type RouteDefinition,
  type Transport,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { wireNamesOf } from '../src/catalog.js';
import { createTestBus } from './helpers.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderCancelled extends defineEvent<{ orderId: string }>('order.cancelled.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

const OrderEvents = defineCatalog(
  { OrderPlaced, OrderCancelled, OrderShipped },
  { name: 'OrderEvents' },
);
const WiredEvents = defineCatalog(
  { OrderPlaced, OrderCancelled, OrderShipped },
  {
    name: 'WiredEvents',
    wireTypes: { OrderPlaced: 'Order Placed', OrderCancelled: 'Order Cancelled' },
  },
);

function busWith(
  routes: readonly RouteDefinition[],
  transport: Transport = new InMemoryTransport('broker'),
) {
  return createTestBus({ transactionMode: 'optional', transports: [transport], routes });
}

function errorOf(action: () => unknown): unknown {
  try {
    action();
  } catch (error) {
    return error;
  }
  throw new Error('expected a failure');
}

describe('route(produce(...))', () => {
  it('creates a frozen catalog route', () => {
    const created = route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
      transport: 'broker',
    });

    expect(Object.isFrozen(created)).toBe(true);
    expect(Object.isFrozen(created.options)).toBe(true);
    expect(created.declaration.events).toEqual(['OrderPlaced']);
  });

  it.each([
    ['a consume declaration', () => consume(OrderEvents)],
    ['a hand-made object', () => ({ ...produce(OrderEvents) })],
  ])('refuses %s', (_label, declaration) => {
    const error = errorOf(() => route(declaration(), { transport: 'broker' }));

    expect(error).toBeInstanceOf(ConfigurationError);
    expect((error as Error).message).toBe(
      'route() takes a produce(catalog, { events }) declaration',
    );
  });

  it('refuses a wire name for an event it does not route', () => {
    const error = errorOf(() =>
      route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
        transport: 'broker',
        wireTypes: { OrderShipped: 'Order Shipped' } as never,
      }),
    );

    expect(error).toBeInstanceOf(UnknownCatalogEventError);
    expect((error as UnknownCatalogEventError).code).toBe('RELAYBUS_CATALOG_UNKNOWN_EVENT');
    expect((error as Error).message).toBe(
      "The route of catalog 'OrderEvents' has no event named 'OrderShipped' to give a wire name; its events are OrderPlaced",
    );
  });

  it('refuses an empty wire name and one wire name for two events', () => {
    const produced = produce(OrderEvents);

    expect(
      errorOf(() => route(produced, { transport: 'broker', wireTypes: { OrderPlaced: '' } })),
    ).toBeInstanceOf(ConfigurationError);
    const clash = errorOf(() =>
      route(produced, { transport: 'broker', wireTypes: { OrderPlaced: 'x', OrderShipped: 'x' } }),
    );
    expect(clash).toBeInstanceOf(WireTypeCollisionError);
    expect((clash as WireTypeCollisionError).code).toBe('RELAYBUS_WIRE_TYPE_COLLISION');
  });
});

describe('catalog routes on a bus', () => {
  it('turns each selected event into a route of one destination with the shared settings', async () => {
    const transport = new InMemoryTransport('broker');
    const retry = { initialDelay: 100, jitter: 'none' } as const;
    const { bus } = busWith(
      [
        route(produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }), {
          transport: 'broker',
          destination: 'orders',
          retry,
          timeout: '5s',
        }),
      ],
      transport,
    );

    const handlers = bus.handlers();
    expect(handlers.map(({ id, eventType }) => `${id} ${eventType}`)).toEqual([
      'relaybus.transport:broker:orders order.placed.v1',
      'relaybus.transport:broker:orders order.cancelled.v1',
    ]);
    expect(
      handlers.every(
        ({ timeoutMs, forwardsToTransport }) => timeoutMs === 5000 && forwardsToTransport,
      ),
    ).toBe(true);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();
    expect(transport.published.map(({ destination }) => destination)).toEqual(['orders']);
  });

  it('routes every event of the catalog by default, each to its own event type', () => {
    const { bus } = busWith([route(produce(OrderEvents), { transport: 'broker' })]);

    expect(bus.handlers().map(({ id }) => id)).toEqual([
      'relaybus.transport:broker:order.placed.v1',
      'relaybus.transport:broker:order.cancelled.v1',
      'relaybus.transport:broker:order.shipped.v1',
    ]);
  });

  it('keeps events of one key in one queue per destination, next to plain routes', async () => {
    const transport = new InMemoryTransport('broker');
    const { bus, clock } = busWith(
      [
        route(produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }), {
          transport: 'broker',
          destination: 'orders',
          retry: { initialDelay: 100, jitter: 'none' },
        }),
        { event: OrderShipped, transport: 'broker', destination: 'shipments' },
      ],
      transport,
    );
    transport.failNextPublishes(1);
    await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' });
    await bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' });
    await bus.drain();
    expect(transport.published).toEqual([]);

    clock.advance(100);
    await bus.drain();

    expect(
      transport.published.map(({ destination, envelope }) => `${destination} ${envelope.type}`),
    ).toEqual(['orders order.placed.v1', 'orders order.cancelled.v1']);
  });

  it('counts as the route a produce declaration needs at start', async () => {
    const covered = busWith([route(produce(OrderEvents), { transport: 'broker' })]);
    covered.bus.registerCatalog(produce(OrderEvents));
    await covered.bus.start();
    await covered.bus.stop();

    const partial = busWith([
      route(produce(OrderEvents, { events: ['OrderPlaced'] }), { transport: 'broker' }),
    ]);
    partial.bus.registerCatalog(produce(OrderEvents));
    await expect(partial.bus.start()).rejects.toThrow(
      /OrderCancelled \(order.cancelled.v1\): produced here/,
    );
  });

  it('still rejects an unknown transport and routes with different retry to one destination', () => {
    expect(() => busWith([route(produce(OrderEvents), { transport: 'nowhere' })])).toThrow(
      "No transport named 'nowhere' is configured",
    );
    expect(() =>
      busWith([
        route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
          transport: 'broker',
          destination: 'orders',
        }),
        { event: OrderShipped, transport: 'broker', destination: 'orders', timeout: '1s' },
      ]),
    ).toThrow(/must share retry and timeout/);
  });
});

describe('events routed twice', () => {
  const placed = produce(OrderEvents, { events: ['OrderPlaced'] });

  it.each([
    [
      'two catalog routes',
      [
        route(placed, { transport: 'broker', destination: 'orders' }),
        route(produce(OrderEvents), { transport: 'broker', destination: 'orders' }),
      ],
    ],
    [
      'a catalog route after a plain route',
      [
        { event: OrderPlaced, transport: 'broker', destination: 'orders' },
        route(placed, { transport: 'broker', destination: 'orders' }),
      ],
    ],
    [
      'a plain route after a catalog route',
      [route(placed, { transport: 'broker' }), { event: 'order.placed.v1', transport: 'broker' }],
    ],
  ] satisfies [string, RouteDefinition[]][])('fails for %s', (_label, routes) => {
    const error = errorOf(() => busWith(routes));

    expect(error).toBeInstanceOf(DuplicateRouteEventError);
    expect(error).toBeInstanceOf(ConfigurationError);
    expect((error as DuplicateRouteEventError).code).toBe('RELAYBUS_ROUTE_DUPLICATE_EVENT');
    expect((error as Error).message).toMatch(
      /^Event type order\.placed\.v1 is routed to '.+' of transport 'broker' twice$/,
    );
  });

  it('allows the same event to different destinations or transports', () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      transports: [new InMemoryTransport('broker'), new InMemoryTransport('other')],
      routes: [
        route(placed, { transport: 'broker', destination: 'orders' }),
        route(placed, { transport: 'broker', destination: 'audit' }),
        route(placed, { transport: 'other', destination: 'orders' }),
      ],
    });

    expect(bus.handlers()).toHaveLength(3);
  });

  it('keeps failing two plain routes with the same ConfigurationError as before', () => {
    const error = errorOf(() =>
      busWith([
        { event: OrderPlaced, transport: 'broker', destination: 'orders' },
        { event: OrderPlaced, transport: 'broker', destination: 'orders' },
      ]),
    );

    expect((error as ConfigurationError).code).toBe('RELAYBUS_CONFIGURATION');
    expect(error).not.toBeInstanceOf(DuplicateRouteEventError);
  });
});

describe('wire names of catalog routes', () => {
  it('declares the wire names of the catalog for the events the routes send', () => {
    const transport = new InMemoryTransport('broker');
    busWith(
      [
        route(produce(WiredEvents, { events: ['OrderPlaced', 'OrderShipped'] }), {
          transport: 'broker',
        }),
      ],
      transport,
    );

    expect(transport.declaredWireTypes).toEqual({ 'order.placed.v1': 'Order Placed' });
  });

  it('lets the route override the wire names of the catalog', () => {
    const transport = new InMemoryTransport('broker');
    busWith(
      [
        route(produce(WiredEvents), {
          transport: 'broker',
          wireTypes: { OrderPlaced: 'Placed', OrderShipped: 'Shipped' },
        }),
      ],
      transport,
    );

    expect(transport.declaredWireTypes).toEqual({
      'order.placed.v1': 'Placed',
      'order.cancelled.v1': 'Order Cancelled',
      'order.shipped.v1': 'Shipped',
    });
  });

  it('declares nothing without wire names, and nothing for plain routes', () => {
    const transport = new InMemoryTransport('broker');
    busWith(
      [
        route(produce(OrderEvents), { transport: 'broker' }),
        { event: 'order.audited.v1', transport: 'broker' },
      ],
      transport,
    );

    expect(transport.declaredWireTypes).toEqual({});
  });

  it('accepts the same wire name from two routes of one transport', () => {
    const transport = new InMemoryTransport('broker');
    busWith(
      [
        route(produce(WiredEvents, { events: ['OrderPlaced'] }), {
          transport: 'broker',
          destination: 'a',
        }),
        route(produce(WiredEvents, { events: ['OrderPlaced'] }), {
          transport: 'broker',
          destination: 'b',
        }),
      ],
      transport,
    );

    expect(transport.declaredWireTypes).toEqual({ 'order.placed.v1': 'Order Placed' });
  });

  it('fails when one event type gets two wire names on a transport', () => {
    const error = errorOf(() =>
      busWith([
        route(produce(WiredEvents, { events: ['OrderPlaced'] }), {
          transport: 'broker',
          destination: 'a',
        }),
        route(produce(WiredEvents, { events: ['OrderPlaced'] }), {
          transport: 'broker',
          destination: 'b',
          wireTypes: { OrderPlaced: 'Placed' },
        }),
      ]),
    );

    expect(error).toBeInstanceOf(WireTypeCollisionError);
    expect((error as Error).message).toBe(
      "Event type 'order.placed.v1' has the wire names 'Order Placed' and 'Placed' on transport 'broker'; a transport writes one",
    );
  });

  it('fails when two event types share a wire name on a transport', () => {
    const error = errorOf(() =>
      busWith([
        route(produce(WiredEvents, { events: ['OrderPlaced'] }), { transport: 'broker' }),
        route(produce(OrderEvents, { events: ['OrderShipped'] }), {
          transport: 'broker',
          wireTypes: { OrderShipped: 'Order Placed' },
        }),
      ]),
    );

    expect(error).toBeInstanceOf(WireTypeCollisionError);
    expect((error as Error).message).toBe(
      "Event types 'order.placed.v1' and 'order.shipped.v1' share the wire name 'Order Placed' on transport 'broker'",
    );
  });

  it('allows the same wire name on different transports', () => {
    const [broker, other] = [new InMemoryTransport('broker'), new InMemoryTransport('other')];
    createTestBus({
      transactionMode: 'optional',
      transports: [broker, other],
      routes: [
        route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
          transport: 'broker',
          wireTypes: { OrderPlaced: 'x' },
        }),
        route(produce(OrderEvents, { events: ['OrderShipped'] }), {
          transport: 'other',
          wireTypes: { OrderShipped: 'x' },
        }),
      ],
    });

    expect([broker.declaredWireTypes, other.declaredWireTypes]).toEqual([
      { 'order.placed.v1': 'x' },
      { 'order.shipped.v1': 'x' },
    ]);
  });

  it('refuses a transport that cannot take wire names, naming the events', () => {
    const plain: Transport = {
      name: 'plain',
      publish: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const error = errorOf(() =>
      busWith([route(produce(WiredEvents), { transport: 'plain' })], plain),
    );

    expect(error).toBeInstanceOf(WireTypesUnsupportedError);
    expect((error as WireTypesUnsupportedError).code).toBe('RELAYBUS_WIRE_TYPES_UNSUPPORTED');
    expect((error as Error).message).toBe(
      "Transport 'plain' cannot map event types to wire names, but routes or catalogs declare them for: order.placed.v1, order.cancelled.v1",
    );
  });

  it('leaves transports untouched when the routes are refused', () => {
    const transport = new InMemoryTransport('broker');

    expect(() =>
      busWith(
        [
          route(produce(WiredEvents, { events: ['OrderPlaced'] }), {
            transport: 'broker',
            destination: 'a',
          }),
          route(produce(WiredEvents, { events: ['OrderCancelled'] }), {
            transport: 'broker',
            destination: 'a',
            timeout: '1s',
          }),
        ],
        transport,
      ),
    ).toThrow(/must share retry and timeout/);
    expect(transport.declaredWireTypes).toEqual({});
  });
});

describe('wire names of defineCatalog', () => {
  it('lists them by event type for a codec that reads them', () => {
    expect(catalogWireTypes(WiredEvents)).toEqual({
      'order.placed.v1': 'Order Placed',
      'order.cancelled.v1': 'Order Cancelled',
    });
    expect(catalogWireTypes(OrderEvents)).toEqual({});
  });

  it('keeps them apart from the entries, which are unchanged', () => {
    const { entries } = describeCatalog(WiredEvents);

    expect(Object.keys(entries[0]!).sort()).toEqual(['eventClass', 'handlerName', 'name', 'type']);
    expect(Object.fromEntries(wireNamesOf(WiredEvents))).toEqual({
      OrderPlaced: 'Order Placed',
      OrderCancelled: 'Order Cancelled',
    });
    expect(wireNamesOf(OrderEvents).size).toBe(0);
  });

  it('refuses a wire name for an event the catalog does not have', () => {
    const error = errorOf(() =>
      defineCatalog({ OrderPlaced }, { name: 'Small', wireTypes: { Missing: 'x' } as never }),
    );

    expect(error).toBeInstanceOf(UnknownCatalogEventError);
    expect((error as Error).message).toBe(
      "Catalog 'Small' has no event named 'Missing' to give a wire name; its events are OrderPlaced",
    );
  });

  it('refuses empty and shared wire names', () => {
    expect(
      errorOf(() => defineCatalog({ OrderPlaced }, { wireTypes: { OrderPlaced: '' } })),
    ).toBeInstanceOf(ConfigurationError);
    const error = errorOf(() =>
      defineCatalog(
        { OrderPlaced, OrderShipped },
        { wireTypes: { OrderPlaced: 'x', OrderShipped: 'x' } },
      ),
    );
    expect(error).toBeInstanceOf(WireTypeCollisionError);
    expect((error as Error).message).toBe(
      "Catalog gives events 'OrderPlaced' and 'OrderShipped' the same wire name 'x'",
    );
  });
});

describe('wire names that would clash with what the routes send', () => {
  const placed = produce(OrderEvents, { events: ['OrderPlaced'] });

  it.each([
    [
      'a catalog route',
      route(produce(OrderEvents, { events: ['OrderShipped'] }), { transport: 'broker' }),
    ],
    ['a plain route', { event: OrderShipped, transport: 'broker' }],
  ] satisfies [string, RouteDefinition][])(
    'refuses a wire name that is the type of an event %s sends',
    (_label, other) => {
      const transport = new InMemoryTransport('broker');
      const error = errorOf(() =>
        busWith(
          [
            route(placed, { transport: 'broker', wireTypes: { OrderPlaced: 'order.shipped.v1' } }),
            other,
          ],
          transport,
        ),
      );

      expect(error).toBeInstanceOf(WireTypeCollisionError);
      expect((error as Error).message).toBe(
        "Event types 'order.placed.v1' and 'order.shipped.v1' both travel as 'order.shipped.v1' on transport 'broker'",
      );
      expect(transport.declaredWireTypes).toEqual({});
    },
  );

  it('accepts a wire name that is the type of an event the transport does not send', () => {
    const transport = new InMemoryTransport('broker');
    busWith(
      [route(placed, { transport: 'broker', wireTypes: { OrderPlaced: 'order.shipped.v1' } })],
      transport,
    );

    expect(transport.declaredWireTypes).toEqual({ 'order.placed.v1': 'order.shipped.v1' });
  });
});

describe('declaring wire names changes nothing until the whole configuration is accepted', () => {
  it('leaves the first transport alone when a later one refuses', () => {
    const first = new InMemoryTransport('first');
    const plain: Transport = {
      name: 'plain',
      publish: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const wired = { transport: 'first', wireTypes: { OrderPlaced: 'x' } };

    expect(() =>
      createTestBus({
        transactionMode: 'optional',
        transports: [first, plain],
        routes: [
          route(produce(OrderEvents, { events: ['OrderPlaced'] }), wired),
          route(produce(OrderEvents, { events: ['OrderShipped'] }), {
            transport: 'plain',
            wireTypes: { OrderShipped: 'y' },
          }),
        ],
      }),
    ).toThrow(WireTypesUnsupportedError);
    expect(first.declaredWireTypes).toEqual({});
  });

  it('leaves the transport alone when a handler id is refused', () => {
    const transport = new InMemoryTransport('broker');

    expect(() =>
      busWith(
        [route(produce(WiredEvents), { transport: 'broker', destination: 'bad destination' })],
        transport,
      ),
    ).toThrow(ConfigurationError);
    expect(transport.declaredWireTypes).toEqual({});
  });

  it('refuses a transport that two buses use when either declares wire names', () => {
    const shared = new InMemoryTransport('broker');
    busWith([route(produce(WiredEvents), { transport: 'broker' })], shared);

    expect(() => busWith([route(produce(OrderEvents), { transport: 'broker' })], shared)).toThrow(
      WireTypesUnsupportedError,
    );

    const plainFirst = new InMemoryTransport('broker');
    busWith([route(produce(OrderEvents), { transport: 'broker' })], plainFirst);
    expect(() =>
      busWith([route(produce(WiredEvents), { transport: 'broker' })], plainFirst),
    ).toThrow(
      "Transport 'broker' is used by more than one bus, and wire names are declared for it",
    );
    expect(plainFirst.declaredWireTypes).toEqual({});
  });

  it('lets buses without wire names share a transport, as before', () => {
    const shared = new InMemoryTransport('broker');

    busWith([route(produce(OrderEvents), { transport: 'broker' })], shared);
    expect(() =>
      busWith([route(produce(OrderEvents), { transport: 'broker' })], shared),
    ).not.toThrow();
  });
});

describe('route wire names are read from the route only', () => {
  it('does not take an inherited property for a wire name', () => {
    class Constructor extends defineEvent<{ id: string }>('constructor.called.v1') {}
    const Odd = defineCatalog({
      constructor: Constructor,
      OrderPlaced,
    }) as unknown as typeof OrderEvents;
    const transport = new InMemoryTransport('broker');

    busWith(
      [route(produce(Odd), { transport: 'broker', wireTypes: { OrderPlaced: 'Placed' } })],
      transport,
    );

    expect(transport.declaredWireTypes).toEqual({ 'order.placed.v1': 'Placed' });
  });
});

describe('wire names are applied only when the bus is built', () => {
  it('leaves the transport alone when another option is refused, so the corrected bus can use it', () => {
    const transport = new InMemoryTransport('broker');
    const routes = [route(produce(WiredEvents), { transport: 'broker' })];

    expect(() =>
      createTestBus({
        transactionMode: 'optional',
        transports: [transport],
        routes,
        dispatcher: { concurrency: 0 },
      }),
    ).toThrow(ConfigurationError);
    expect(transport.declaredWireTypes).toEqual({});

    createTestBus({ transactionMode: 'optional', transports: [transport], routes });
    expect(Object.keys(transport.declaredWireTypes)).toHaveLength(2);
  });
});
