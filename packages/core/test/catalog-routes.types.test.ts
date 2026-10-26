/**
 * Compile-time contract of catalog routes. Enforced by `tsc -p tsconfig.json`: every
 * `@ts-expect-error` line must produce a compile error. Vitest only checks that the file loads.
 */
import { describe, expectTypeOf, it } from 'vitest';
import {
  InMemoryEventStore,
  defineCatalog,
  defineEvent,
  produce,
  route,
  type CatalogRoute,
  type RelayBusOptions,
  type RouteDefinition,
} from '../src/index.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

const OrderEvents = defineCatalog(
  { OrderPlaced, OrderShipped },
  { name: 'OrderEvents', wireTypes: { OrderPlaced: 'Order Placed' } },
);

describe('catalog route types', () => {
  it('creates a CatalogRoute that the routes option accepts next to plain routes', () => {
    const created = route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
      transport: 'kafka',
      destination: 'orders',
      retry: { initialDelay: '1s' },
      timeout: '5s',
    });
    expectTypeOf(created).toEqualTypeOf<CatalogRoute>();
    expectTypeOf<NonNullable<RelayBusOptions['routes']>[number]>().toEqualTypeOf<RouteDefinition>();
    const options: RelayBusOptions = {
      store: new InMemoryEventStore(),
      routes: [created, { event: OrderShipped, transport: 'kafka' }],
    };
    expectTypeOf(options.routes).toEqualTypeOf<readonly RouteDefinition[] | undefined>();
  });

  it('types the wire names by the selected event names', () => {
    // Never called: these calls only have to compile, or fail to.
    const compileOnly = (): void => {
      route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
        transport: 'kafka',
        wireTypes: { OrderPlaced: 'Order Placed' },
      });
      route(produce(OrderEvents, { events: ['OrderPlaced'] }), {
        transport: 'kafka',
        // @ts-expect-error OrderShipped is not selected
        wireTypes: { OrderShipped: 'Order Shipped' },
      });
      route(produce(OrderEvents), {
        transport: 'kafka',
        // @ts-expect-error Missing is not an event of the catalog
        wireTypes: { Missing: 'x' },
      });
      // @ts-expect-error a route needs a transport
      route(produce(OrderEvents), { destination: 'orders' });
      // @ts-expect-error a route of a catalog names no event
      route(produce(OrderEvents), { transport: 'kafka', event: OrderPlaced });
    };
    void compileOnly;
  });

  it('types the wire names of a catalog by its event names', () => {
    const compileOnly = (): void => {
      defineCatalog({ OrderPlaced }, { wireTypes: { OrderPlaced: 'x' } });
      // @ts-expect-error Missing is not an event of the catalog
      defineCatalog({ OrderPlaced }, { wireTypes: { Missing: 'x' } });
    };
    void compileOnly;
  });
});
