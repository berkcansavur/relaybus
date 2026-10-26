---
title: Compile-time safety with event catalogs in relaybus
description: Group events in a catalog so the TypeScript compiler rejects a missing handler, a wrong payload or an event published to the wrong catalog, and a boot check catches handlers that were never registered.
---

# Compile-time safety with event catalogs

An event with no handler is the quietest failure in an event-driven system: the publish succeeds, nothing runs, and nobody notices until a customer asks why the shipping email never arrived. A **catalog** groups the events of one domain so that the compiler, and then the application boot, tell you before production does.

Catalogs are **optional, stricter checking** on top of the default style. Method-level `@RelayBusConsumer(Event)` handlers already have their parameter type checked against the event at compile time, need no class-level setup, and can mix events of any domain in one class; start there. Add a catalog when a team wants the compiler and the boot to prove that every event of a shared contract has a handler or a route.

Compiler output below is abridged: `OrderEvents` stands for the expanded type of the catalog that TypeScript prints.

## Before: nothing connects events and handlers

```ts
class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>('order.shipped.v1') {}

@Injectable()
class ShippingHandlers {
  @RelayBusConsumer(OrderPlaced)
  onOrderPlaced(event: EventEnvelope<{ orderId: string }>) {}
  // OrderShipped is forgotten. Everything compiles, the app boots, shipments are silently ignored.
}
```

## After: the catalog makes the gaps visible

```ts
export const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });

@Injectable()
@RelayBusCatalogConsumer(OrderEvents)
class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
}
```

```text
error TS2420: Class 'ShippingHandlers' incorrectly implements interface 'HandlersOf<OrderEvents>'.
  Property 'onOrderShipped' is missing in type 'ShippingHandlers' but required in type 'HandlersOf<OrderEvents>'.
```

Add the method and the error is gone. `EventOf<typeof OrderShipped>` is `EventEnvelope<{ orderId: string; carrier: string }>`, so the payload is checked too:

```ts
onOrderPlaced(event: EventEnvelope<{ orderId: number }>) {}
```

```text
error TS2416: Property 'onOrderPlaced' in type 'ShippingHandlers' is not assignable to the same property in base type 'HandlersOf<OrderEvents>'.
  Type '(event: EventEnvelope<{ orderId: number; }>) => void' is not assignable to type '(event: EventOf<typeof OrderPlaced>, context: HandlerContext<unknown>) => unknown'.
    Types of property 'orderId' are incompatible.
      Type 'string' is not assignable to type 'number'.
```

## Handlers as an object

Without NestJS, `defineHandlers` declares the handlers of a catalog and `bus.subscribeCatalog` registers them under the stable ids `${prefix}.on${EventName}`:

```ts
const handlers = defineHandlers(OrderEvents, {
  onOrderPlaced: (event) => reserveStock(event.payload.orderId),
  onOrderShipped: (event) => notify(event.payload.carrier),
});
bus.subscribeCatalog(OrderEvents, handlers, { idPrefix: 'fulfilment' });
```

```text
// onOrderShipped missing
error TS2345: Argument of type '{ onOrderPlaced: () => undefined; }' is not assignable to parameter of type 'HandlerShapeOf<OrderEvents, "OrderPlaced" | "OrderShipped"> & MatchingHandlers<...>'.
  Property 'onOrderShipped' is missing in type '{ onOrderPlaced: () => undefined; }' but required in type 'HandlerShapeOf<OrderEvents, "OrderPlaced" | "OrderShipped">'.

// onOrderRefunded is not an event of the catalog
error TS2322: Type '() => undefined' is not assignable to type 'never'.
```

A consumer that deliberately handles only some events says so with `HandlersOf<typeof OrderEvents, 'OrderShipped'>`, `defineHandlers(OrderEvents, handlers, { events: ['OrderShipped'] })` or `bus.subscribeCatalog(OrderEvents, handlers, { events: ['OrderShipped'] })`; see the microservices section below.

## Publishing only what belongs to the catalog

```ts
const orders = bus.forCatalog(OrderEvents);

await orders.publish(new OrderPlaced({ orderId }), { transaction });
await orders.publish(new UserRegistered({ userId })); // compile error
```

```text
error TS2345: Argument of type 'UserRegistered' is not assignable to parameter of type 'OrderPlaced | OrderShipped'.
  Types of property 'payload' are incompatible.
    Type '{ userId: number; }' is missing the following properties from type '{ orderId: string; carrier: string; }': orderId, carrier
```

TypeScript compares classes by shape, so two events with identical payload types look the same to the compiler. The view therefore also checks the event type at runtime and throws a `ConfigurationError` before anything is stored. The existing `bus.publish` is unchanged.

## The boot check

Types cannot see a module that was never imported or a handler that was never registered. Pass the catalogs to the bus (`new RelayBus({ catalogs })`, `bus.registerCatalog(catalog)` or `RelayBusModule.forRoot({ catalogs: [OrderEvents] })`) and `start()` verifies that every event has at least one handler or route:

```text
ConfigurationError: Catalog events without a handler or route:
  - OrderEvents.OrderShipped (order.shipped.v1): expected handler method onOrderShipped
```

The check runs before the store is set up or any transport connects, so a failed check has no side effects and the NestJS application simply does not boot.

## Routing a catalog production

A service that publishes several events of a catalog to one topic does not need a route per event. `route(produce(catalog, { events }), settings)` turns every selected event into a route with the same `transport`, `destination`, `retry` and `timeout`:

```ts
import { produce, route } from '@relaybus/core';

new RelayBus({
  store,
  transports: [kafka],
  routes: [
    route(produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }), {
      transport: 'kafka',
      destination: 'orders',
      retry: { initialDelay: '500ms', maxDelay: '30s' },
      timeout: '10s',
    }),
    { event: OrderShipped, transport: 'kafka', destination: 'shipments' }, // a plain route still works
  ],
});
```

- **Same as one route per event.** The result is exactly the routes you would have written by hand: one handler `relaybus.transport:<transport>:<destination>` per destination, so events of one key reach the broker in commit order whatever their types, and `bus.handlers()` lists it once per event type. Without `events`, every event of the catalog is routed.
- **`destination` is optional.** Without it every event goes to its own event type, as a plain route does. With it, all selected events share it.
- **It covers `produce`.** A route made this way is a route like any other, so `catalogs: [produce(OrderEvents, ...)]` and `RelayBusModule.forFeature({ produces })` see the events as routed. Routing fewer events than a `produce` declaration lists still fails the boot check, naming the events left out.
- **Mix freely.** Catalog routes and plain routes can share a destination when they resolve to the same `retry` and `timeout`, as plain routes always had to.

The boot refuses a configuration that cannot work, before any transport connects. The errors extend `ConfigurationError`, so existing `catch` blocks keep matching, and carry their own [code](/concepts/error-codes):

| Mistake                                                                       | Error                       | Code                              |
| ----------------------------------------------------------------------------- | --------------------------- | --------------------------------- |
| An event routed twice to one destination, when a catalog route is involved    | `DuplicateRouteEventError`  | `RELAYBUS_ROUTE_DUPLICATE_EVENT`  |
| A wire name for an event the catalog (or the route's selection) does not have | `UnknownCatalogEventError`  | `RELAYBUS_CATALOG_UNKNOWN_EVENT`  |
| One event with two wire names on a transport, or two events with one          | `WireTypeCollisionError`    | `RELAYBUS_WIRE_TYPE_COLLISION`    |
| Wire names for a transport or codec that cannot map type names                | `WireTypesUnsupportedError` | `RELAYBUS_WIRE_TYPES_UNSUPPORTED` |

Two plain routes to the same event and destination keep failing with the `ConfigurationError` they always did. An unknown event name in `produce(catalog, { events })` does not compile and fails with the `ConfigurationError` of `produce`.

## Wire names next to the catalog and the route

Some wire formats name event types differently from relaybus (`Order Placed` instead of `order.placed.v1`). Declare those names where the event or the route is, not only on the codec:

```ts
export const OrderEvents = defineCatalog(
  { OrderPlaced, OrderCancelled, OrderShipped },
  {
    name: 'OrderEvents',
    wireTypes: { OrderPlaced: 'Order Placed', OrderCancelled: 'Order Cancelled' },
  },
);

new RelayBus({
  store,
  transports: [
    new KafkaTransport({
      kafka,
      codec: jsonEnvelope({
        type: { body: 'eventName' },
        id: { body: 'messageId' },
        payload: { body: 'data' },
      }),
    }),
  ],
  routes: [
    route(produce(OrderEvents), {
      transport: 'kafka',
      destination: 'orders',
      wireTypes: { OrderShipped: 'Order Shipped' }, // the route adds to the catalog's names, and wins on a clash
    }),
  ],
});
```

The names of the events a transport sends reach its codec when the bus is created, and the codec writes and reads those events under them, byte for byte as `jsonEnvelope`'s own `wireTypes` would (a contract test in the Kafka and RabbitMQ packages proves it). Keys are catalog event names, checked by the compiler; `route` can name only the events it routes. Events without a name travel under their event type. Rules:

- A codec has one set of names per transport: the same event cannot have two names on a transport, and no two events can share one. An event without a name travels as its event type, so a name equal to the type of another event the transport sends is refused too (`WireTypeCollisionError`). A clash with the `wireTypes` of the codec itself fails too (`WireTypeCollisionError`); naming the same pair in both places is fine.
- The transport needs a codec that maps type names: `jsonEnvelope` with a type at `{ body }` or `{ header }`. The default CloudEvents codecs, a `fixed` type and a hand-written codec refuse wire names at boot (`WireTypesUnsupportedError`). A transport written outside relaybus takes them through the optional `declareWireTypes` method of `Transport`, which checks the names and returns the function that applies them, so a refused configuration changes no transport.
- The names live in the transport's codec, so they belong to the transport, not to a bus: two buses that use one transport instance cannot declare wire names for it (`WireTypesUnsupportedError`; give each bus its own transport), and a transport that already connected or subscribed refuses them. Buses that share a transport without declaring any are unaffected.
- Wire names of a catalog are used only for the events a route sends. A service that only consumes a catalog has no route to declare them, so give its codec `wireTypes: catalogWireTypes(OrderEvents)`, which lists the catalog's names by event type; names that only a route declared go next to it.

## NestJS: module contracts

In NestJS the recommended form is a contract per feature module: `RelayBusModule.forFeature({ consumes: [OrderEvents], produces: [produce(BillingEvents)] })` in the module's `imports`. At boot every consumed event needs a method-level `@RelayBusConsumer` handler among the providers of that module (free method names, any number of classes), and every produced event a route; otherwise the application fails to start naming the module and each missing event. See [@relaybus/nestjs](/packages/nestjs#module-contracts-relaybusmodule-forfeature).

## Microservices: shared catalogs

Between services, a catalog is a contract: it lives in a shared package, one service produces an event and another consumes it. Each service only covers its part, so instead of a bare catalog (every event needs a handler or route here) it declares its role per event.

```ts
// @acme/contracts
export const OrderEvents = defineCatalog(
  { OrderPlaced, OrderCancelled, OrderShipped },
  { name: 'OrderEvents' },
);
```

**Producer service** (orders): publishes `OrderPlaced` and `OrderCancelled`, handles `OrderShipped`.

```ts
const bus = new RelayBus({
  store: new PostgresEventStore({ pool }),
  transports: [kafka],
  routes: [
    route(produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }), {
      transport: 'kafka',
      destination: 'orders',
    }),
  ],
  subscriptions: [{ transport: 'kafka', destinations: ['shipments'] }],
  catalogs: [
    produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }),
    consume(OrderEvents, { events: ['OrderShipped'] }),
  ],
});

bus.subscribeCatalog(
  OrderEvents,
  { onOrderShipped: (event) => markShipped(event.payload.orderId) },
  { events: ['OrderShipped'] },
);

const orders = bus.forCatalog(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] });
await orders.publish(new OrderPlaced({ orderId, total }), { transaction, key: orderId });
```

**Consumer service** (shipping, NestJS): handles `OrderPlaced` and `OrderCancelled`, publishes `OrderShipped`.

```ts
@Injectable()
@RelayBusCatalogConsumer(OrderEvents, {
  events: ['OrderPlaced', 'OrderCancelled'],
  transactional: true,
})
export class ShippingHandlers {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>, context: HandlerContext<ClientSession>) {}
  onOrderCancelled(event: EventOf<typeof OrderCancelled>, context: HandlerContext<ClientSession>) {}
}

RelayBusModule.forRoot({
  store: new MongoEventStore({ client }),
  transports: [kafka],
  subscriptions: [{ transport: 'kafka', destinations: ['orders'] }],
  routes: [{ event: OrderShipped, transport: 'kafka', destination: 'shipments' }],
  catalogs: [
    consume(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }),
    produce(OrderEvents, { events: ['OrderShipped'] }),
  ],
});
```

What each declaration checks:

| Declaration                     | Each listed event needs | Does not count  |
| ------------------------------- | ----------------------- | --------------- |
| `consume(catalog, { events? })` | a local handler         | a route         |
| `produce(catalog, { events? })` | a route to a transport  | a local handler |
| `catalog` (bare)                | a handler or a route    |                 |

The compiler catches the mistakes it can see:

```text
// unknown event name
error TS2322: Type '"OrderRefunded"' is not assignable to type '"OrderPlaced" | "OrderCancelled" | "OrderShipped"'.

// subset missing handler: events lists OrderShipped but the handlers object has no onOrderShipped
error TS2345: Argument of type '{ onOrderPlaced: () => undefined; }' is not assignable to parameter of type 'HandlersOf<OrderEvents, "OrderPlaced" | "OrderShipped", PoolClient>'.

// publishing an event the view does not produce
error TS2345: Argument of type 'OrderPlaced' is not assignable to parameter of type 'OrderShipped'.

// @RelayBusCatalogConsumer(OrderEvents, { events: ['OrderPlaced', 'OrderShipped'] }) on a class without onOrderShipped
error TS1238: Unable to resolve signature of class decorator when called as an expression.
```

and the boot check catches the rest, per event:

```text
ConfigurationError: Catalog events this service does not cover:
  - OrderEvents.OrderPlaced (order.placed.v1): consumed here, needs handler method onOrderPlaced
  - OrderEvents.OrderShipped (order.shipped.v1): produced here, needs a route to a transport; a local handler does not deliver it to other services
```

On the consuming side, events arrive through the transport (`bus.receive`). They are matched to the catalog handlers by event type, stored once per event id (a broker redelivery is a no-op) and validated against the event's schema before the handler runs; a payload that fails validation is dead-lettered.

Every route to one destination shares one queue per key, so events of one key reach the broker in publish order whatever their types, even when a send is retried; the consumer receives them in that order when the broker keeps it (Kafka: same partition, which the key decides; RabbitMQ: `ordered: true`). On the consuming side, order holds per handler: the handlers of two event types run independently and can interleave; put a version or timestamp in the payload when one handler depends on another's effect.

## Related

- [Deliveries](/concepts/deliveries)
- [@relaybus/core](/packages/core)
- [@relaybus/nestjs](/packages/nestjs)
