---
title: Compile-time safety with event catalogs in relaybus
description: Group events in a catalog so the TypeScript compiler rejects a missing handler, a wrong payload or an event published to the wrong catalog, and a boot check catches handlers that were never registered.
---

# Compile-time safety with event catalogs

An event with no handler is the quietest failure in an event-driven system: the publish succeeds, nothing runs, and nobody notices until a customer asks why the shipping email never arrived. A **catalog** groups the events of one domain so that the compiler, and then the application boot, tell you before production does.

Catalogs are **optional, stricter checking** on top of the default style. Method-level `@RelayBusHandler(Event)` handlers already have their parameter type checked against the event at compile time, need no class-level setup, and can mix events of any domain in one class; start there. Add a catalog when a team wants the compiler and the boot to prove that every event of a shared contract has a handler or a route.

Compiler output below is abridged: `OrderEvents` stands for the expanded type of the catalog that TypeScript prints.

## Before: nothing connects events and handlers

```ts
class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>('order.shipped.v1') {}

@Injectable()
class ShippingHandlers {
  @RelayBusHandler(OrderPlaced)
  onOrderPlaced(event: EventEnvelope<{ orderId: string }>) {}
  // OrderShipped is forgotten. Everything compiles, the app boots, shipments are silently ignored.
}
```

## After: the catalog makes the gaps visible

```ts
export const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });

@Injectable()
@RelayBusConsumer(OrderEvents)
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
    { event: OrderPlaced, transport: 'kafka', destination: 'orders' },
    { event: OrderCancelled, transport: 'kafka', destination: 'orders' },
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
@RelayBusConsumer(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'], transactional: true })
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

// @RelayBusConsumer(OrderEvents, { events: ['OrderPlaced', 'OrderShipped'] }) on a class without onOrderShipped
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
