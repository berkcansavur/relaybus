---
title: Compile-time safety with event catalogs in relaybus
description: Group events in a catalog so the TypeScript compiler rejects a missing handler, a wrong payload or an event published to the wrong catalog, and a boot check catches handlers that were never registered.
---

# Compile-time safety with event catalogs

An event with no handler is the quietest failure in an event-driven system: the publish succeeds, nothing runs, and nobody notices until a customer asks why the shipping email never arrived. A **catalog** groups the events of one domain so that the compiler, and then the application boot, tell you before production does.

Compiler output below is abridged: `OrderEvents` stands for the expanded type of the catalog that TypeScript prints.

## Before: nothing connects events and handlers

```ts
class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>('order.shipped.v1') {}

@Injectable()
class ShippingHandlers {
  @Subscribe(OrderPlaced)
  onOrderPlaced(event: EventEnvelope<{ orderId: string }>) {}
  // OrderShipped is forgotten. Everything compiles, the app boots, shipments are silently ignored.
}
```

## After: the catalog makes the gaps visible

```ts
export const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });

@Injectable()
@EventConsumer(OrderEvents)
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
error TS2345: Property 'onOrderShipped' is missing in type '{ onOrderPlaced: () => undefined; }' but required in type 'HandlersOf<OrderEvents, unknown>'.

// onOrderRefunded is not an event of the catalog
error TS2322: Type '() => undefined' is not assignable to type 'never'.
```

A consumer that deliberately handles only some events says so with `PartialHandlersOf<typeof OrderEvents, 'OrderShipped'>`.

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

## Related

- [Deliveries](/concepts/deliveries)
- [@relaybus/core](/packages/core)
- [@relaybus/nestjs](/packages/nestjs)
