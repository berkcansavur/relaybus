/**
 * Compile-time contract of event catalogs. These tests are enforced by
 * `tsc -p tsconfig.json` (the repo typecheck): every `@ts-expect-error` line
 * must produce a compile error, otherwise tsc reports an unused directive.
 * Vitest only checks that the file loads, so nothing here runs a handler.
 */
import { describe, expectTypeOf, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  defineCatalog,
  defineEvent,
  defineHandlers,
  type CatalogEvents,
  type CatalogPublisher,
  type DomainEvent,
  type EventEnvelope,
  type EventOf,
  type HandlerContext,
  type HandlersOf,
  type PartialHandlersOf,
  type PayloadOf,
} from '../src/index.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>('order.shipped.v1') {}
class UserRegistered extends defineEvent<{ userId: number }>('user.registered.v1') {}

const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });
type OrderEvents = typeof OrderEvents;

describe('catalog types', () => {
  it('derives event helpers from the catalog', () => {
    expectTypeOf<EventOf<typeof OrderPlaced>>().toEqualTypeOf<EventEnvelope<{ orderId: string }>>();
    expectTypeOf<CatalogEvents<OrderEvents>>().toEqualTypeOf<OrderPlaced | OrderShipped>();
    expectTypeOf<PayloadOf<OrderEvents['OrderShipped']>>().toEqualTypeOf<{
      orderId: string;
      carrier: string;
    }>();
  });

  it('requires one on<Key> method per event in HandlersOf', () => {
    expectTypeOf<HandlersOf<OrderEvents>>().toHaveProperty('onOrderPlaced');
    expectTypeOf<HandlersOf<OrderEvents>>().toHaveProperty('onOrderShipped');
    expectTypeOf<HandlersOf<OrderEvents>['onOrderPlaced']>().toEqualTypeOf<
      (event: EventEnvelope<{ orderId: string }>, context: HandlerContext) => unknown
    >();
    expectTypeOf<keyof HandlersOf<OrderEvents>>().toEqualTypeOf<
      'onOrderPlaced' | 'onOrderShipped'
    >();
  });

  it('accepts a class that implements every handler', () => {
    class Complete implements HandlersOf<OrderEvents> {
      onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
        void event.payload.orderId;
      }
      async onOrderShipped(event: EventOf<typeof OrderShipped>, context: HandlerContext) {
        void [event.payload.carrier, context.attempt];
      }
    }
    expectTypeOf<Complete>().toExtend<HandlersOf<OrderEvents>>();
  });

  it('rejects a class that misses a handler (TS2420)', () => {
    // @ts-expect-error: Class 'Incomplete' incorrectly implements ... Property 'onOrderShipped' is missing
    class Incomplete implements HandlersOf<OrderEvents> {
      onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
        void event;
      }
    }
    expectTypeOf<Incomplete>().not.toExtend<HandlersOf<OrderEvents>>();
  });

  it('rejects a handler with the wrong payload type', () => {
    class WrongPayload implements HandlersOf<OrderEvents> {
      // @ts-expect-error: onOrderPlaced receives { orderId: string }, not { orderId: number }
      onOrderPlaced(event: EventEnvelope<{ orderId: number }>): void {
        void event;
      }
      onOrderShipped(event: EventOf<typeof OrderShipped>): void {
        void event;
      }
    }
    expectTypeOf<WrongPayload>().not.toExtend<HandlersOf<OrderEvents>>();
  });

  it('rejects a misspelled handler name', () => {
    // @ts-expect-error: onOrderShiped is not onOrderShipped
    class Misspelled implements HandlersOf<OrderEvents> {
      onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
        void event;
      }
      onOrderShiped(event: EventOf<typeof OrderShipped>): void {
        void event;
      }
    }
    expectTypeOf<Misspelled>().not.toExtend<HandlersOf<OrderEvents>>();
  });

  it('lets PartialHandlersOf name the subset a consumer handles', () => {
    class AuditOnly implements PartialHandlersOf<OrderEvents, 'OrderShipped'> {
      onOrderShipped(event: EventOf<typeof OrderShipped>): void {
        void event;
      }
    }
    expectTypeOf<AuditOnly>().toExtend<PartialHandlersOf<OrderEvents, 'OrderShipped'>>();
    expectTypeOf<
      keyof PartialHandlersOf<OrderEvents, 'OrderShipped'>
    >().toEqualTypeOf<'onOrderShipped'>();
    // @ts-expect-error: 'Refunded' is not an event of the catalog
    type Unknown = PartialHandlersOf<OrderEvents, 'Refunded'>;
    expectTypeOf<Unknown>().toBeObject();
  });

  it('checks defineHandlers for completeness, extras and payload types', () => {
    // Never called: the failing calls below are compile-time assertions only.
    const compileOnly = (): void => {
      const complete = defineHandlers(OrderEvents, {
        onOrderPlaced: (event) => {
          expectTypeOf(event).toEqualTypeOf<EventEnvelope<{ orderId: string }>>();
        },
        onOrderShipped: (event, context) => {
          expectTypeOf(event.payload.carrier).toBeString();
          expectTypeOf(context).toEqualTypeOf<HandlerContext>();
        },
      });
      expectTypeOf(complete).toHaveProperty('onOrderShipped');

      // @ts-expect-error: onOrderShipped is missing
      defineHandlers(OrderEvents, { onOrderPlaced: () => undefined });

      defineHandlers(OrderEvents, {
        onOrderPlaced: () => undefined,
        onOrderShipped: () => undefined,
        // @ts-expect-error: onOrderRefunded is not a handler of the catalog
        onOrderRefunded: () => undefined,
      });

      const withExtra = {
        onOrderPlaced: () => undefined,
        onOrderShipped: () => undefined,
        onOrderRefunded: () => undefined,
      };
      // @ts-expect-error: extra keys are rejected even when the object is not a literal
      defineHandlers(OrderEvents, withExtra);

      defineHandlers(OrderEvents, {
        onOrderPlaced: (event) => {
          // @ts-expect-error: payload has no 'missingField'
          void event.payload.missingField;
        },
        onOrderShipped: () => undefined,
      });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('limits a catalog publisher to the events of its catalog', () => {
    const bus = new RelayBus({ store: new InMemoryEventStore() });
    const orders = bus.forCatalog(OrderEvents);
    const typed: CatalogPublisher<OrderEvents> = orders;
    expectTypeOf(typed).toBeObject();

    // Never called: publishing is asserted at compile time only.
    const compileOnly = (): void => {
      void orders.publish(new OrderPlaced({ orderId: 'o-1' }));
      void orders.publishAll([
        new OrderPlaced({ orderId: 'o-1' }),
        new OrderShipped({ orderId: 'o-1', carrier: 'ups' }),
      ]);

      // @ts-expect-error: UserRegistered is not part of OrderEvents
      void orders.publish(new UserRegistered({ userId: 1 }));
      const mixed = [new OrderPlaced({ orderId: 'o-1' }), new UserRegistered({ userId: 1 })];
      // @ts-expect-error: UserRegistered is not part of OrderEvents
      void orders.publishAll(mixed);
      // @ts-expect-error: the payload must match the event class
      void new OrderPlaced({ orderId: 1 });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('keeps the existing bus API unchanged', () => {
    const bus = new RelayBus({ store: new InMemoryEventStore() });
    expectTypeOf(bus.publish<{ userId: number }>)
      .parameter(0)
      .toExtend<DomainEvent<{ userId: number }>>();
  });
});
