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
  consume,
  defineCatalog,
  defineEvent,
  defineHandlers,
  type CatalogDeclaration,
  type CatalogEvents,
  type CatalogPublisher,
  type DomainEvent,
  type EventEnvelope,
  type EventOf,
  type HandlerContext,
  type HandlersOf,
  type InMemoryTransaction,
  type PartialHandlersOf,
  type PayloadOf,
  type SubscribeCatalogOptions,
  produce,
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

  it('keeps the existing bus API unchanged for a shared catalog', () => {
    expectTypeOf<HandlersOf<OrderEvents>>().toEqualTypeOf<
      HandlersOf<OrderEvents, 'OrderPlaced' | 'OrderShipped', unknown>
    >();
    expectTypeOf<PartialHandlersOf<OrderEvents, 'OrderPlaced'>>().toEqualTypeOf<
      HandlersOf<OrderEvents, 'OrderPlaced'>
    >();
  });
});

describe('catalog subsets for services that share a catalog', () => {
  it('lets HandlersOf<C, K> name the events a class consumes', () => {
    class BillingHandlers implements HandlersOf<OrderEvents, 'OrderPlaced'> {
      onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
        void event.payload.orderId;
      }
    }
    expectTypeOf<keyof HandlersOf<OrderEvents, 'OrderPlaced'>>().toEqualTypeOf<'onOrderPlaced'>();
    expectTypeOf<BillingHandlers>().toExtend<HandlersOf<OrderEvents, 'OrderPlaced'>>();

    // @ts-expect-error: subset missing handler, onOrderShipped is required for 'OrderShipped'
    class MissingShipped implements HandlersOf<OrderEvents, 'OrderPlaced' | 'OrderShipped'> {
      onOrderPlaced(): void {}
    }
    expectTypeOf<MissingShipped>().not.toExtend<HandlersOf<OrderEvents>>();

    // @ts-expect-error: unknown event name, 'OrderRefunded' is not an event of OrderEvents
    type Unknown = HandlersOf<OrderEvents, 'OrderRefunded'>;
    expectTypeOf<Unknown>().toBeObject();
  });

  it('types context.transaction through HandlersOf<C, K, Tx>', () => {
    class Transactional implements HandlersOf<OrderEvents, 'OrderPlaced', InMemoryTransaction> {
      onOrderPlaced(
        event: EventOf<typeof OrderPlaced>,
        context: HandlerContext<InMemoryTransaction>,
      ) {
        void [event, context.transaction];
      }
    }
    expectTypeOf<
      Parameters<HandlersOf<OrderEvents, 'OrderPlaced', InMemoryTransaction>['onOrderPlaced']>[1]
    >().toEqualTypeOf<HandlerContext<InMemoryTransaction>>();
    expectTypeOf<Transactional>().toBeObject();
  });

  it('types subscribeCatalog handlers by the selected events', () => {
    const bus = new RelayBus<InMemoryTransaction>({ store: new InMemoryEventStore() });
    // Never called: subscriptions are asserted at compile time only.
    const compileOnly = (): void => {
      bus.subscribeCatalog(
        OrderEvents,
        {
          onOrderPlaced: (event, context) => {
            expectTypeOf(event.payload).toEqualTypeOf<{ orderId: string }>();
            expectTypeOf(context.transaction).toEqualTypeOf<InMemoryTransaction | undefined>();
          },
        },
        { events: ['OrderPlaced'] },
      );

      const placedOnly = { onOrderPlaced: () => undefined };
      bus.subscribeCatalog(
        OrderEvents,
        // @ts-expect-error: subset missing handler, onOrderShipped is required by events
        placedOnly,
        { events: ['OrderPlaced', 'OrderShipped'] },
      );

      // @ts-expect-error: without events every handler of the catalog is required
      bus.subscribeCatalog(OrderEvents, { onOrderPlaced: () => undefined });

      bus.subscribeCatalog(
        OrderEvents,
        { onOrderPlaced: () => undefined, onOrderShipped: () => undefined },
        // @ts-expect-error: unknown event name, 'OrderRefunded' is not an event of OrderEvents
        { events: ['OrderPlaced', 'OrderRefunded'] },
      );

      // An options variable whose events may be absent cannot narrow the required handlers.
      const maybeSelected: SubscribeCatalogOptions<OrderEvents, 'OrderPlaced'> = {
        idPrefix: 'billing',
      };
      // @ts-expect-error: events is optional in the variable's type, so every handler is required
      bus.subscribeCatalog(OrderEvents, { onOrderPlaced: () => undefined }, maybeSelected);
      const untyped: SubscribeCatalogOptions = {};
      // @ts-expect-error: a plain options variable does not widen the catalog, onOrderShipped is required
      bus.subscribeCatalog(OrderEvents, { onOrderPlaced: () => undefined }, untyped);
      const plain: SubscribeCatalogOptions = { idPrefix: 'billing', transactional: true };
      bus.subscribeCatalog(
        OrderEvents,
        { onOrderPlaced: () => undefined, onOrderShipped: () => undefined },
        plain,
      );
      const options: SubscribeCatalogOptions<OrderEvents> = { idPrefix: 'billing' };
      bus.subscribeCatalog(
        OrderEvents,
        { onOrderPlaced: () => undefined, onOrderShipped: () => undefined },
        options,
      );

      // A class instance may carry methods of events it does not subscribe to.
      class Everything implements HandlersOf<OrderEvents> {
        onOrderPlaced(): void {}
        onOrderShipped(): void {}
      }
      bus.subscribeCatalog(OrderEvents, new Everything(), { events: ['OrderShipped'] });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('checks defineHandlers subsets for missing and extra handlers', () => {
    // Never called: the failing calls below are compile-time assertions only.
    const compileOnly = (): void => {
      const billing = defineHandlers(
        OrderEvents,
        { onOrderPlaced: (event) => void event.payload.orderId },
        { events: ['OrderPlaced'] },
      );
      expectTypeOf(billing).toHaveProperty('onOrderPlaced');

      // @ts-expect-error: extra handler in subset, OrderShipped is not selected
      defineHandlers(
        OrderEvents,
        { onOrderPlaced: () => undefined, onOrderShipped: () => undefined },
        { events: ['OrderPlaced'] },
      );

      defineHandlers(
        OrderEvents,
        // @ts-expect-error: subset missing handler, onOrderShipped is required by events
        { onOrderPlaced: () => undefined },
        { events: ['OrderPlaced', 'OrderShipped'] },
      );

      const extra = { onOrderPlaced: () => undefined, onOrderShipped: () => undefined };
      // @ts-expect-error: extra handler in subset, rejected even when the object is not a literal
      defineHandlers(OrderEvents, extra, { events: ['OrderPlaced'] });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('accepts handlers that type their transaction, and keeps payloads strict', () => {
    // Never called: compile-time assertions only.
    const compileOnly = (): void => {
      const transactional = defineHandlers(OrderEvents, {
        onOrderPlaced: (event, context: HandlerContext<InMemoryTransaction>) => {
          void [event.payload.orderId, context.transaction];
        },
        onOrderShipped: () => undefined,
      });
      new RelayBus<InMemoryTransaction>({ store: new InMemoryEventStore() }).subscribeCatalog(
        OrderEvents,
        transactional,
      );
      expectTypeOf(transactional.onOrderPlaced)
        .parameter(1)
        .toEqualTypeOf<HandlerContext<InMemoryTransaction>>();

      defineHandlers(OrderEvents, {
        // @ts-expect-error: the envelope has no 'note', so a narrower payload is rejected
        onOrderPlaced: (event: EventEnvelope<{ orderId: string; note: string }>) => event,
        onOrderShipped: () => undefined,
      });

      // A wider payload type is fine: the handler accepts every OrderPlaced envelope.
      defineHandlers(OrderEvents, {
        onOrderPlaced: (event: EventEnvelope<unknown>) => event,
        onOrderShipped: () => undefined,
      });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('limits a restricted publish view to the produced events', () => {
    const bus = new RelayBus({ store: new InMemoryEventStore() });
    const shippedOnly = bus.forCatalog(OrderEvents, { events: ['OrderShipped'] });
    const typed: CatalogPublisher<OrderEvents, unknown, 'OrderShipped'> = shippedOnly;
    // @ts-expect-error: a view of some events cannot stand in for a view of the whole catalog
    const widened: CatalogPublisher<OrderEvents> = shippedOnly;
    const narrowed: CatalogPublisher<OrderEvents, unknown, 'OrderShipped'> =
      bus.forCatalog(OrderEvents);
    expectTypeOf([widened, narrowed]).toBeArray();
    expectTypeOf(typed).toBeObject();
    expectTypeOf<CatalogEvents<OrderEvents, 'OrderShipped'>>().toEqualTypeOf<OrderShipped>();

    // Never called: publishing is asserted at compile time only.
    const compileOnly = (): void => {
      void shippedOnly.publish(new OrderShipped({ orderId: 'o-1', carrier: 'ups' }));
      // @ts-expect-error: publishing a non-produced event, OrderPlaced is not in the view
      void shippedOnly.publish(new OrderPlaced({ orderId: 'o-1' }));
      // @ts-expect-error: publishing a non-produced event in a batch
      void shippedOnly.publishAll([new OrderPlaced({ orderId: 'o-1' })]);
      // @ts-expect-error: unknown event name in the view
      bus.forCatalog(OrderEvents, { events: ['OrderRefunded'] });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('types consume and produce declarations by catalog keys', () => {
    const consumed = consume(OrderEvents, { events: ['OrderPlaced'] });
    const produced = produce(OrderEvents);
    expectTypeOf(consumed).toEqualTypeOf<CatalogDeclaration<OrderEvents, 'OrderPlaced'>>();
    expectTypeOf(consumed.events).toEqualTypeOf<readonly 'OrderPlaced'[]>();
    expectTypeOf(produced.events).toEqualTypeOf<readonly ('OrderPlaced' | 'OrderShipped')[]>();
    const bus = new RelayBus({ store: new InMemoryEventStore(), catalogs: [consumed, produced] });
    bus.registerCatalog(OrderEvents);

    // Never called: compile-time assertions only.
    const compileOnly = (): void => {
      // @ts-expect-error: unknown event name, 'OrderRefunded' is not an event of OrderEvents
      consume(OrderEvents, { events: ['OrderRefunded'] });
      // @ts-expect-error: unknown event name, 'OrderRefunded' is not an event of OrderEvents
      produce(OrderEvents, { events: ['OrderPlaced', 'OrderRefunded'] });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });
});
