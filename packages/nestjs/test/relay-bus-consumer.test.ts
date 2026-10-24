import { Inject, Injectable, Scope } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import {
  ConfigurationError,
  InMemoryEventStore,
  RelayBus,
  consume,
  defineCatalog,
  defineEvent,
  produce,
  type EventEnvelope,
  type EventOf,
  type HandlerContext,
  type HandlersOf,
  type InMemoryTransaction,
  type PartialHandlersOf,
} from '@relaybus/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RelayBusConsumer, RelayBusHandler } from '../src/index.js';
import { OrderPlaced, createApplication, manualOptions, relayBusRoot } from './helpers.js';

class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}
class PaymentTaken extends defineEvent<{ orderId: string }>('payment.taken.v1') {}

const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });
const PaymentEvents = defineCatalog({ PaymentTaken });

afterEach(() => {
  vi.restoreAllMocks();
});

async function start(moduleRef: TestingModule): Promise<RelayBus<InMemoryTransaction>> {
  await moduleRef.init();
  return moduleRef.get(RelayBus);
}

function publishPlaced(bus: RelayBus<InMemoryTransaction>, orderId: string): Promise<unknown> {
  return bus.transaction((transaction) =>
    bus.publish(new OrderPlaced({ orderId }), { transaction }),
  );
}

describe('@RelayBusConsumer discovery', () => {
  it('registers every on<Key> method under ClassName.method with this bound', async () => {
    @Injectable()
    class Mailer {
      readonly sent: string[] = [];
    }
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
      readonly seen: string[] = [];
      constructor(@Inject(Mailer) private readonly mailer: Mailer) {}

      onOrderPlaced(event: EventOf<typeof OrderPlaced>, context: HandlerContext): void {
        this.seen.push(`placed:${event.payload.orderId}`);
        this.mailer.sent.push(context.handler);
      }

      onOrderShipped(event: EventOf<typeof OrderShipped>): void {
        this.seen.push(`shipped:${event.payload.orderId}`);
      }

      helper(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [Mailer, ShippingHandlers],
    });
    const bus = await start(moduleRef);

    await bus.transaction(async (transaction) => {
      await bus.publishAll(
        [new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })],
        {
          transaction,
        },
      );
    });
    await bus.drain();

    expect(bus.handlers().map((handler) => [handler.id, handler.eventType])).toEqual([
      ['ShippingHandlers.onOrderPlaced', 'order.placed.v1'],
      ['ShippingHandlers.onOrderShipped', 'order.shipped.v1'],
    ]);
    expect(moduleRef.get(ShippingHandlers).seen.sort()).toEqual(['placed:1', 'shipped:1']);
    expect(moduleRef.get(Mailer).sent).toEqual(['ShippingHandlers.onOrderPlaced']);
    await moduleRef.close();
  });

  it('passes the decorator options to every handler of the class', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents, { transactional: true, concurrency: 2 })
    class Handlers implements HandlersOf<typeof OrderEvents> {
      onOrderPlaced(): void {}
      onOrderShipped(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => [handler.transactional, handler.concurrency])).toEqual([
      [true, 2],
      [true, 2],
    ]);
    await moduleRef.close();
  });

  it('handles the subset it defines, as PartialHandlersOf describes', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class AuditHandlers implements PartialHandlersOf<typeof OrderEvents, 'OrderShipped'> {
      onOrderShipped(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [AuditHandlers],
    });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => handler.id)).toEqual(['AuditHandlers.onOrderShipped']);
    await moduleRef.close();
  });

  it('works next to @RelayBusHandler methods of the same class', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Handlers {
      readonly seen: string[] = [];

      onOrderPlaced(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(`consumer:${event.payload.orderId}`);
      }

      @RelayBusHandler(OrderPlaced)
      audit(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(`subscribe:${event.payload.orderId}`);
      }

      @RelayBusHandler(OrderPlaced, { id: 'explicit.placed' })
      explicitAudit(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });
    const bus = await start(moduleRef);

    await publishPlaced(bus, '1');
    await bus.drain();

    expect(
      bus
        .handlers()
        .map((handler) => handler.id)
        .sort(),
    ).toEqual(['Handlers.audit', 'Handlers.onOrderPlaced', 'explicit.placed']);
    expect(moduleRef.get(Handlers).seen.sort()).toEqual(['consumer:1', 'subscribe:1']);
    await moduleRef.close();
  });

  it('rejects a @RelayBusHandler that claims the id of a consumer method', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });

    await expect(moduleRef.init()).rejects.toThrow(
      new ConfigurationError(
        "Handler id 'Handlers.onOrderPlaced' of @RelayBusConsumer(OrderEvents) is already used by " +
          'Handlers.onOrderPlaced; give the @RelayBusHandler a unique { id } or drop it',
      ),
    );
  });

  it('accepts the same method with a @RelayBusHandler that has its own id', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Handlers {
      @RelayBusHandler(OrderPlaced, { id: 'audit.placed' })
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });
    const bus = await start(moduleRef);

    expect(
      bus
        .handlers()
        .map((handler) => handler.id)
        .sort(),
    ).toEqual(['Handlers.onOrderPlaced', 'audit.placed']);
    await moduleRef.close();
  });

  it('names an unnamed catalog generically when a class defines none of its methods', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Misspelled {
      onOrderShiped(): void {}
    }
    @Injectable()
    @RelayBusConsumer(PaymentEvents)
    class Unnamed {}
    const named = await createApplication({ imports: [relayBusRoot()], providers: [Misspelled] });
    const unnamed = await createApplication({ imports: [relayBusRoot()], providers: [Unnamed] });

    await expect(named.init()).rejects.toThrow(
      'Misspelled uses @RelayBusConsumer(OrderEvents) but defines none of its handler methods ' +
        '(onOrderPlaced, onOrderShipped)',
    );
    await expect(unnamed.init()).rejects.toThrow(
      'Unnamed uses @RelayBusConsumer(catalog) but defines none of its handler methods (onPaymentTaken)',
    );
  });

  it('supports several catalogs on one class and rejects colliding method ids', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    @RelayBusConsumer(PaymentEvents)
    class Handlers {
      onOrderPlaced(): void {}
      onPaymentTaken(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => handler.id)).toEqual([
      'Handlers.onPaymentTaken',
      'Handlers.onOrderPlaced',
    ]);
    await moduleRef.close();

    const SameKeys = defineCatalog({ OrderPlaced: PaymentTaken });
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    @RelayBusConsumer(SameKeys)
    class Colliding {
      onOrderPlaced(): void {}
    }
    const collision = await createApplication({
      imports: [relayBusRoot()],
      providers: [Colliding],
    });
    await expect(collision.init()).rejects.toThrow(/Handler id 'Colliding.onOrderPlaced'/);
  });

  it('inherits the decoration in subclasses and uses the subclass name in ids', async () => {
    @RelayBusConsumer(OrderEvents)
    class BaseHandlers {
      onOrderPlaced(): void {}
    }
    @Injectable()
    class ShippingHandlers extends BaseHandlers {}
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [ShippingHandlers],
    });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => handler.id)).toEqual(['ShippingHandlers.onOrderPlaced']);
    await moduleRef.close();
  });

  it('registers once when a useExisting alias points at the provider', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Handlers {
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [Handlers, { provide: 'ALIAS', useExisting: Handlers }],
    });
    const bus = await start(moduleRef);

    expect(bus.handlers()).toHaveLength(1);
    await moduleRef.close();
  });

  it('rejects request-scoped and transient providers', async () => {
    @Injectable({ scope: Scope.REQUEST })
    @RelayBusConsumer(OrderEvents)
    class RequestHandlers {
      onOrderPlaced(): void {}
    }
    @Injectable({ scope: Scope.TRANSIENT })
    @RelayBusConsumer(OrderEvents)
    class TransientHandlers {
      onOrderPlaced(): void {}
    }
    const request = await createApplication({
      imports: [relayBusRoot()],
      providers: [RequestHandlers],
    });
    const transient = await createApplication({
      imports: [relayBusRoot()],
      providers: [TransientHandlers],
    });

    await expect(request.init()).rejects.toThrow(
      'RequestHandlers uses @RelayBusConsumer but its provider is request-scoped or transient',
    );
    await expect(transient.init()).rejects.toThrow(
      /TransientHandlers uses @RelayBusConsumer but its provider is request-scoped or transient/,
    );
  });

  it('refuses a value that is not a catalog when the class is declared', () => {
    expect(() => RelayBusConsumer({ OrderPlaced })).toThrow(/created with defineCatalog/);
  });
});

describe('catalogs module option', () => {
  it('fails the boot listing every event nobody handles', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Partial {
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot({ ...manualOptions(), catalogs: [OrderEvents, PaymentEvents] })],
      providers: [Partial],
    });

    await expect(moduleRef.init()).rejects.toThrow(
      [
        'Catalog events without a handler or route:',
        '  - OrderEvents.OrderShipped (order.shipped.v1): expected handler method onOrderShipped',
        '  - PaymentTaken (payment.taken.v1): expected handler method onPaymentTaken',
      ].join('\n'),
    );
  });

  it('boots when consumers, @RelayBusHandler methods and routes cover the catalog', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class OrderHandlers {
      onOrderPlaced(): void {}
    }
    @Injectable()
    class OtherHandlers {
      @RelayBusHandler(OrderShipped)
      shipped(): void {}

      @RelayBusHandler(PaymentTaken)
      paid(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot({ ...manualOptions(), catalogs: [OrderEvents, PaymentEvents] })],
      providers: [OrderHandlers, OtherHandlers],
    });

    await expect(start(moduleRef)).resolves.toBeInstanceOf(RelayBus);
    await moduleRef.close();
  });

  it('is not checked when no catalogs are configured', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents)
    class Partial {
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot({ store: new InMemoryEventStore(), dispatcher: { enabled: false } })],
      providers: [Partial],
    });

    await expect(start(moduleRef)).resolves.toBeInstanceOf(RelayBus);
    await moduleRef.close();
  });
});

describe('@RelayBusConsumer with selected events', () => {
  it('registers exactly the selected methods and ignores the others', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents, { events: ['OrderShipped'], concurrency: 3 })
    class ShippingOnly {
      readonly seen: string[] = [];
      onOrderPlaced(): void {
        this.seen.push('placed');
      }
      onOrderShipped(event: EventOf<typeof OrderShipped>): void {
        this.seen.push(`shipped:${event.payload.orderId}`);
      }
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [ShippingOnly],
    });
    const bus = await start(moduleRef);

    await bus.transaction((transaction) =>
      bus.publishAll([new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })], {
        transaction,
      }),
    );
    await bus.drain();

    expect(bus.handlers().map((handler) => [handler.id, handler.concurrency])).toEqual([
      ['ShippingOnly.onOrderShipped', 3],
    ]);
    expect(moduleRef.get(ShippingOnly).seen).toEqual(['shipped:1']);
    await moduleRef.close();
  });

  it('fails the boot listing the methods a selected event lacks', async () => {
    @Injectable()
    @(RelayBusConsumer(OrderEvents, { events: ['OrderPlaced', 'OrderShipped'] }) as ClassDecorator)
    class Incomplete {
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [Incomplete],
    });

    await expect(moduleRef.init()).rejects.toThrow(
      new ConfigurationError(
        'Incomplete uses @RelayBusConsumer(OrderEvents) but lacks the handler methods of events ' +
          'it consumes: onOrderShipped',
      ),
    );
  });

  it('rejects an invalid selection when the class is declared', () => {
    expect(() => RelayBusConsumer(OrderEvents, { events: ['OrderRefunded' as never] })).toThrow(
      "Catalog 'OrderEvents' has no event named 'OrderRefunded'",
    );
    expect(() => RelayBusConsumer(OrderEvents, { events: [] })).toThrow(
      /must name at least one event/,
    );
  });
});

describe('@RelayBusConsumer inheritance', () => {
  it('registers the catalogs of the base class and of the subclass', async () => {
    @RelayBusConsumer(OrderEvents)
    class OrderBase {
      onOrderPlaced(): void {}
    }
    @Injectable()
    @RelayBusConsumer(PaymentEvents)
    class Derived extends OrderBase {
      onPaymentTaken(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Derived] });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => handler.id)).toEqual([
      'Derived.onOrderPlaced',
      'Derived.onPaymentTaken',
    ]);
    await moduleRef.close();
  });

  it('lets a subclass replace the inherited declaration of the same catalog', async () => {
    @RelayBusConsumer(OrderEvents, { transactional: true })
    class OrderBase {
      onOrderPlaced(): void {}
      onOrderShipped(): void {}
    }
    @Injectable()
    @RelayBusConsumer(OrderEvents, { events: ['OrderShipped'] })
    class ShippingOnly extends OrderBase {}
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [ShippingOnly],
    });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => [handler.id, handler.transactional])).toEqual([
      ['ShippingOnly.onOrderShipped', false],
    ]);
    await moduleRef.close();
  });

  it('rejects the same catalog declared twice on one class', () => {
    expect(() => {
      @RelayBusConsumer(OrderEvents)
      @RelayBusConsumer(OrderEvents, { events: ['OrderPlaced'] })
      class Twice {
        onOrderPlaced(): void {}
      }
      return Twice;
    }).toThrow(
      new ConfigurationError(
        '@RelayBusConsumer(OrderEvents) is declared twice on Twice; merge the declarations into one',
      ),
    );
    expect(() => {
      @RelayBusConsumer(PaymentEvents)
      @RelayBusConsumer(PaymentEvents)
      class Unnamed {
        onPaymentTaken(): void {}
      }
      return Unnamed;
    }).toThrow('@RelayBusConsumer(catalog) is declared twice on Unnamed');
  });
});

describe('catalogs module option with consume and produce', () => {
  it('boots a consumer that handles its part of a shared catalog', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents, { events: ['OrderShipped'] })
    class ShippingHandlers {
      onOrderShipped(): void {}
    }
    const moduleRef = await createApplication({
      imports: [
        relayBusRoot({
          ...manualOptions(),
          catalogs: [consume(OrderEvents, { events: ['OrderShipped'] })],
        }),
      ],
      providers: [ShippingHandlers],
    });

    await expect(start(moduleRef)).resolves.toBeInstanceOf(RelayBus);
    await moduleRef.close();
  });

  it('fails the boot when a consumed event has no handler or a produced event no route', async () => {
    @Injectable()
    @RelayBusConsumer(OrderEvents, { events: ['OrderPlaced'] })
    class PlacedHandlers {
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({
      imports: [
        relayBusRoot({
          ...manualOptions(),
          catalogs: [
            consume(OrderEvents),
            produce(PaymentEvents),
            produce(OrderEvents, { events: ['OrderPlaced'] }),
          ],
        }),
      ],
      providers: [PlacedHandlers],
    });

    await expect(moduleRef.init()).rejects.toThrow(
      new ConfigurationError(
        [
          'Catalog events this service does not cover:',
          '  - OrderEvents.OrderShipped (order.shipped.v1): consumed here, needs handler method onOrderShipped',
          '  - PaymentTaken (payment.taken.v1): produced here, needs a route to a transport',
          '  - OrderEvents.OrderPlaced (order.placed.v1): produced here, needs a route to a transport; ' +
            'a local handler does not deliver it to other services',
        ].join('\n'),
      ),
    );
  });
});
