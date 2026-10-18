import { Inject, Injectable, Scope } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import {
  ConfigurationError,
  InMemoryEventStore,
  RelayBus,
  defineCatalog,
  defineEvent,
  type EventEnvelope,
  type EventOf,
  type HandlerContext,
  type HandlersOf,
  type InMemoryTransaction,
  type PartialHandlersOf,
} from '@relaybus/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventConsumer, Subscribe } from '../src/index.js';
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

describe('@EventConsumer discovery', () => {
  it('registers every on<Key> method under ClassName.method with this bound', async () => {
    @Injectable()
    class Mailer {
      readonly sent: string[] = [];
    }
    @Injectable()
    @EventConsumer(OrderEvents)
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
    @EventConsumer(OrderEvents, { transactional: true, concurrency: 2 })
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
    @EventConsumer(OrderEvents)
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

  it('works next to @Subscribe methods of the same class', async () => {
    @Injectable()
    @EventConsumer(OrderEvents)
    class Handlers {
      readonly seen: string[] = [];

      onOrderPlaced(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(`consumer:${event.payload.orderId}`);
      }

      @Subscribe(OrderPlaced)
      audit(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(`subscribe:${event.payload.orderId}`);
      }

      @Subscribe(OrderPlaced, { id: 'explicit.placed' })
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

  it('rejects a @Subscribe that claims the id of a consumer method', async () => {
    @Injectable()
    @EventConsumer(OrderEvents)
    class Handlers {
      @Subscribe(OrderPlaced)
      onOrderPlaced(): void {}
    }
    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });

    await expect(moduleRef.init()).rejects.toThrow(
      new ConfigurationError(
        "Handler id 'Handlers.onOrderPlaced' of @EventConsumer(OrderEvents) is already used by " +
          'Handlers.onOrderPlaced; give the @Subscribe a unique { id } or drop it',
      ),
    );
  });

  it('accepts the same method with a @Subscribe that has its own id', async () => {
    @Injectable()
    @EventConsumer(OrderEvents)
    class Handlers {
      @Subscribe(OrderPlaced, { id: 'audit.placed' })
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
    @EventConsumer(OrderEvents)
    class Misspelled {
      onOrderShiped(): void {}
    }
    @Injectable()
    @EventConsumer(PaymentEvents)
    class Unnamed {}
    const named = await createApplication({ imports: [relayBusRoot()], providers: [Misspelled] });
    const unnamed = await createApplication({ imports: [relayBusRoot()], providers: [Unnamed] });

    await expect(named.init()).rejects.toThrow(
      'Misspelled uses @EventConsumer(OrderEvents) but defines none of its handler methods ' +
        '(onOrderPlaced, onOrderShipped)',
    );
    await expect(unnamed.init()).rejects.toThrow(
      'Unnamed uses @EventConsumer(catalog) but defines none of its handler methods (onPaymentTaken)',
    );
  });

  it('supports several catalogs on one class and rejects colliding method ids', async () => {
    @Injectable()
    @EventConsumer(OrderEvents)
    @EventConsumer(PaymentEvents)
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
    @EventConsumer(OrderEvents)
    @EventConsumer(SameKeys)
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
    @EventConsumer(OrderEvents)
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
    @EventConsumer(OrderEvents)
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
    @EventConsumer(OrderEvents)
    class RequestHandlers {
      onOrderPlaced(): void {}
    }
    @Injectable({ scope: Scope.TRANSIENT })
    @EventConsumer(OrderEvents)
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
      'RequestHandlers uses @EventConsumer but its provider is request-scoped or transient',
    );
    await expect(transient.init()).rejects.toThrow(
      /TransientHandlers uses @EventConsumer but its provider is request-scoped or transient/,
    );
  });

  it('refuses a value that is not a catalog when the class is declared', () => {
    expect(() => EventConsumer({ OrderPlaced })).toThrow(/created with defineCatalog/);
  });
});

describe('catalogs module option', () => {
  it('fails the boot listing every event nobody handles', async () => {
    @Injectable()
    @EventConsumer(OrderEvents)
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

  it('boots when consumers, @Subscribe methods and routes cover the catalog', async () => {
    @Injectable()
    @EventConsumer(OrderEvents)
    class OrderHandlers {
      onOrderPlaced(): void {}
    }
    @Injectable()
    class OtherHandlers {
      @Subscribe(OrderShipped)
      shipped(): void {}

      @Subscribe(PaymentTaken)
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
    @EventConsumer(OrderEvents)
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
