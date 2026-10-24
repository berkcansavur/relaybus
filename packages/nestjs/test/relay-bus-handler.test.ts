import { Inject, Injectable, Module, Scope } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';
import type { TestingModule } from '@nestjs/testing';
import {
  InMemoryEventStore,
  RelayBus,
  type EventEnvelope,
  type HandlerContext,
  type InMemoryTransaction,
} from '@relaybus/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RelayBusHandler } from '../src/index.js';
import { OrderPlaced, createApplication, relayBusRoot } from './helpers.js';

afterEach(() => {
  vi.restoreAllMocks();
});

@Injectable()
class OrderService {
  constructor(@Inject(RelayBus) private readonly bus: RelayBus<InMemoryTransaction>) {}

  place(orderId: string): Promise<void> {
    return this.bus.transaction(async (transaction) => {
      await this.bus.publish(new OrderPlaced({ orderId }), { transaction });
    });
  }
}

async function start(moduleRef: TestingModule): Promise<RelayBus<InMemoryTransaction>> {
  await moduleRef.init();
  return moduleRef.get(RelayBus);
}

describe('@RelayBusHandler discovery', () => {
  it('invokes handlers from several modules with this bound to the provider', async () => {
    @Injectable()
    class Mailer {
      readonly sent: string[] = [];
    }
    @Injectable()
    class ShippingHandlers {
      readonly seen: string[] = [];
      constructor(@Inject(Mailer) private readonly mailer: Mailer) {}

      @RelayBusHandler(OrderPlaced)
      onOrderPlaced(event: EventEnvelope<{ orderId: string }>, context: HandlerContext): void {
        this.seen.push(event.payload.orderId);
        this.mailer.sent.push(context.handler);
      }
    }
    @Injectable()
    class AuditHandlers {
      readonly seen: string[] = [];

      @RelayBusHandler(OrderPlaced, { id: 'audit.order-placed' })
      async audit(event: EventEnvelope<{ orderId: string }>): Promise<void> {
        this.seen.push(event.id);
      }
    }
    @Module({ providers: [Mailer, ShippingHandlers], exports: [Mailer] })
    class ShippingModule {}
    @Module({ providers: [AuditHandlers] })
    class AuditModule {}
    @Module({ providers: [OrderService] })
    class OrdersModule {}

    const moduleRef = await createApplication({
      imports: [relayBusRoot(), ShippingModule, AuditModule, OrdersModule],
    });
    const bus = await start(moduleRef);

    await moduleRef.get(OrderService).place('o-1');
    await bus.drain();

    const shipping = moduleRef.get(ShippingHandlers);
    expect(shipping.seen).toEqual(['o-1']);
    expect(moduleRef.get(Mailer).sent).toEqual(['ShippingHandlers.onOrderPlaced']);
    expect(moduleRef.get(AuditHandlers).seen).toHaveLength(1);
    expect(
      bus
        .handlers()
        .map((handler) => handler.id)
        .sort(),
    ).toEqual(['ShippingHandlers.onOrderPlaced', 'audit.order-placed']);
    await moduleRef.close();
  });

  it('subscribes by event type string and passes options through', async () => {
    @Injectable()
    class Handlers {
      @RelayBusHandler('order.placed.v1', { concurrency: 2, transactional: true })
      handle(_event: EventEnvelope<unknown>, context: HandlerContext<InMemoryTransaction>): void {
        expect(context.transaction).toBeDefined();
      }
    }
    @Module({ providers: [Handlers, OrderService] })
    class FeatureModule {}

    const moduleRef = await createApplication({ imports: [relayBusRoot(), FeatureModule] });
    const bus = await start(moduleRef);

    await moduleRef.get(OrderService).place('o-2');
    const processed = await bus.drain();

    const [registration] = bus.handlers();
    expect(processed).toBe(1);
    expect(registration).toMatchObject({
      id: 'Handlers.handle',
      eventType: 'order.placed.v1',
      concurrency: 2,
      transactional: true,
    });
    await moduleRef.close();
  });

  it('supports several events on one method with explicit ids', async () => {
    @Injectable()
    class Handlers {
      calls = 0;

      @RelayBusHandler(OrderPlaced, { id: 'multi.a' })
      @RelayBusHandler('order.shipped.v1', { id: 'multi.b' })
      handle(): void {
        this.calls += 1;
      }
    }

    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [Handlers],
    });
    const bus = await start(moduleRef);

    expect(bus.handlers().map((handler) => handler.id)).toEqual(['multi.b', 'multi.a']);
    await moduleRef.close();
  });

  it('skips non-object providers and providers without a prototype', async () => {
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [
        { provide: 'TEXT', useValue: 'text' },
        { provide: 'NOTHING', useValue: null },
        { provide: 'BARE', useValue: Object.create(null) as object },
        { provide: 'PLAIN', useValue: { handle: () => undefined } },
      ],
    });

    const bus = await start(moduleRef);

    expect(bus.handlers()).toEqual([]);
    await moduleRef.close();
  });

  it('runs handlers through a running dispatcher', async () => {
    const seen: string[] = [];
    @Injectable()
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      handle(event: EventEnvelope<{ orderId: string }>): void {
        seen.push(event.payload.orderId);
      }
    }

    const moduleRef = await createApplication({
      imports: [
        relayBusRoot({ store: new InMemoryEventStore(), dispatcher: { pollInterval: '10ms' } }),
      ],
      providers: [Handlers, OrderService],
    });
    await start(moduleRef);

    await moduleRef.get(OrderService).place('o-3');

    await vi.waitFor(() => expect(seen).toEqual(['o-3']), { timeout: 5_000 });
    await moduleRef.close();
  });
});

describe('@RelayBusHandler validation', () => {
  it('fails init on duplicate default ids with both locations named', async () => {
    @Injectable()
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      @RelayBusHandler('order.shipped.v1')
      handle(): void {}
    }

    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });

    await expect(moduleRef.init()).rejects.toThrow(
      "Handler id 'Handlers.handle' is used by both Handlers.handle and Handlers.handle",
    );
  });

  it('fails init when two providers share an explicit id', async () => {
    @Injectable()
    class First {
      @RelayBusHandler(OrderPlaced, { id: 'shared' })
      handle(): void {}
    }
    @Injectable()
    class Second {
      @RelayBusHandler(OrderPlaced, { id: 'shared' })
      handle(): void {}
    }

    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [First, Second],
    });

    await expect(moduleRef.init()).rejects.toThrow(/Handler id 'shared' is used by both/);
  });

  it('fails init on an invalid handler id from the core registry', async () => {
    @Injectable()
    class Handlers {
      @RelayBusHandler(OrderPlaced, { id: 'not valid' })
      handle(): void {}
    }

    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });

    await expect(moduleRef.init()).rejects.toThrow(/Handler id 'not valid'/);
  });

  it('rejects request-scoped providers', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      handle(): void {}
    }

    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });

    await expect(moduleRef.init()).rejects.toThrow(
      /Handlers\.handle uses @RelayBusHandler but its provider is request-scoped or transient/,
    );
  });

  it('rejects transient providers', async () => {
    @Injectable({ scope: Scope.TRANSIENT })
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      handle(): void {}
    }

    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Handlers] });

    await expect(moduleRef.init()).rejects.toThrow(/request-scoped or transient/);
  });

  it('rejects default-scoped providers that depend on a request-scoped provider', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class RequestContext {}
    @Injectable()
    class Handlers {
      constructor(@Inject(RequestContext) readonly context: RequestContext) {}

      @RelayBusHandler(OrderPlaced)
      handle(): void {}
    }

    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [RequestContext, Handlers],
    });

    await expect(moduleRef.init()).rejects.toThrow(/request-scoped or transient/);
  });

  it('ignores request-scoped providers that declare no handlers', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class Plain {
      handle(): void {}
    }

    const moduleRef = await createApplication({ imports: [relayBusRoot()], providers: [Plain] });

    await expect(start(moduleRef)).resolves.toBeInstanceOf(RelayBus);
    await moduleRef.close();
  });

  it('keeps the subscription when a later decorator replaces the method', async () => {
    function wrapMethod(): MethodDecorator {
      return (_target, _key, descriptor) => {
        const original = descriptor.value as (...args: unknown[]) => unknown;
        descriptor.value = function (this: unknown, ...args: unknown[]) {
          return original.apply(this, args);
        } as never;
      };
    }
    @Injectable()
    class WrappedAbove {
      readonly seen: string[] = [];

      @wrapMethod()
      @RelayBusHandler(OrderPlaced)
      handle(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(event.payload.orderId);
      }
    }
    @Injectable()
    class WrappedBelow {
      readonly seen: string[] = [];

      @RelayBusHandler(OrderPlaced)
      @wrapMethod()
      handle(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(event.payload.orderId);
      }
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [OrderService, WrappedAbove, WrappedBelow],
    });
    const bus = await start(moduleRef);

    await moduleRef.get(OrderService).place('o-1');
    await bus.drain();

    expect(moduleRef.get(WrappedAbove).seen).toEqual(['o-1']);
    expect(moduleRef.get(WrappedBelow).seen).toEqual(['o-1']);
    await moduleRef.close();
  });

  it('discovers handlers declared on a base class', async () => {
    class BaseHandlers {
      readonly seen: string[] = [];

      @RelayBusHandler(OrderPlaced, { id: 'base.handle' })
      handle(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(event.payload.orderId);
      }
    }
    @Injectable()
    class DerivedHandlers extends BaseHandlers {}
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [OrderService, DerivedHandlers],
    });
    const bus = await start(moduleRef);

    await moduleRef.get(OrderService).place('o-1');
    await bus.drain();

    expect(moduleRef.get(DerivedHandlers).seen).toEqual(['o-1']);
    await moduleRef.close();
  });

  it('registers a handler once when a useExisting alias points at its provider', async () => {
    @Injectable()
    class Handlers {
      readonly seen: string[] = [];

      @RelayBusHandler(OrderPlaced)
      handle(event: EventEnvelope<{ orderId: string }>): void {
        this.seen.push(event.payload.orderId);
      }
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [OrderService, Handlers, { provide: 'HANDLERS_ALIAS', useExisting: Handlers }],
    });
    const bus = await start(moduleRef);

    await moduleRef.get(OrderService).place('o-1');
    await bus.drain();

    expect(bus.handlers()).toHaveLength(1);
    expect(moduleRef.get(Handlers).seen).toEqual(['o-1']);
    await moduleRef.close();
  });

  it('rejects factory providers that inject REQUEST', async () => {
    @Injectable()
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      handle(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [{ provide: Handlers, useFactory: () => new Handlers(), inject: [REQUEST] }],
    });

    await expect(moduleRef.init()).rejects.toThrow(
      /Handlers\.handle uses @RelayBusHandler but its provider is request-scoped or transient/,
    );
  });

  it('rejects factory providers that depend on a request-scoped provider', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class RequestContext {}
    @Injectable()
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      handle(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [
        RequestContext,
        { provide: Handlers, useFactory: () => new Handlers(), inject: [RequestContext] },
      ],
    });

    await expect(moduleRef.init()).rejects.toThrow(/request-scoped or transient/);
  });

  it('rejects request-scoped class providers registered under a different token', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class Handlers {
      @RelayBusHandler(OrderPlaced)
      handle(): void {}
    }
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [{ provide: 'HANDLERS', useClass: Handlers }],
    });

    await expect(moduleRef.init()).rejects.toThrow(/Handlers\.handle uses @RelayBusHandler/);
  });

  it('cannot inspect request-scoped factories with a string token and leaves them alone', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class RequestContext {}
    const moduleRef = await createApplication({
      imports: [relayBusRoot()],
      providers: [
        RequestContext,
        { provide: 'PLAIN', useFactory: (context: unknown) => context, inject: [RequestContext] },
      ],
    });

    await expect(start(moduleRef)).resolves.toBeInstanceOf(RelayBus);
    await moduleRef.close();
  });
});
