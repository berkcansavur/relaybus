import { Inject, Injectable, Module } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import {
  ConfigurationError,
  InMemoryEventStore,
  RelayBus,
  TransactionRequiredError,
  type InMemoryTransaction,
} from '@relaybus/core';
import 'reflect-metadata';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RelayBusTransaction, RelayBusTransactionHost } from '../src/index.js';
import { OrderPlaced, createApplication, manualOptions, relayBusRoot } from './helpers.js';

const ROUTE_METADATA = 'test:route';

/** Stands in for a framework decorator that stores metadata on the method function, like `@Get()`. */
function Route(path: string): MethodDecorator {
  return (_target, _propertyKey, descriptor) => {
    Reflect.defineMetadata(ROUTE_METADATA, path, descriptor.value as object);
  };
}

@Injectable()
class OrderRepository {
  readonly writes: InMemoryTransaction[] = [];
  constructor(
    @Inject(RelayBusTransactionHost)
    private readonly transactions: RelayBusTransactionHost<InMemoryTransaction>,
  ) {}

  async insert(): Promise<void> {
    this.writes.push(this.transactions.current());
  }
}

@Injectable()
class OrderService {
  constructor(
    @Inject(RelayBus) private readonly bus: RelayBus<InMemoryTransaction>,
    @Inject(OrderRepository) readonly repository: OrderRepository,
  ) {}

  @RelayBusTransaction()
  async place(orderId: string): Promise<string> {
    await this.repository.insert();
    await this.bus.publish(new OrderPlaced({ orderId }));
    return orderId;
  }

  @RelayBusTransaction()
  async placeTwice(orderId: string): Promise<void> {
    await this.place(`${orderId}-a`);
    await this.place(`${orderId}-b`);
  }

  @RelayBusTransaction()
  async placeAndFail(orderId: string): Promise<void> {
    await this.place(orderId);
    throw new Error('payment declined');
  }

  // Applied first, so the route metadata sits on the original method and has to be carried over.
  @RelayBusTransaction()
  @Route('/orders')
  async routed(): Promise<void> {}
}

@Module({ providers: [OrderRepository, OrderService] })
class OrdersModule {}

async function startApplication(store = new InMemoryEventStore()): Promise<TestingModule> {
  const moduleRef = await createApplication({
    imports: [relayBusRoot(manualOptions(store)), OrdersModule],
  });
  await moduleRef.init();
  moduleRef.get(RelayBus).subscribe(OrderPlaced, () => undefined, { id: 'h' });
  return moduleRef;
}

const open: TestingModule[] = [];

async function application(store?: InMemoryEventStore): Promise<TestingModule> {
  const moduleRef = await startApplication(store);
  open.push(moduleRef);
  return moduleRef;
}

afterEach(async () => {
  for (const moduleRef of open.splice(0)) {
    await moduleRef.close();
  }
  vi.restoreAllMocks();
});

describe('@RelayBusTransaction', () => {
  it('publishes without passing the transaction and commits when the method resolves', async () => {
    const store = new InMemoryEventStore();
    const moduleRef = await application(store);
    const service = moduleRef.get(OrderService);

    await expect(service.place('o-1')).resolves.toBe('o-1');

    const deliveries = await store.listDeliveries({});
    expect(deliveries.map((delivery) => delivery.event.payload)).toEqual([{ orderId: 'o-1' }]);
    expect(service.repository.writes[0]?.state).toBe('committed');
  });

  it('shares one transaction across this.method() calls and rolls it back on a throw', async () => {
    const store = new InMemoryEventStore();
    const transaction = vi.spyOn(store, 'transaction');
    const moduleRef = await application(store);
    const service = moduleRef.get(OrderService);

    await service.placeTwice('o');
    expect(transaction).toHaveBeenCalledTimes(1);
    const [first, second] = service.repository.writes;
    expect(second).toBe(first);

    await expect(service.placeAndFail('o-3')).rejects.toThrow('payment declined');
    expect(transaction).toHaveBeenCalledTimes(2);
    expect(service.repository.writes[2]?.state).toBe('rolled-back');
    expect(await store.listDeliveries({})).toHaveLength(2);
  });

  it('joins bus.transaction and the transaction the resolver finds', async () => {
    const store = new InMemoryEventStore();
    let resolved: InMemoryTransaction | undefined;
    const moduleRef = await createApplication({
      imports: [
        relayBusRoot({ ...manualOptions(store), transactionResolver: () => resolved }),
        OrdersModule,
      ],
    });
    open.push(moduleRef);
    await moduleRef.init();
    const bus = moduleRef.get<RelayBus<InMemoryTransaction>>(RelayBus);
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    const service = moduleRef.get(OrderService);

    const outer = await bus.transaction(async (transaction) => {
      await service.place('o-1');
      return transaction;
    });
    await store.transaction(async (transaction) => {
      resolved = transaction;
      await service.place('o-2');
      resolved = undefined;
    });

    expect(service.repository.writes[0]).toBe(outer);
    expect(service.repository.writes[1]).not.toBe(outer);
    expect(service.repository.writes[1]?.state).toBe('committed');
    expect(await store.listDeliveries({})).toHaveLength(2);
  });

  it('keeps the name and the metadata of the decorated method', async () => {
    const moduleRef = await application();
    const service = moduleRef.get(OrderService);
    expect(service.routed.name).toBe('routed');
    expect(Reflect.getMetadata(ROUTE_METADATA, service.routed)).toBe('/orders');
    await expect(service.routed()).resolves.toBeUndefined();
  });

  it('uses the bus of the provider application when several applications run', async () => {
    const firstStore = new InMemoryEventStore();
    const secondStore = new InMemoryEventStore();
    const first = await application(firstStore);
    const second = await application(secondStore);

    await second.get(OrderService).place('o-2');
    await first.get(OrderService).place('o-1');

    expect((await firstStore.listDeliveries({})).map((d) => d.event.payload)).toEqual([
      { orderId: 'o-1' },
    ]);
    expect((await secondStore.listDeliveries({})).map((d) => d.event.payload)).toEqual([
      { orderId: 'o-2' },
    ]);
    const detached = new OrderService(first.get(RelayBus), first.get(OrderRepository));
    await expect(detached.place('o-3')).rejects.toThrow(
      /cannot tell which of 2 running applications it belongs to/,
    );
  });

  it('runs a method of an object created outside the container on the only running bus', async () => {
    const store = new InMemoryEventStore();
    const moduleRef = await application(store);
    const detached = new OrderService(moduleRef.get(RelayBus), moduleRef.get(OrderRepository));

    await detached.place('o-1');

    expect(await store.listDeliveries({})).toHaveLength(1);
  });

  it('fails clearly without a running module and after the application shut down', async () => {
    class Plain {
      @RelayBusTransaction()
      async run(): Promise<void> {}
    }
    await expect(new Plain().run()).rejects.toThrow(ConfigurationError);
    await expect(new Plain().run()).rejects.toThrow(
      /Plain\.run found no running RelayBusModule: import RelayBusModule\.forRoot/,
    );

    await expect(Plain.prototype.run.call(undefined)).rejects.toThrow(/found no running/);

    const moduleRef = await startApplication();
    const service = moduleRef.get(OrderService);
    await moduleRef.close();
    await expect(service.place('o-1')).rejects.toThrow(
      /OrderService\.place was called after its application shut down/,
    );
  });
});

describe('RelayBusTransactionHost', () => {
  it('requires an active transaction for current() and reports none through find()', async () => {
    const moduleRef = await application();
    const host =
      moduleRef.get<RelayBusTransactionHost<InMemoryTransaction>>(RelayBusTransactionHost);
    const bus = moduleRef.get<RelayBus<InMemoryTransaction>>(RelayBus);

    expect(host.find()).toBeUndefined();
    expect(() => host.current()).toThrow(TransactionRequiredError);
    expect(() => host.current()).toThrow(/from a @RelayBusTransaction\(\) method/);
    await bus.transaction(async (transaction) => {
      expect(host.current()).toBe(transaction);
      expect(host.find()).toBe(transaction);
    });
  });
});
