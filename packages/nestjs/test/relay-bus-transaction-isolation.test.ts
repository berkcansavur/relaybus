import { Inject, Injectable, Module } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import {
  ConfigurationError,
  InMemoryEventStore,
  RelayBus,
  TRANSACTION_ISOLATION_LEVELS,
  TransactionIsolationConflictError,
  type EventStoreTransactionOptions,
  type InMemoryTransaction,
  type TransactionIsolationLevel,
} from '@relaybus/core';
import 'reflect-metadata';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RelayBusTransaction } from '../src/index.js';
import { OrderPlaced, createApplication, manualOptions, relayBusRoot } from './helpers.js';

class SerializationFailure extends Error {
  readonly code = '40001';
}

/** An in-memory store that behaves like a database with isolation levels and serialization failures. */
class IsolatingStore extends InMemoryEventStore {
  readonly isolationLevels: readonly TransactionIsolationLevel[] = TRANSACTION_ISOLATION_LEVELS;
  readonly seen: Array<EventStoreTransactionOptions | undefined> = [];

  override transaction<T>(
    work: (transaction: InMemoryTransaction) => Promise<T>,
    options?: EventStoreTransactionOptions,
  ): Promise<T> {
    this.seen.push(options);
    return super.transaction(work);
  }

  isSerializationFailure(error: unknown): boolean {
    return error instanceof SerializationFailure;
  }
}

@Injectable()
class ReservationService {
  attempts = 0;
  constructor(@Inject(RelayBus) readonly bus: RelayBus<InMemoryTransaction>) {}

  @RelayBusTransaction({ isolationLevel: 'serializable' })
  async reserve(orderId: string, conflicts = 0): Promise<number> {
    this.attempts += 1;
    await this.bus.publish(new OrderPlaced({ orderId: `${orderId}-${this.attempts}` }));
    if (this.attempts <= conflicts) {
      throw new SerializationFailure('could not serialize access');
    }
    return this.attempts;
  }

  @RelayBusTransaction({ isolationLevel: 'serializable', maxAttempts: 2 })
  async reserveTwice(): Promise<void> {
    this.attempts += 1;
    throw new SerializationFailure('could not serialize access');
  }

  @RelayBusTransaction({ isolationLevel: 'repeatable read' })
  async reserveSnapshot(): Promise<void> {}

  @RelayBusTransaction()
  async plain(): Promise<TransactionIsolationLevel | undefined> {
    return this.bus.currentIsolationLevel();
  }

  @RelayBusTransaction({ isolationLevel: 'serializable' })
  async reserveAndNest(): Promise<Array<TransactionIsolationLevel | undefined>> {
    return [this.bus.currentIsolationLevel(), await this.plain(), await this.reserveInner()];
  }

  @RelayBusTransaction({ isolationLevel: 'serializable' })
  async reserveInner(): Promise<TransactionIsolationLevel | undefined> {
    return this.bus.currentIsolationLevel();
  }

  @RelayBusTransaction()
  async plainThenSerializable(): Promise<void> {
    await this.reserveInner();
  }

  @RelayBusTransaction({ isolationLevel: 'serializable' })
  async serializableThenSnapshot(): Promise<void> {
    await this.reserveSnapshot();
  }
}

@Module({ providers: [ReservationService] })
class ReservationModule {}

const open: TestingModule[] = [];

async function application(
  store: InMemoryEventStore = new IsolatingStore(),
  extra: { transactionResolver?: () => InMemoryTransaction | undefined } = {},
): Promise<TestingModule> {
  const moduleRef = await createApplication({
    imports: [relayBusRoot({ ...manualOptions(store), ...extra }), ReservationModule],
  });
  open.push(moduleRef);
  await moduleRef.init();
  moduleRef.get(RelayBus).subscribe(OrderPlaced, () => undefined, { id: 'h' });
  return moduleRef;
}

afterEach(async () => {
  for (const moduleRef of open.splice(0)) {
    await moduleRef.close();
  }
  vi.restoreAllMocks();
});

describe('@RelayBusTransaction({ isolationLevel })', () => {
  it('opens the transaction at the level', async () => {
    const store = new IsolatingStore();
    const service = (await application(store)).get(ReservationService);

    await service.reserve('o');

    expect(store.seen).toEqual([{ timeoutMs: 5_000, isolationLevel: 'serializable' }]);
  });

  it('reruns the method after a serialization failure and keeps only the last attempt events', async () => {
    const store = new IsolatingStore();
    const service = (await application(store)).get(ReservationService);

    await expect(service.reserve('o', 2)).resolves.toBe(3);

    expect(store.seen).toHaveLength(3);
    expect((await store.listDeliveries({})).map((delivery) => delivery.event.payload)).toEqual([
      { orderId: 'o-3' },
    ]);
  });

  it('stops after maxAttempts and rethrows the failure', async () => {
    const service = (await application()).get(ReservationService);

    await expect(service.reserveTwice()).rejects.toBeInstanceOf(SerializationFailure);

    expect(service.attempts).toBe(2);
  });

  it('is refused by a store without isolation levels', async () => {
    const service = (await application(new InMemoryEventStore())).get(ReservationService);

    await expect(service.reserve('o')).rejects.toBeInstanceOf(ConfigurationError);
  });

  it('joins a transaction opened at the same level, and a method without a level joins any', async () => {
    const service = (await application()).get(ReservationService);

    await expect(service.reserveAndNest()).resolves.toEqual([
      'serializable',
      'serializable',
      'serializable',
    ]);
  });

  it('refuses to join a transaction opened without a level', async () => {
    const service = (await application()).get(ReservationService);

    const error = await service.plainThenSerializable().catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionIsolationConflictError);
    expect(error).toMatchObject({ code: 'RELAYBUS_TRANSACTION_ISOLATION_CONFLICT' });
    expect((error as Error).message).toMatch(
      /^ReservationService\.reserveInner asks for isolationLevel 'serializable' but runs inside a transaction that relaybus did not open at an isolation level/,
    );
  });

  it('refuses to join a transaction opened at another level', async () => {
    const service = (await application()).get(ReservationService);

    const error = await service.serializableThenSnapshot().catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionIsolationConflictError);
    expect((error as Error).message).toMatch(
      /ReservationService\.reserveSnapshot asks for isolationLevel 'repeatable read' but runs inside a transaction opened at 'serializable', and the isolation level of a running transaction cannot be changed/,
    );
  });

  it('refuses to join the transaction of the transactionResolver', async () => {
    const store = new IsolatingStore();
    let resolved: InMemoryTransaction | undefined;
    const service = (await application(store, { transactionResolver: () => resolved })).get(
      ReservationService,
    );

    await store.transaction(async (transaction) => {
      resolved = transaction;
      await expect(service.reserve('o')).rejects.toBeInstanceOf(TransactionIsolationConflictError);
      await expect(service.plain()).resolves.toBeUndefined();
      resolved = undefined;
    });
  });

  it('leaves a method without options exactly as it was', async () => {
    const store = new IsolatingStore();
    const service = (await application(store)).get(ReservationService);

    await expect(service.plain()).resolves.toBeUndefined();

    expect(store.seen).toEqual([{ timeoutMs: 5_000 }]);
  });
});
