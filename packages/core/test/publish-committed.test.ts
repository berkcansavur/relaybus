import { describe, expect, it, vi } from 'vitest';
import {
  InMemoryEventStore,
  TRANSACTION_ISOLATION_LEVELS,
  type EventStoreTransactionOptions,
  type InMemoryTransaction,
  type TransactionIsolationLevel,
} from '@relaybus/core';
import { OrderPlaced, OrderShipped, createTestBus } from './helpers.js';

class SerializationFailure extends Error {
  readonly code = '40001';
}

class IsolatingStore extends InMemoryEventStore {
  readonly isolationLevels: readonly TransactionIsolationLevel[] = TRANSACTION_ISOLATION_LEVELS;

  override transaction<T>(
    work: (transaction: InMemoryTransaction) => Promise<T>,
    _options?: EventStoreTransactionOptions,
  ): Promise<T> {
    return super.transaction(work);
  }

  isSerializationFailure(error: unknown): boolean {
    return error instanceof SerializationFailure;
  }
}

class RerunningStore extends InMemoryEventStore {
  override async transaction<T>(
    work: (transaction: InMemoryTransaction) => Promise<T>,
    _options?: EventStoreTransactionOptions,
  ): Promise<T> {
    // Like MongoDB's withTransaction after a transient error: the first run is discarded.
    await super
      .transaction(async (transaction) => {
        await work(transaction);
        throw new Error('transient');
      })
      .catch(() => undefined);
    return super.transaction(work);
  }
}

function busWithHandler(options: Parameters<typeof createTestBus>[0] = {}) {
  const test = createTestBus(options);
  test.bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
  return test;
}

describe('onPublishCommitted', () => {
  it('reports the events of a bus.transaction once it committed, not before', async () => {
    const { bus, observer } = busWithHandler();

    await bus.transaction(async () => {
      await bus.publish(new OrderPlaced({ orderId: 'a' }));
      await bus.publish(new OrderPlaced({ orderId: 'b' }));
      expect(observer.onPublished).toHaveBeenCalledTimes(2);
      expect(observer.onPublishCommitted).not.toHaveBeenCalled();
    });

    expect(observer.onPublishCommitted).toHaveBeenCalledTimes(2);
    expect(vi.mocked(observer.onPublishCommitted).mock.calls.map(([event]) => event)).toEqual([
      { envelopes: [expect.objectContaining({ payload: { orderId: 'a' } })], deliveries: 1 },
      { envelopes: [expect.objectContaining({ payload: { orderId: 'b' } })], deliveries: 1 },
    ]);
  });

  it('reports nothing for a bus.transaction that rolled back', async () => {
    const { bus, observer } = busWithHandler();

    await expect(
      bus.transaction(async () => {
        await bus.publish(new OrderPlaced({ orderId: 'a' }));
        throw new Error('limit reached');
      }),
    ).rejects.toThrow('limit reached');

    expect(observer.onPublished).toHaveBeenCalledTimes(1);
    expect(observer.onPublishCommitted).not.toHaveBeenCalled();
  });

  it('reports only the attempt of an isolated transaction that committed', async () => {
    const test = createTestBus({ store: new IsolatingStore() });
    test.bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    let attempts = 0;

    await test.bus.transaction(
      async () => {
        attempts += 1;
        await test.bus.publish(new OrderPlaced({ orderId: `o-${attempts}` }));
        if (attempts === 1) {
          throw new SerializationFailure('could not serialize access');
        }
      },
      { isolationLevel: 'serializable' },
    );

    expect(test.observer.onPublishCommitted).toHaveBeenCalledTimes(1);
    expect(test.observer.onPublishCommitted).toHaveBeenCalledWith({
      envelopes: [expect.objectContaining({ payload: { orderId: 'o-2' } })],
      deliveries: 1,
    });
  });

  it('reports a publish the bus commits in a transaction of its own at once', async () => {
    const { bus, observer } = busWithHandler({ transactionMode: 'optional' });

    await bus.publish(new OrderPlaced({ orderId: 'a' }));

    expect(observer.onPublishCommitted).toHaveBeenCalledWith({
      envelopes: [expect.objectContaining({ payload: { orderId: 'a' } })],
      deliveries: 1,
    });
  });

  it('reports nothing for a publish into a transaction the caller opened', async () => {
    const { bus, store, observer } = busWithHandler();

    await store.transaction(async (transaction) => {
      await bus.publish(new OrderPlaced({ orderId: 'a' }), { transaction });
    });

    expect(observer.onPublished).toHaveBeenCalledTimes(1);
    expect(observer.onPublishCommitted).not.toHaveBeenCalled();
  });

  it('reports each nested bus.transaction when that one commits', async () => {
    const { bus, observer } = busWithHandler();
    const reported: string[] = [];
    vi.mocked(observer.onPublishCommitted).mockImplementation(({ envelopes }) => {
      reported.push(...envelopes.map((envelope) => envelope.type));
    });
    bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });

    await bus.transaction(async () => {
      await bus.transaction(async () => {
        await bus.publish(new OrderShipped({ orderId: 'inner' }));
      });
      expect(reported).toEqual(['order.shipped.v1']);
      await bus.publish(new OrderPlaced({ orderId: 'outer' }));
      expect(reported).toEqual(['order.shipped.v1']);
    });

    expect(reported).toEqual(['order.shipped.v1', 'order.placed.v1']);
  });

  it('reports the events a transactional handler published once its delivery committed', async () => {
    const test = createTestBus({ transactionMode: 'optional' });
    const committedDuringHandler: number[] = [];
    test.bus.subscribe(
      OrderPlaced,
      async (event) => {
        await test.bus.publish(new OrderShipped({ orderId: event.payload.orderId }));
        committedDuringHandler.push(vi.mocked(test.observer.onPublishCommitted).mock.calls.length);
      },
      { id: 'ship', transactional: true },
    );
    test.bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    await test.bus.publish(new OrderPlaced({ orderId: 'a' }));
    vi.mocked(test.observer.onPublishCommitted).mockClear();

    await test.bus.runOnce();

    expect(committedDuringHandler).toEqual([0]);
    expect(test.observer.onPublishCommitted).toHaveBeenCalledWith({
      envelopes: [expect.objectContaining({ type: 'order.shipped.v1' })],
      deliveries: 1,
    });
  });

  it('reports nothing for a transactional handler whose delivery rolled back', async () => {
    const test = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 1, initialDelay: 1, factor: 1, jitter: 'none' },
    });
    test.bus.subscribe(
      OrderPlaced,
      async (event) => {
        await test.bus.publish(new OrderShipped({ orderId: event.payload.orderId }));
        throw new Error('handler failed');
      },
      { id: 'ship', transactional: true },
    );
    test.bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    await test.bus.publish(new OrderPlaced({ orderId: 'a' }));
    vi.mocked(test.observer.onPublishCommitted).mockClear();

    await test.bus.runOnce();

    expect(test.observer.onPublishCommitted).not.toHaveBeenCalled();
  });

  it('reports only the run a store kept when it reran the work of a bus.transaction', async () => {
    const test = createTestBus({ store: new RerunningStore() });
    test.bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    let runs = 0;

    await test.bus.transaction(async () => {
      runs += 1;
      await test.bus.publish(new OrderPlaced({ orderId: `run-${runs}` }));
    });

    expect(runs).toBe(2);
    expect(test.observer.onPublishCommitted).toHaveBeenCalledTimes(1);
    expect(test.observer.onPublishCommitted).toHaveBeenCalledWith({
      envelopes: [expect.objectContaining({ payload: { orderId: 'run-2' } })],
      deliveries: 1,
    });
  });

  it('reports only the run a store kept when it reran a transactional handler', async () => {
    const test = createTestBus({ store: new RerunningStore(), transactionMode: 'optional' });
    let runs = 0;
    test.bus.subscribe(
      OrderPlaced,
      async () => {
        runs += 1;
        await test.bus.publish(new OrderShipped({ orderId: `run-${runs}` }));
      },
      { id: 'ship', transactional: true },
    );
    test.bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    await test.bus.publish(new OrderPlaced({ orderId: 'a' }));
    vi.mocked(test.observer.onPublishCommitted).mockClear();

    await test.bus.runOnce();

    expect(runs).toBe(2);
    expect(test.observer.onPublishCommitted).toHaveBeenCalledTimes(1);
    expect(test.observer.onPublishCommitted).toHaveBeenCalledWith({
      envelopes: [expect.objectContaining({ payload: { orderId: 'run-2' } })],
      deliveries: 1,
    });
  });

  it('reports a publish into the outer bus.transaction from a nested one when the outer commits', async () => {
    const { bus, observer } = busWithHandler();

    await bus.transaction(async (outer) => {
      await bus.transaction(async () => {
        await bus.publish(new OrderPlaced({ orderId: 'into-outer' }), { transaction: outer });
      });
      expect(observer.onPublishCommitted).not.toHaveBeenCalled();
    });

    expect(observer.onPublishCommitted).toHaveBeenCalledWith({
      envelopes: [expect.objectContaining({ payload: { orderId: 'into-outer' } })],
      deliveries: 1,
    });
  });

  it('does not report a non-transactional handler publish twice', async () => {
    const test = createTestBus({ transactionMode: 'optional' });
    test.bus.subscribe(
      OrderPlaced,
      async (event) => {
        await test.bus.publish(new OrderShipped({ orderId: event.payload.orderId }));
      },
      { id: 'ship' },
    );
    test.bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    await test.bus.publish(new OrderPlaced({ orderId: 'a' }));
    vi.mocked(test.observer.onPublishCommitted).mockClear();

    await test.bus.runOnce();

    expect(test.observer.onPublishCommitted).toHaveBeenCalledTimes(1);
  });
});
