import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  InMemoryEventStore,
  TRANSACTION_ISOLATION_LEVELS,
  TransactionTimeoutError,
  type EventStoreTransactionOptions,
  type InMemoryTransaction,
  type TransactionIsolationLevel,
} from '@relaybus/core';
import { OrderPlaced, createTestBus } from './helpers.js';

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

function isolatedBus(
  options: Parameters<typeof createTestBus>[0] = {},
): ReturnType<typeof createTestBus> & { readonly isolating: IsolatingStore } {
  const isolating = new IsolatingStore();
  const testBus = createTestBus({ store: isolating, ...options });
  testBus.bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
  return { ...testBus, isolating };
}

describe('bus.transaction with an isolation level', () => {
  it('passes the level and the budget to the store', async () => {
    const { bus, isolating } = isolatedBus();

    await bus.transaction(async () => 'done', { isolationLevel: 'serializable' });

    expect(isolating.seen).toEqual([{ timeoutMs: 5_000, isolationLevel: 'serializable' }]);
  });

  it('opens the transaction exactly as before without an isolation level', async () => {
    const { bus, isolating } = isolatedBus();

    await bus.transaction(async () => 'done');
    await bus.transaction(async () => 'done', {});
    await bus.transaction(async () => 'done', { maxAttempts: 9 });

    expect(isolating.seen).toEqual([
      { timeoutMs: 5_000 },
      { timeoutMs: 5_000 },
      { timeoutMs: 5_000 },
    ]);
    expect(isolating.seen.every((options) => !('isolationLevel' in options!))).toBe(true);
  });

  it('knows the level inside the transaction, and only inside the one opened with it', async () => {
    const { bus } = isolatedBus();
    const levels: Array<TransactionIsolationLevel | undefined> = [];

    levels.push(bus.currentIsolationLevel());
    await bus.transaction(
      async () => {
        levels.push(bus.currentIsolationLevel());
        // A nested transaction without a level is another transaction, at the database default.
        await bus.transaction(async () => {
          levels.push(bus.currentIsolationLevel());
        });
        await bus.transaction(
          async () => {
            levels.push(bus.currentIsolationLevel());
          },
          { isolationLevel: 'repeatable read' },
        );
        levels.push(bus.currentIsolationLevel());
      },
      { isolationLevel: 'serializable' },
    );
    await bus.transaction(async () => {
      levels.push(bus.currentIsolationLevel());
    });

    expect(levels).toEqual([
      undefined,
      'serializable',
      undefined,
      'repeatable read',
      'serializable',
      undefined,
    ]);
  });

  it('makes publish join the transaction and returns the result of work', async () => {
    const { bus, isolating } = isolatedBus();

    const result = await bus.transaction(
      async () => {
        await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
        return 'placed';
      },
      { isolationLevel: 'serializable' },
    );

    expect(result).toBe('placed');
    expect(await isolating.listDeliveries({})).toHaveLength(1);
  });

  it.each(['repeatable read', 'serializable'] as const)(
    'reruns work in a new transaction after a serialization failure at %s',
    async (isolationLevel) => {
      const { bus, isolating } = isolatedBus();
      const transactions = new Set<unknown>();
      let attempts = 0;

      const result = await bus.transaction(
        async (transaction) => {
          attempts += 1;
          transactions.add(transaction);
          await bus.publish(new OrderPlaced({ orderId: `o-${attempts}` }));
          if (attempts < 3) {
            throw new SerializationFailure('could not serialize access');
          }
          return attempts;
        },
        { isolationLevel },
      );

      expect(result).toBe(3);
      expect(transactions.size).toBe(3);
      expect(isolating.seen).toHaveLength(3);
      // The events of the two failed attempts were rolled back with them.
      const payloads = (await isolating.listDeliveries({})).map((d) => d.event.payload);
      expect(payloads).toEqual([{ orderId: 'o-3' }]);
    },
  );

  it('rethrows the last failure unchanged once the attempts are used up', async () => {
    const { bus, isolating } = isolatedBus();
    const failures: SerializationFailure[] = [];

    const error = await bus
      .transaction(
        async () => {
          await bus.publish(new OrderPlaced({ orderId: 'o' }));
          const failure = new SerializationFailure(`attempt ${failures.length + 1}`);
          failures.push(failure);
          throw failure;
        },
        { isolationLevel: 'serializable' },
      )
      .catch((caught: unknown) => caught);

    expect(failures).toHaveLength(3);
    expect(error).toBe(failures[2]);
    expect(isolating.seen).toHaveLength(3);
    expect(await isolating.listDeliveries({})).toEqual([]);
  });

  it('honours maxAttempts, one attempt meaning no rerun', async () => {
    const { bus } = isolatedBus();
    const attempts = vi.fn();
    const fail = (): Promise<never> => {
      attempts();
      return Promise.reject(new SerializationFailure('conflict'));
    };

    await expect(
      bus.transaction(fail, { isolationLevel: 'serializable', maxAttempts: 5 }),
    ).rejects.toBeInstanceOf(SerializationFailure);
    expect(attempts).toHaveBeenCalledTimes(5);

    attempts.mockClear();
    await expect(
      bus.transaction(fail, { isolationLevel: 'serializable', maxAttempts: 1 }),
    ).rejects.toBeInstanceOf(SerializationFailure);
    expect(attempts).toHaveBeenCalledTimes(1);
  });

  it('never reruns at read committed', async () => {
    const { bus } = isolatedBus();
    const attempts = vi.fn();

    await expect(
      bus.transaction(
        () => {
          attempts();
          return Promise.reject(new SerializationFailure('conflict'));
        },
        { isolationLevel: 'read committed', maxAttempts: 5 },
      ),
    ).rejects.toBeInstanceOf(SerializationFailure);

    expect(attempts).toHaveBeenCalledTimes(1);
  });

  it('does not rerun other errors', async () => {
    const { bus } = isolatedBus();
    const attempts = vi.fn();
    const failure = new Error('limit reached');

    await expect(
      bus.transaction(
        () => {
          attempts();
          return Promise.reject(failure);
        },
        { isolationLevel: 'serializable' },
      ),
    ).rejects.toBe(failure);

    expect(attempts).toHaveBeenCalledTimes(1);
  });

  it('does not rerun when the store cannot recognize a serialization failure', async () => {
    class BlindStore extends IsolatingStore {
      override isSerializationFailure = undefined as never;
    }
    const store = new BlindStore();
    const { bus } = createTestBus({ store });
    const attempts = vi.fn();

    await expect(
      bus.transaction(
        () => {
          attempts();
          return Promise.reject(new SerializationFailure('conflict'));
        },
        { isolationLevel: 'serializable' },
      ),
    ).rejects.toBeInstanceOf(SerializationFailure);

    expect(attempts).toHaveBeenCalledTimes(1);
  });

  it('holds every attempt to its own budget and does not rerun a timeout', async () => {
    const { bus, clock } = isolatedBus({ transactionTimeout: 1_000 });
    let attempts = 0;

    const error = await bus
      .transaction(
        async () => {
          attempts += 1;
          clock.advance(1_000);
          throw new SerializationFailure('terminated by the budget');
        },
        { isolationLevel: 'serializable' },
      )
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    expect(attempts).toBe(1);
  });

  it('does not rerun a timeout that wraps a serialization failure', async () => {
    class WrappingStore extends IsolatingStore {
      override isSerializationFailure(error: unknown): boolean {
        return error instanceof TransactionTimeoutError || super.isSerializationFailure(error);
      }
    }
    const { bus, clock } = createTestBus({
      store: new WrappingStore(),
      transactionTimeout: 1_000,
    });
    let attempts = 0;

    await expect(
      bus.transaction(
        async () => {
          attempts += 1;
          clock.advance(1_000);
          throw new SerializationFailure('ended by the budget');
        },
        { isolationLevel: 'serializable' },
      ),
    ).rejects.toBeInstanceOf(TransactionTimeoutError);

    expect(attempts).toBe(1);
  });

  it('opens a fresh transaction scope per attempt', async () => {
    const { bus } = isolatedBus();
    const seen: unknown[] = [];

    await bus.transaction(
      async (transaction) => {
        seen.push(transaction, bus.currentTransaction());
        if (seen.length === 2) {
          throw new SerializationFailure('conflict');
        }
      },
      { isolationLevel: 'serializable' },
    );

    expect(seen[0]).toBe(seen[1]);
    expect(seen[2]).toBe(seen[3]);
    expect(seen[2]).not.toBe(seen[0]);
  });

  describe('what a publish announces', () => {
    it('reports the events of the attempt that committed, after it committed', async () => {
      const { bus, observer } = isolatedBus();
      let attempts = 0;

      await bus.transaction(
        async () => {
          attempts += 1;
          await bus.publish(new OrderPlaced({ orderId: `o-${attempts}` }));
          // Not yet: this attempt may still be rolled back.
          expect(observer.onPublished).not.toHaveBeenCalled();
          if (attempts === 1) {
            throw new SerializationFailure('could not serialize access');
          }
        },
        { isolationLevel: 'serializable' },
      );

      expect(observer.onPublished).toHaveBeenCalledTimes(1);
      expect(observer.onPublished).toHaveBeenCalledWith({
        envelopes: [expect.objectContaining({ payload: { orderId: 'o-2' } })],
        deliveries: 1,
      });
    });

    it('reports nothing for a transaction that never committed', async () => {
      const { bus, observer } = isolatedBus();

      await expect(
        bus.transaction(
          async () => {
            await bus.publish(new OrderPlaced({ orderId: 'o' }));
            throw new Error('limit reached');
          },
          { isolationLevel: 'serializable' },
        ),
      ).rejects.toThrow('limit reached');

      expect(observer.onPublished).not.toHaveBeenCalled();
    });

    it('keeps reporting at publish time without an isolation level, as before', async () => {
      const { bus, observer } = isolatedBus();

      await bus.transaction(async () => {
        await bus.publish(new OrderPlaced({ orderId: 'o' }));
        expect(observer.onPublished).toHaveBeenCalledTimes(1);
      });
    });

    it('reports a publish into another transaction at once, and one of a nested transaction without a level when it is made', async () => {
      const { bus, observer } = isolatedBus();
      const announced: number[] = [];
      vi.mocked(observer.onPublished).mockImplementation(() => {
        announced.push(announced.length);
      });

      await bus.transaction(
        async () => {
          await bus.transaction(async () => {
            await bus.publish(new OrderPlaced({ orderId: 'nested' }));
            expect(announced).toEqual([0]);
          });
          await bus.publish(new OrderPlaced({ orderId: 'outer' }));
          expect(announced).toEqual([0]);
        },
        { isolationLevel: 'serializable' },
      );

      expect(announced).toEqual([0, 1]);
    });

    it('wakes the dispatcher only once the attempt that committed is over', async () => {
      const { bus, store, clock } = isolatedBus({ dispatcher: { pollInterval: '1h' } });
      await bus.start();
      try {
        const claim = vi.spyOn(store, 'claim');
        let attempts = 0;

        await bus.transaction(
          async () => {
            attempts += 1;
            await bus.publish(new OrderPlaced({ orderId: `o-${attempts}` }));
            if (attempts === 1) {
              throw new SerializationFailure('could not serialize access');
            }
            clock.advance(300); // past every wake-up delay of a publish in a caller's transaction
            await new Promise((resolve) => setImmediate(resolve));
            expect(claim).not.toHaveBeenCalled();
          },
          { isolationLevel: 'serializable' },
        );
        expect(claim).not.toHaveBeenCalled();

        clock.advance(25);
        await vi.waitFor(() => expect(claim).toHaveBeenCalled());
      } finally {
        await bus.stop();
      }
    });
  });

  describe('validation', () => {
    it('rejects an unknown level', async () => {
      const { bus, isolating } = isolatedBus();

      const error = await bus
        .transaction(async () => 'done', { isolationLevel: 'snapshot' as never })
        .catch((caught: unknown) => caught);

      expect(error).toBeInstanceOf(ConfigurationError);
      expect((error as Error).message).toBe(
        `Unknown isolationLevel "snapshot": use 'read committed', 'repeatable read', 'serializable'`,
      );
      expect(isolating.seen).toEqual([]);
    });

    it.each([0, -1, 1.5, Number.NaN, '3'])('rejects maxAttempts %j', async (maxAttempts) => {
      const { bus, isolating } = isolatedBus();

      await expect(
        bus.transaction(async () => 'done', {
          isolationLevel: 'serializable',
          maxAttempts: maxAttempts as never,
        }),
      ).rejects.toThrow(ConfigurationError);
      expect(isolating.seen).toEqual([]);
    });

    it('rejects a level the store does not list, naming the ones it does', async () => {
      class RepeatableOnly extends IsolatingStore {
        override readonly isolationLevels: readonly TransactionIsolationLevel[] = [
          'repeatable read',
        ];
      }
      const { bus } = createTestBus({ store: new RepeatableOnly() });

      const error = await bus
        .transaction(async () => 'done', { isolationLevel: 'serializable' })
        .catch((caught: unknown) => caught);

      expect(error).toBeInstanceOf(ConfigurationError);
      expect((error as Error).message).toMatch(
        /does not support isolationLevel 'serializable' \(supported: repeatable read\)/,
      );
    });

    it('rejects every level on the in-memory store, which runs at its own level only', async () => {
      const { bus, store } = createTestBus({ store: new InMemoryEventStore() });
      const transaction = vi.spyOn(store, 'transaction');

      for (const isolationLevel of TRANSACTION_ISOLATION_LEVELS) {
        const error = await bus
          .transaction(async () => 'done', { isolationLevel })
          .catch((caught: unknown) => caught);
        expect(error).toBeInstanceOf(ConfigurationError);
        expect((error as Error).message).toMatch(
          new RegExp(`does not support isolationLevel '${isolationLevel}' \\(supported: none\\)`),
        );
      }
      expect(transaction).not.toHaveBeenCalled();
    });
  });
});
