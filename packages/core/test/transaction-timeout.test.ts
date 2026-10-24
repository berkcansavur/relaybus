import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  InMemoryEventStore,
  RelayBusError,
  TransactionTimeoutError,
} from '@relaybus/core';
import { OrderPlaced, createTestBus } from './helpers.js';

function timedBus(
  options: Parameters<typeof createTestBus>[0] = {},
): ReturnType<typeof createTestBus> {
  const testBus = createTestBus({ store: new InMemoryEventStore(), ...options });
  testBus.bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
  return testBus;
}

describe('transaction time budget', () => {
  it('passes the budget to the store, 5 seconds by default', async () => {
    const { bus, store } = timedBus();
    const transaction = vi.spyOn(store, 'transaction');
    await bus.transaction(async () => 'done');
    expect(transaction).toHaveBeenCalledWith(expect.any(Function), { timeoutMs: 5_000 });

    const custom = timedBus({ transactionTimeout: '30s' });
    const customTransaction = vi.spyOn(custom.store, 'transaction');
    await custom.bus.transaction(async () => 'done');
    expect(customTransaction).toHaveBeenCalledWith(expect.any(Function), { timeoutMs: 30_000 });
  });

  it('rolls back a transaction that outlives its budget and names the likely cause', async () => {
    const { bus, store, clock } = timedBus();
    const overrun = bus.transaction(async () => {
      await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
      clock.advance(5_000);
      return 'committed?';
    });

    const error = await overrun.catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    expect(error).toBeInstanceOf(RelayBusError);
    expect(error).toMatchObject({
      code: 'RELAYBUS_TRANSACTION_TIMEOUT',
      name: 'TransactionTimeoutError',
    });
    expect((error as Error).message).toBe(
      'The transaction ran past its 5000 ms budget (transactionTimeout) and was rolled back. ' +
        'The usual cause is a slow call to another service (HTTP, gRPC, a broker, a lock) inside ' +
        'the transaction: make external calls before the transaction starts or after it commits, ' +
        'or raise transactionTimeout',
    );
    expect((error as Error).cause).toBeUndefined();
    expect(await store.listDeliveries({})).toEqual([]);
  });

  it('reports an error raised after the budget ran out as a timeout caused by it', async () => {
    const { bus, clock } = timedBus({ transactionTimeout: 1_000 });
    const killed = new Error('terminating connection due to idle-in-transaction timeout');

    const error = await bus
      .transaction(async () => {
        clock.advance(1_000);
        throw killed;
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    expect((error as Error).cause).toBe(killed);
    expect((error as Error).message).toMatch(
      /ran past its 1000 ms budget .* \| cause=terminating connection due to idle-in-transaction timeout$/,
    );
  });

  it('keeps errors raised within the budget and results that finish in time', async () => {
    const { bus, clock } = timedBus();
    const failure = new Error('payment declined');

    await expect(
      bus.transaction(async () => {
        clock.advance(4_999);
        throw failure;
      }),
    ).rejects.toBe(failure);
    await expect(
      bus.transaction(async () => {
        clock.advance(4_999);
        return 'in time';
      }),
    ).resolves.toBe('in time');
  });

  it.each([0, '0s', -1])('rejects transactionTimeout %j', (transactionTimeout) => {
    expect(() => createTestBus({ transactionTimeout: transactionTimeout as never })).toThrow(
      ConfigurationError,
    );
  });
});
