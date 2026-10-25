import { setTimeout as sleep } from 'node:timers/promises';
import type { ClientSession, MongoClient } from 'mongodb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, TransactionTimeoutError, defineEvent } from '@relaybus/core';
import { MongoEventStore } from '../src/index.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };

let replicaSet: MongoReplicaSet;
let client: MongoClient;

beforeAll(async () => {
  replicaSet = await startReplicaSet();
  client = await replicaSet.connect();
});

afterAll(async () => {
  await client.close();
  await replicaSet.stop();
});

async function createBus(): Promise<{
  bus: RelayBus<ClientSession>;
  store: MongoEventStore;
  database: string;
}> {
  const database = uniqueDatabaseName();
  const store = new MongoEventStore({ client, database, logger: quiet });
  await store.setup();
  const bus = new RelayBus<ClientSession>({
    store,
    transactionTimeout: '500ms',
    dispatcher: { enabled: false },
    logger: quiet,
  });
  bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
  return { bus, store, database };
}

describe('transaction time budget on MongoDB', () => {
  it('commits a transaction that finishes within the budget', async () => {
    const { bus, store, database } = await createBus();
    await bus.transaction(async (session) => {
      await client
        .db(database)
        .collection('orders')
        .insertOne({ _id: 'o-1' } as never, { session });
      await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
    expect(await client.db(database).collection('orders').countDocuments()).toBe(1);
  });

  it('aborts a transaction held up by a slow external call, with all its writes', async () => {
    const { bus, store, database } = await createBus();
    const orders = client.db(database).collection('orders');

    const error = await bus
      .transaction(async (session) => {
        await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
        await sleep(1_000); // a slow HTTP call inside the transaction
        // An operation with its own client-side timeout keeps working inside the transaction.
        await orders.insertOne({ _id: 'o-1' } as never, { session, timeoutMS: 5_000 });
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    expect((error as Error).cause).toBeUndefined();
    expect(await store.listDeliveries({})).toEqual([]);
    expect(await orders.countDocuments()).toBe(0);
  });

  it('does not commit work that resolves after the budget ran out', async () => {
    const { bus, store } = await createBus();

    const error = await bus
      .transaction(async () => {
        await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
        await sleep(1_000);
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    expect(await store.listDeliveries({})).toEqual([]);
  });
});
