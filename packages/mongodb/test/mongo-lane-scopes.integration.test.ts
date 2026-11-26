import { describe, expect, it } from 'vitest';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import type { EventEnvelope } from '@relaybus/core';
import { MongoEventStore } from '../src/index.js';
import { laneOf } from '../src/lanes.js';
import {
  append,
  claim,
  client,
  collections,
  createStore,
  error,
  recordCommands,
  useReplicaSet,
} from './store-support.js';
import { uniqueDatabaseName } from './support.js';

useReplicaSet();

function scoped(database: string, laneScope?: string): MongoEventStore {
  return new MongoEventStore({
    client,
    database,
    ...(laneScope === undefined ? {} : { laneScope }),
  });
}

/** A key other than `key` whose deliveries share the default lane of `key`. */
function keyInTheLaneOf(key: string): string {
  for (let index = 1; ; index += 1) {
    if (laneOf(`other-${index}`, '') === laneOf(key, '')) {
      return `other-${index}`;
    }
  }
}

async function publish(
  store: MongoEventStore,
  event: EventEnvelope,
  handler: string,
): Promise<void> {
  await append(store, [{ event, handlers: [handler] }]);
}

async function drain(store: MongoEventStore, handler: string, owner: string): Promise<string[]> {
  const ran: string[] = [];
  for (let round = 0; round < 50; round += 1) {
    const claimed = await claim(store, [handler], owner);
    if (claimed.length === 0) {
      return ran;
    }
    expect(await store.completeMany(claimed, new Date())).toEqual(claimed.map(() => true));
    ran.push(...claimed.map((delivery) => delivery.event.id));
  }
  throw new Error('deliveries never ran out');
}

async function laneIds(database: string): Promise<Map<string, number>> {
  const deliveries = await collections(database).deliveries.find({}).toArray();
  return new Map(
    deliveries.map((delivery) => [delivery['eventId'] as string, delivery['laneId'] as number]),
  );
}

async function blocks(
  database: string,
): Promise<Map<string, { lanes: number; first: number; last: number }>> {
  const lanes = await collections(database).lanes.find({}).toArray();
  const byScope = new Map<string, number[]>();
  for (const lane of lanes) {
    const scope = (lane['scope'] as string | undefined) ?? 'default';
    byScope.set(scope, [...(byScope.get(scope) ?? []), lane._id as number]);
  }
  return new Map(
    [...byScope].map(([scope, ids]) => [
      scope,
      { lanes: ids.length, first: Math.min(...ids), last: Math.max(...ids) },
    ]),
  );
}

describe('lane scopes', () => {
  it('a store of the default scope hides the deliveries of a lane it holds from other services', async () => {
    const { store: orders, database } = await createStore();
    const billing = scoped(database);
    await publish(orders, createEnvelope({ key: 'order-1' }), 'orders.handler');
    await publish(billing, createEnvelope({ key: keyInTheLaneOf('order-1') }), 'billing.handler');

    expect(await claim(orders, ['orders.handler'], 'orders-worker')).toHaveLength(1);
    expect(await claim(billing, ['billing.handler'], 'billing-worker')).toHaveLength(0);
  });

  it('stores of two scopes never wait for each other, even for keys of one default lane', async () => {
    const { database } = await createStore();
    const orders = scoped(database, 'orders');
    const billing = scoped(database, 'billing');
    await publish(orders, createEnvelope({ key: 'order-1' }), 'orders.handler');
    await publish(billing, createEnvelope({ key: keyInTheLaneOf('order-1') }), 'billing.handler');

    expect(await claim(orders, ['orders.handler'], 'orders-worker')).toHaveLength(1);
    expect(await claim(billing, ['billing.handler'], 'billing-worker')).toHaveLength(1);
    expect([...(await laneIds(database)).values()].every((lane) => lane >= 256)).toBe(true);
  });

  it('gives each scope one block when many processes start at once', async () => {
    const database = uniqueDatabaseName();
    await scoped(database).setup();
    const scopes = ['orders', 'billing', 'shipping'];
    await Promise.all(
      scopes.flatMap((scope) =>
        Array.from({ length: 4 }, () => claim(scoped(database, scope), ['any.handler'], scope)),
      ),
    );

    const allocated = await blocks(database);
    expect(allocated.get('default')).toEqual({ lanes: 256, first: 0, last: 255 });
    const starts = scopes.map((scope) => {
      expect(allocated.get(scope)!.lanes).toBe(256);
      expect(allocated.get(scope)!.last - allocated.get(scope)!.first).toBe(255);
      return allocated.get(scope)!.first;
    });
    expect(new Set(starts).size).toBe(3);
  });

  it('finishes the deliveries of the default lanes in key order after a service moves to a scope', async () => {
    const { store: before, database } = await createStore();
    const first = createEnvelope({ key: 'order-1' });
    const second = createEnvelope({ key: 'order-1' });
    const unkeyedBefore = createEnvelope();
    await publish(before, first, 'orders.handler');
    await publish(before, second, 'orders.handler');
    await publish(before, unkeyedBefore, 'orders.handler');

    const after = scoped(database, 'orders');
    const third = createEnvelope({ key: 'order-1' });
    await publish(after, third, 'orders.handler');
    expect((await laneIds(database)).get(third.id)).toBeGreaterThanOrEqual(256);

    const ran = await drain(after, 'orders.handler', 'scoped-worker');
    expect(new Set(ran)).toEqual(new Set([first.id, second.id, third.id, unkeyedBefore.id]));
    expect(ran.filter((id) => [first.id, second.id, third.id].includes(id))).toEqual([
      first.id,
      second.id,
      third.id,
    ]);
  });

  it('holds a requeued delivery of a default lane while a later one of its key runs in the block', async () => {
    const { store: old, database } = await createStore();
    const scopedStore = scoped(database, 'orders');
    const first = createEnvelope({ key: 'order-1' });
    await publish(old, first, 'orders.handler');
    const [head] = await claim(scopedStore, ['orders.handler'], 'worker-a');
    expect(head!.event.id).toBe(first.id);
    expect(await scopedStore.bury(head!, error, at(1))).toBe(true);
    await scopedStore.relinquish('worker-a');

    const second = createEnvelope({ key: 'order-1' });
    await publish(scopedStore, second, 'orders.handler');
    const [running] = await claim(scopedStore, ['orders.handler'], 'worker-b', at(2));
    expect(running!.event.id).toBe(second.id);
    expect(await scopedStore.requeue([head!.deliveryId], at(3))).toBe(1);

    // worker-c can lease the default lane, but the key runs in the block.
    expect(await claim(scopedStore, ['orders.handler'], 'worker-c', at(4))).toEqual([]);
    expect(await scopedStore.complete(running!, at(5))).toBe(true);
    const [requeued] = await claim(scopedStore, ['orders.handler'], 'worker-c', at(6));
    expect(requeued!.event.id).toBe(first.id);
  });

  it('claims a keyed delivery of a default lane while an unkeyed one runs in the matching lane of the block', async () => {
    const { store: old, database } = await createStore();
    const scopedStore = scoped(database, 'orders');
    // An unkeyed delivery must not block the key 'undefined' of its handler.
    const key = 'undefined';
    let unkeyed = createEnvelope();
    while (laneOf(null, unkeyed.id) !== laneOf(key, '')) {
      unkeyed = createEnvelope();
    }
    await publish(scopedStore, unkeyed, 'orders.handler');
    const [running] = await claim(scopedStore, ['orders.handler'], 'worker-a');
    expect(running!.event.id).toBe(unkeyed.id);
    expect((await laneIds(database)).get(unkeyed.id)).toBe(laneOf(key, '') + 256);

    const keyed = createEnvelope({ key });
    await publish(old, keyed, 'orders.handler');
    const [claimed] = await claim(scopedStore, ['orders.handler'], 'worker-b');
    expect(claimed!.event.id).toBe(keyed.id);
  });

  it('allocates the block of a first publish inside a transaction that read before, in one attempt', async () => {
    const { database, events } = await createStore();
    const store = scoped(database, 'orders');
    let attempts = 0;
    await store.transaction(async (session) => {
      attempts += 1;
      await events.findOne({}, { session });
      await store.append(
        session,
        [
          {
            event: createEnvelope({ key: 'order-1' }),
            deliveries: [createDelivery('orders.handler')],
          },
        ],
        new Date(),
      );
    });
    expect(attempts).toBe(1);
    expect([...(await laneIds(database)).values()][0]).toBeGreaterThanOrEqual(256);
  }, 20_000);

  it("publishes inside the caller's transaction without a command outside it once its store has the block", async () => {
    const { database } = await createStore();
    const store = scoped(database, 'orders');
    await store.setup();
    const commands = await recordCommands(() =>
      publish(store, createEnvelope({ key: 'order-1' }), 'orders.handler'),
    );
    const outside = commands.filter(
      (command) =>
        command.databaseName === database &&
        (command.command as { txnNumber?: unknown }).txnNumber === undefined,
    );
    expect(outside.map((command) => command.commandName)).toEqual([]);
    expect((await laneIds(database)).size).toBe(1);
    expect([...(await laneIds(database)).values()][0]).toBeGreaterThanOrEqual(256);
  });

  it('leaves the deliveries of other handlers in the default lanes to their own service', async () => {
    const { store: billing, database } = await createStore();
    await publish(billing, createEnvelope({ key: 'invoice-1' }), 'billing.handler');
    const orders = scoped(database, 'orders');
    expect(await claim(orders, ['orders.handler'], 'orders-worker')).toHaveLength(0);
    expect(await claim(billing, ['billing.handler'], 'billing-worker')).toHaveLength(1);
  });

  it('claims unkeyed deliveries of its block', async () => {
    const { database } = await createStore();
    const store = scoped(database, 'orders');
    const event = createEnvelope();
    await publish(store, event, 'orders.handler');
    expect(await drain(store, 'orders.handler', 'worker')).toEqual([event.id]);
  });

  it('rejects a scope when the lane numbers have no room for its block, and allocates once there is', async () => {
    const { database } = await createStore();
    const { lanes } = collections(database);
    await lanes.insertOne({ _id: 32_600, scope: 'filler' });
    const store = scoped(database, 'orders');
    await expect(claim(store, ['orders.handler'], 'worker')).rejects.toThrow(/no room/);

    await lanes.deleteOne({ _id: 32_600 });
    expect(await claim(store, ['orders.handler'], 'worker')).toEqual([]);
    expect((await blocks(database)).get('orders')).toEqual({ lanes: 256, first: 256, last: 511 });
  });

  it('passes on errors of the allocation other than a block taken by another store', async () => {
    const { database } = await createStore();
    const { lanes } = collections(database);
    await client.db(database).command({
      collMod: lanes.collectionName,
      validator: { scopeHead: { $exists: false } },
    });
    await expect(claim(scoped(database, 'orders'), ['orders.handler'], 'worker')).rejects.toThrow(
      /validation/i,
    );
  });

  it('allocates the first block above the default lanes before they are seeded', async () => {
    const database = uniqueDatabaseName();
    expect(await claim(scoped(database, 'orders'), ['orders.handler'], 'worker')).toEqual([]);
    expect((await blocks(database)).get('orders')).toEqual({ lanes: 256, first: 256, last: 511 });
  });

  it('rejects a scope that is not a plain name', () => {
    expect(() => scoped(uniqueDatabaseName(), 'Orders With Spaces')).toThrow();
    expect(() => scoped(uniqueDatabaseName(), '')).toThrow();
  });
});
