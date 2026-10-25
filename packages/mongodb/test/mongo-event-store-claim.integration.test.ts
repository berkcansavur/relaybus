import { type Document, type ClientSession } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { RelayBus, defineEvent, type ClaimedDelivery, type RelayBusLogger } from '@relaybus/core';
import { at, createDelivery, createEnvelope, FakeClock } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { uniqueDatabaseName } from './support.js';
import { client, useReplicaSet, createStore } from './store-support.js';

useReplicaSet();

const silentLogger: RelayBusLogger = { log: () => {}, warn: () => {}, error: () => {} };

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

describe('claim under contention', () => {
  it('never runs two deliveries of one handler and key at once, never claims one twice, keeps publish order', async () => {
    const { store } = await createStore({ completionHistory: true });
    const handlers = ['handler-a', 'handler-b'];
    const keys = Array.from({ length: 8 }, (_, index) => `key-${index}`);
    const eventsPerKey = 20;
    const publishOrder = new Map<string, string[]>();
    const total = keys.length * eventsPerKey * handlers.length + 30 * handlers.length;

    const publishers = keys.map(async (key) => {
      for (let index = 0; index < eventsPerKey; index += 1) {
        const event = createEnvelope({ key, payload: { index } });
        await store.transaction((session) =>
          store.append(
            session,
            [{ event, deliveries: handlers.map((handler) => createDelivery(handler)) }],
            at(0),
          ),
        );
        for (const handler of handlers) {
          const slot = `${handler}/${key}`;
          publishOrder.set(slot, [...(publishOrder.get(slot) ?? []), event.id]);
        }
      }
    });
    const unkeyed = (async () => {
      for (let index = 0; index < 30; index += 1) {
        await store.transaction((session) =>
          store.append(
            session,
            [
              {
                event: createEnvelope(),
                deliveries: handlers.map((handler) => createDelivery(handler)),
              },
            ],
            at(0),
          ),
        );
      }
    })();

    const inFlight = new Set<string>();
    const completionOrder = new Map<string, string[]>();
    const completedIds = new Set<string>();
    const violations: string[] = [];
    let finished = 0;
    let retries = 0;

    async function handle(delivery: ClaimedDelivery): Promise<void> {
      const slot =
        delivery.event.key === null ? undefined : `${delivery.handler}/${delivery.event.key}`;
      if (slot !== undefined) {
        if (inFlight.has(slot)) {
          violations.push(`two in flight for ${slot}`);
        }
        inFlight.add(slot);
      }
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 4));
      const shouldRetry = retries < 40 && Math.random() < 0.1;
      if (slot !== undefined) {
        inFlight.delete(slot);
      }
      if (shouldRetry) {
        retries += 1;
        expect(
          await store.reschedule(delivery, at(0), { name: 'Error', message: 'retry' }, at(0)),
        ).toBe(true);
        return;
      }
      expect(await store.complete(delivery, at(0))).toBe(true);
      if (completedIds.has(delivery.deliveryId)) {
        violations.push(`completed twice ${delivery.deliveryId}`);
      }
      completedIds.add(delivery.deliveryId);
      if (slot !== undefined) {
        completionOrder.set(slot, [...(completionOrder.get(slot) ?? []), delivery.event.id]);
      }
      finished += 1;
    }

    async function worker(index: number): Promise<void> {
      const deadline = Date.now() + 90_000;
      while (finished < total && Date.now() < deadline) {
        const claimed = await store.claim({
          handlers,
          limit: 5,
          leaseOwner: `worker-${index}`,
          leaseExpiresAt: at(10 ** 9),
          now: at(0),
        });
        await Promise.all(claimed.map(handle));
        if (claimed.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      }
    }

    await Promise.all(
      [publishers, unkeyed, ...Array.from({ length: 10 }, (_, i) => worker(i))].flat(),
    );
    expect(violations).toEqual([]);
    expect(finished).toBe(total);
    for (const [slot, expected] of publishOrder) {
      expect(completionOrder.get(slot)).toEqual(expected);
    }
    expect((await store.statistics(at(0))).completed).toBe(total);
  });
});

describe('RelayBus on MongoDB', () => {
  async function createBus(
    clock: FakeClock,
    database: string,
    completionHistory = false,
  ): Promise<{ bus: RelayBus<ClientSession>; store: MongoEventStore }> {
    const store = new MongoEventStore({ client, database, completionHistory });
    const bus = new RelayBus<ClientSession>({
      store,
      clock,
      logger: silentLogger,
      random: () => 0.5,
      retry: { attempts: 3, initialDelay: 1_000, jitter: 'none' },
    });
    await store.setup();
    return { bus, store };
  }

  it('publishes with a business write and runs the handler to completion', async () => {
    const database = uniqueDatabaseName();
    const { bus } = await createBus(new FakeClock(), database, true);
    const received: string[] = [];
    bus.subscribe(OrderPlaced, (event) => void received.push(event.payload.orderId), {
      id: 'audit',
    });
    await bus.transaction(async (session) => {
      await client.db(database).collection('orders').insertOne({ orderId: '1' }, { session });
      await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'order-1' });
    });
    await bus.drain();
    expect(received).toEqual(['1']);
    expect(await client.db(database).collection('orders').countDocuments()).toBe(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'completed', attempts: 1 });
  });

  it('stores neither the business write nor the event when the transaction fails', async () => {
    const database = uniqueDatabaseName();
    const { bus } = await createBus(new FakeClock(), database);
    bus.subscribe(OrderPlaced, () => {}, { id: 'audit' });
    await expect(
      bus.transaction(async (session) => {
        await client.db(database).collection('orders').insertOne({ orderId: '2' }, { session });
        await bus.publish(new OrderPlaced({ orderId: '2' }));
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    expect(await client.db(database).collection('orders').countDocuments()).toBe(0);
    expect(await bus.listDeliveries()).toEqual([]);
  });

  it('commits a transactional handler write together with the completion and rolls both back on failure', async () => {
    const database = uniqueDatabaseName();
    const clock = new FakeClock();
    const { bus } = await createBus(clock, database, true);
    const projections = client.db(database).collection<Document>('projections');
    let failNext = true;
    bus.subscribe(
      OrderPlaced,
      async (event, context) => {
        await projections.insertOne(
          { orderId: event.payload.orderId },
          { session: context.transaction! },
        );
        if (failNext) {
          failNext = false;
          throw new Error('after the write');
        }
      },
      { id: 'projector', transactional: true },
    );
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '3' })));
    await bus.runOnce();
    expect(await projections.countDocuments()).toBe(0);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'pending', attempts: 1 });
    clock.advance(1_000);
    await bus.runOnce();
    expect(await projections.countDocuments()).toBe(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'completed', attempts: 2 });
  });

  it('reclaims a delivery whose worker disappeared once the lease expired', async () => {
    const database = uniqueDatabaseName();
    const clock = new FakeClock();
    const { bus, store } = await createBus(clock, database);
    const attempts: number[] = [];
    bus.subscribe(OrderPlaced, (_event, context) => void attempts.push(context.attempt), {
      id: 'audit',
    });
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '4' })));
    const [lost] = await store.claim({
      handlers: ['audit'],
      limit: 1,
      leaseOwner: 'crashed-worker',
      leaseExpiresAt: new Date(clock.now().getTime() + 30_000),
      now: clock.now(),
    });
    await bus.runOnce();
    expect(attempts).toEqual([]);
    clock.advance(30_000);
    await bus.runOnce();
    expect(attempts).toEqual([2]);
    expect(await store.complete(lost!, clock.now())).toBe(false);
  });
});
