import type { PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, defineEvent } from '@relaybus/core';
import { FakeClock } from '@relaybus/testing';
import { PostgresEventStore } from '../src/index.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

function createBus(schema: string, clock: FakeClock) {
  const store = new PostgresEventStore({ pool: database.pool, schema });
  const bus = new RelayBus<PoolClient>({
    store,
    clock,
    source: 'e2e',
    dispatcher: { enabled: false, leaseDuration: '30s' },
    retry: { attempts: 3 },
    logger: { log: () => undefined, warn: () => undefined, error: () => undefined },
  });
  return { store, bus };
}

describe('RelayBus on PostgreSQL', () => {
  it('publishes in a transaction, runs the handler and completes the delivery', async () => {
    const { store, bus } = createBus(uniqueSchema(), new FakeClock());
    const handled: string[] = [];
    bus.subscribe(OrderPlaced, (event) => void handled.push(event.payload.orderId), { id: 'h' });
    await bus.start();
    await bus.transaction(async (transaction) => {
      await bus.publish(new OrderPlaced({ orderId: 'o-1' }), { transaction, key: 'o-1' });
    });
    expect(await bus.drain()).toBe(1);
    expect(handled).toEqual(['o-1']);
    const [record] = await store.listDeliveries({});
    expect(record).toMatchObject({ status: 'completed', attempts: 1, handler: 'h' });
    expect(record!.event.key).toBe('o-1');
    expect(await bus.statistics()).toMatchObject({ completed: 1, pending: 0 });
  });

  it('reclaims a delivery whose worker crashed once the lease expires', async () => {
    const clock = new FakeClock();
    const { store, bus } = createBus(uniqueSchema(), clock);
    const attempts: number[] = [];
    bus.subscribe(OrderPlaced, (_event, context) => void attempts.push(context.attempt), {
      id: 'h',
    });
    await bus.start();
    await bus.transaction(async (transaction) => {
      await bus.publish(new OrderPlaced({ orderId: 'o-1' }), { transaction });
    });

    // A worker claims the delivery and dies without settling it.
    const now = clock.now();
    const [crashed] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'crashed-worker',
      leaseExpiresAt: new Date(now.getTime() + 30_000),
      now,
    });
    expect(crashed!.attempt).toBe(1);

    clock.advance(29_999);
    expect(await bus.drain()).toBe(0);

    clock.advance(1);
    expect(await bus.drain()).toBe(1);
    expect(attempts).toEqual([2]);
    expect((await store.listDeliveries({}))[0]).toMatchObject({ status: 'completed', attempts: 2 });
    // The dead worker's late completion is fenced out.
    expect(await store.complete(crashed!, clock.now())).toBe(false);
  });

  it('keeps a key in order across retries', async () => {
    const clock = new FakeClock();
    const { bus } = createBus(uniqueSchema(), clock);
    const handled: string[] = [];
    let failFirst = true;
    bus.subscribe(
      OrderPlaced,
      (event) => {
        if (event.payload.orderId === 'first' && failFirst) {
          failFirst = false;
          throw new Error('transient');
        }
        handled.push(event.payload.orderId);
      },
      { id: 'h' },
    );
    await bus.start();
    for (const orderId of ['first', 'second']) {
      await bus.transaction(async (transaction) => {
        await bus.publish(new OrderPlaced({ orderId }), { transaction, key: 'same' });
      });
    }
    await bus.drain();
    expect(handled).toEqual([]);
    clock.advance(10 * 60_000);
    await bus.drain();
    expect(handled).toEqual(['first', 'second']);
  });
});
