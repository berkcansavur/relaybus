import { readFileSync } from 'node:fs';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDelivery, createEnvelope } from '@relaybus/testing';
import type { ClaimedDelivery, EventEnvelope } from '@relaybus/core';
// The published 1.1.0 package: lanes 0-255 only, no scope column.
import { PostgresEventStore as Version110Store } from 'relaybus-postgres-1.1.0';
import { PostgresEventStore } from '../src/index.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

/** The store API both versions share and the test uses. */
type Store = Pick<PostgresEventStore, 'setup' | 'transaction' | 'append' | 'claim' | 'complete'>;

function current(schema: string, laneScope?: string): PostgresEventStore {
  return new PostgresEventStore({
    pool: database.pool,
    schema,
    wakeUp: 'poll',
    ...(laneScope === undefined ? {} : { laneScope }),
  });
}

function version110(schema: string): Store {
  return new Version110Store({ pool: database.pool, schema, wakeUp: 'poll' });
}

async function publish(store: Store, event: EventEnvelope, handler: string): Promise<void> {
  await store.transaction((transaction) =>
    store.append(transaction, [{ event, deliveries: [createDelivery(handler)] }], new Date()),
  );
}

async function claim(
  store: Store,
  handlers: readonly string[],
  owner: string,
): Promise<ClaimedDelivery[]> {
  const now = new Date();
  return store.claim({
    handlers,
    limit: 50,
    leaseOwner: owner,
    leaseExpiresAt: new Date(now.getTime() + 60_000),
    now,
  });
}

/** Claims and completes until nothing is left; returns the event ids in the order they ran. */
async function drain(store: Store, handlers: readonly string[], owner: string): Promise<string[]> {
  const ran: string[] = [];
  for (let round = 0; round < 50; round += 1) {
    const claimed = await claim(store, handlers, owner);
    if (claimed.length === 0) {
      return ran;
    }
    for (const delivery of claimed) {
      expect(await store.complete(delivery, new Date())).toBe(true);
      ran.push(delivery.event.id);
    }
  }
  throw new Error('deliveries never ran out');
}

/** A key other than `key` whose deliveries share the default lane of `key`. */
async function keyInTheLaneOf(key: string): Promise<string> {
  const { rows } = await database.pool.query<{ key: string }>(
    `SELECT 'other-' || n AS key FROM generate_series(1, 100000) AS n
     WHERE hashtextextended('other-' || n, 0) & 255 = hashtextextended($1::text, 0) & 255
     LIMIT 1`,
    [key],
  );
  return rows[0]!.key;
}

async function deliveryLanes(schema: string): Promise<Map<string, number>> {
  const { rows } = await database.pool.query<{ event_id: string; lane: number }>(
    `SELECT event_id, lane FROM "${schema}".relaybus_deliveries`,
  );
  return new Map(rows.map((row) => [row.event_id, row.lane]));
}

async function scopeBlocks(
  schema: string,
): Promise<Map<string, { lanes: number; first: number; last: number }>> {
  const { rows } = await database.pool.query<{
    scope: string;
    lanes: number;
    first: number;
    last: number;
  }>(
    `SELECT scope, count(*)::int AS lanes, min(lane)::int AS first, max(lane)::int AS last
     FROM "${schema}".relaybus_lanes GROUP BY scope`,
  );
  return new Map(rows.map(({ scope, ...block }) => [scope, block]));
}

/** The DBA upgrade of the README, for `schema`, one statement at a time, as psql would send it. */
async function runReadmeUpgrade(schema: string): Promise<void> {
  const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');
  const section = readme.slice(
    readme.indexOf('## Lane scopes'),
    readme.indexOf('## Upgrading from 0.1.0'),
  );
  const sql = /```sql\n([\s\S]*?)```/.exec(section)![1]!.replaceAll('"public"', `"${schema}"`);
  expect(sql).not.toMatch(/\b(GRANT|OWNER|SET ROLE)\b/i);
  const client = await database.pool.connect();
  try {
    for (const statement of sql.split(/;\n\n/)) {
      await client.query(statement);
    }
    await client.query('RESET lock_timeout');
  } finally {
    client.release();
  }
}

describe('lane scopes', () => {
  it('a store of the default scope hides the deliveries of a lane it holds from other services', async () => {
    const schema = uniqueSchema();
    const orders = current(schema);
    const billing = current(schema);
    await orders.setup();
    const otherKey = await keyInTheLaneOf('order-1');
    await publish(orders, createEnvelope({ key: 'order-1' }), 'orders.handler');
    await publish(billing, createEnvelope({ key: otherKey }), 'billing.handler');

    expect(await claim(orders, ['orders.handler'], 'orders-worker')).toHaveLength(1);
    expect(await claim(billing, ['billing.handler'], 'billing-worker')).toHaveLength(0);
  });

  it('stores of two scopes never wait for each other, even for keys of one default lane', async () => {
    const schema = uniqueSchema();
    await current(schema).setup();
    const orders = current(schema, 'orders');
    const billing = current(schema, 'billing');
    const otherKey = await keyInTheLaneOf('order-1');
    await publish(orders, createEnvelope({ key: 'order-1' }), 'orders.handler');
    await publish(billing, createEnvelope({ key: otherKey }), 'billing.handler');

    expect(await claim(orders, ['orders.handler'], 'orders-worker')).toHaveLength(1);
    expect(await claim(billing, ['billing.handler'], 'billing-worker')).toHaveLength(1);

    const blocks = await scopeBlocks(schema);
    expect(blocks.get('default')).toEqual({ lanes: 256, first: 0, last: 255 });
    const ordersBlock = blocks.get('orders')!;
    const billingBlock = blocks.get('billing')!;
    expect(ordersBlock.lanes).toBe(256);
    expect(ordersBlock.last - ordersBlock.first).toBe(255);
    expect(billingBlock.lanes).toBe(256);
    expect(billingBlock.last - billingBlock.first).toBe(255);
    const lanes = [...(await deliveryLanes(schema)).values()].sort((a, b) => a - b);
    expect(lanes.every((lane) => lane >= 256)).toBe(true);
  });

  it('gives each scope one block when many processes start at once', async () => {
    const schema = uniqueSchema();
    await current(schema).setup();
    const scopes = ['orders', 'billing', 'shipping'];
    await Promise.all(
      scopes.flatMap((scope) =>
        Array.from({ length: 4 }, () => claim(current(schema, scope), ['any.handler'], scope)),
      ),
    );

    const blocks = await scopeBlocks(schema);
    const starts = scopes.map((scope) => {
      const block = blocks.get(scope)!;
      expect(block.lanes).toBe(256);
      expect(block.last - block.first).toBe(255);
      return block.first;
    });
    expect(new Set(starts).size).toBe(3);
    const { rows } = await database.pool.query<{ lanes: number }>(
      `SELECT count(*)::int AS lanes FROM "${schema}".relaybus_lanes`,
    );
    expect(rows[0]!.lanes).toBe(4 * 256);
  });

  it('upgrades a 1.1.0 schema with the README SQL while 1.1.0 holds deliveries, losing and repeating none', async () => {
    const schema = uniqueSchema();
    const old = version110(schema);
    await old.setup();
    const events = Array.from({ length: 6 }, (_, index) =>
      createEnvelope({ key: `order-${index}` }),
    );
    for (const event of events) {
      await publish(old, event, 'orders.handler');
    }
    const inFlight = await claim(old, ['orders.handler'], 'old-worker');
    expect(inFlight.length).toBeGreaterThan(0);
    const before = await deliveryLanes(schema);

    // 1.2.0 with the default scope runs on the schema as 1.1.0 left it: no upgrade needed.
    const next = current(schema);
    const claimedByNext = await claim(next, ['orders.handler'], 'next-worker');
    expect(claimedByNext).toHaveLength(events.length - inFlight.length);
    await runReadmeUpgrade(schema);
    await runReadmeUpgrade(schema);

    expect(await deliveryLanes(schema)).toEqual(before);
    for (const delivery of inFlight) {
      expect(await old.complete(delivery, new Date())).toBe(true);
    }
    for (const delivery of claimedByNext) {
      expect(await next.complete(delivery, new Date())).toBe(true);
    }
    const ran = [...inFlight, ...claimedByNext].map((delivery) => delivery.event.id);
    expect(ran).toHaveLength(events.length);
    expect(new Set(ran)).toEqual(new Set(events.map((event) => event.id)));
    expect(await deliveryLanes(schema)).toEqual(new Map());

    // The upgraded tables take a scope, and 1.1.0 keeps working next to it.
    const scoped = current(schema, 'orders');
    await publish(scoped, createEnvelope({ key: 'order-9' }), 'orders.handler');
    await publish(old, createEnvelope({ key: 'billing-1' }), 'billing.handler');
    expect(await drain(scoped, ['orders.handler'], 'scoped-worker')).toHaveLength(1);
    expect(await drain(old, ['billing.handler'], 'old-worker')).toHaveLength(1);
  });

  it('runs every delivery exactly once while 1.1.0 and 1.2.0 processes claim side by side', async () => {
    const schema = uniqueSchema();
    await version110(schema).setup();
    await runReadmeUpgrade(schema);
    const old = version110(schema);
    const next = current(schema);
    const published = Array.from({ length: 60 }, (_, index) =>
      createEnvelope({ key: index % 3 === 0 ? null : `order-${index % 20}` }),
    );
    for (const [index, event] of published.entries()) {
      await publish(index % 2 === 0 ? old : next, event, 'orders.handler');
    }

    const ran = await Promise.all([
      drain(old, ['orders.handler'], 'old-worker'),
      drain(next, ['orders.handler'], 'next-worker'),
      drain(old, ['orders.handler'], 'old-worker-2'),
      drain(next, ['orders.handler'], 'next-worker-2'),
    ]);
    const all = ran.flat();
    expect(all).toHaveLength(published.length);
    expect(new Set(all)).toEqual(new Set(published.map((event) => event.id)));
  });

  it('finishes the deliveries of the default lanes in key order after a service moves to a scope', async () => {
    const schema = uniqueSchema();
    const before = current(schema);
    await before.setup();
    const first = createEnvelope({ key: 'order-1' });
    const second = createEnvelope({ key: 'order-1' });
    const unkeyedBefore = createEnvelope();
    await publish(before, first, 'orders.handler');
    await publish(before, second, 'orders.handler');
    await publish(before, unkeyedBefore, 'orders.handler');

    const after = current(schema, 'orders');
    const third = createEnvelope({ key: 'order-1' });
    const fresh = createEnvelope({ key: 'order-2' });
    await publish(after, third, 'orders.handler');
    await publish(after, fresh, 'orders.handler');
    const block = (await scopeBlocks(schema)).get('orders')!;
    const lanes = await deliveryLanes(schema);
    expect(lanes.get(third.id)).toBe(block.first + lanes.get(first.id)!);
    expect(lanes.get(fresh.id)).toBeGreaterThanOrEqual(block.first);

    const ran = await drain(after, ['orders.handler'], 'scoped-worker');
    expect(new Set(ran)).toEqual(
      new Set([first.id, second.id, third.id, unkeyedBefore.id, fresh.id]),
    );
    const keyed = ran.filter((id) => [first.id, second.id, third.id].includes(id));
    expect(keyed).toEqual([first.id, second.id, third.id]);
  });

  it('keeps key order across the default lane and the block once every worker has the scope', async () => {
    const schema = uniqueSchema();
    const old = current(schema);
    await old.setup();
    const scoped = current(schema, 'orders');
    const published: string[] = [];
    for (let index = 0; index < 12; index += 1) {
      const event = createEnvelope({ key: 'order-1' });
      await publish(index % 2 === 0 ? old : scoped, event, 'orders.handler');
      published.push(event.id);
    }

    // Both workers complete into one log, so the log is the order the key ran in.
    const completed: string[] = [];
    const work = async (store: PostgresEventStore, owner: string): Promise<void> => {
      for (let idle = 0; idle < 20 && completed.length < published.length;) {
        const claimed = await claim(store, ['orders.handler'], owner);
        idle = claimed.length === 0 ? idle + 1 : 0;
        for (const delivery of claimed) {
          completed.push(delivery.event.id);
          expect(await store.complete(delivery, new Date())).toBe(true);
        }
      }
    };
    await Promise.all([
      work(scoped, 'scoped-worker'),
      work(current(schema, 'orders'), 'scoped-worker-2'),
    ]);
    expect(completed).toEqual(published);
  });

  it('holds a requeued delivery of a default lane while a later one of its key runs in the block', async () => {
    const schema = uniqueSchema();
    const old = current(schema);
    await old.setup();
    const scoped = current(schema, 'orders');
    const first = createEnvelope({ key: 'order-1' });
    await publish(old, first, 'orders.handler');
    const [head] = await claim(scoped, ['orders.handler'], 'worker-a');
    expect(head!.event.id).toBe(first.id);
    expect(await scoped.bury(head!, { name: 'Error', message: 'x' }, new Date())).toBe(true);
    await scoped.relinquish('worker-a');

    const second = createEnvelope({ key: 'order-1' });
    await publish(scoped, second, 'orders.handler');
    const [running] = await claim(scoped, ['orders.handler'], 'worker-b');
    expect(running!.event.id).toBe(second.id);
    expect(await scoped.requeue([head!.deliveryId], new Date())).toBe(1);

    // worker-c can lease the default lane, but the key runs in the block.
    expect(await claim(scoped, ['orders.handler'], 'worker-c')).toEqual([]);
    expect(await scoped.complete(running!, new Date())).toBe(true);
    const [requeued] = await claim(scoped, ['orders.handler'], 'worker-c');
    expect(requeued!.event.id).toBe(first.id);
  });

  it('holds a requeued delivery behind a full window of a lane it takes over while a later one of its key runs in the block', async () => {
    const schema = uniqueSchema();
    const old = current(schema);
    await old.setup();
    const scoped = current(schema, 'orders');
    const first = createEnvelope({ key: 'order-1' });
    await publish(old, first, 'orders.handler');
    const [head] = await claim(scoped, ['orders.handler'], 'worker-a');
    expect(await scoped.bury(head!, { name: 'Error', message: 'x' }, new Date())).toBe(true);
    await scoped.relinquish('worker-a');

    const second = createEnvelope({ key: 'order-1' });
    await publish(scoped, second, 'orders.handler');
    const [running] = await claim(scoped, ['orders.handler'], 'worker-b');
    expect(running!.event.id).toBe(second.id);
    // More deliveries of one key than the claim window holds, all due before the requeued one, so
    // the claim of the lane picks one of them and looks behind them for the heads of other keys.
    const neighbour = await keyInTheLaneOf('order-1');
    const followers = Array.from({ length: 70 }, () => createEnvelope({ key: neighbour }));
    for (const event of followers) {
      await publish(old, event, 'orders.handler');
    }
    // worker-x dies holding the default lane; worker-c takes it over once its lease expires.
    const now = new Date();
    const [lost] = await scoped.claim({
      handlers: ['orders.handler'],
      limit: 1,
      leaseOwner: 'worker-x',
      leaseExpiresAt: new Date(now.getTime() + 1_000),
      now,
    });
    expect(lost!.event.id).toBe(followers[0]!.id);
    expect(await scoped.requeue([head!.deliveryId], new Date())).toBe(1);

    const later = new Date(Date.now() + 2_000);
    const picked = await scoped.claim({
      handlers: ['orders.handler'],
      limit: 50,
      leaseOwner: 'worker-c',
      leaseExpiresAt: new Date(later.getTime() + 60_000),
      now: later,
    });
    expect(picked.map((delivery) => delivery.event.id)).toEqual([followers[0]!.id]);
  });

  it('gives concurrent first publishers of a scope one block', async () => {
    const schema = uniqueSchema();
    await current(schema).setup();
    const events = Array.from({ length: 8 }, (_, index) =>
      createEnvelope({ key: `order-${index}` }),
    );
    await Promise.all(
      events.map((event) => publish(current(schema, 'orders'), event, 'orders.handler')),
    );

    const block = (await scopeBlocks(schema)).get('orders')!;
    expect(block.lanes).toBe(256);
    for (const lane of (await deliveryLanes(schema)).values()) {
      expect(lane).toBeGreaterThanOrEqual(block.first);
      expect(lane).toBeLessThanOrEqual(block.last);
    }
  });

  it('publishes inside a transaction on a pool of one connection once the block exists', async () => {
    const schema = uniqueSchema();
    await current(schema, 'orders').setup();
    const pool = new Pool({ connectionString: database.connectionString, max: 1 });
    try {
      const store = new PostgresEventStore({ pool, schema, wakeUp: 'poll', laneScope: 'orders' });
      const event = createEnvelope({ key: 'order-1' });
      await publish(store, event, 'orders.handler');
      const block = (await scopeBlocks(schema)).get('orders')!;
      expect((await deliveryLanes(schema)).get(event.id)).toBeGreaterThanOrEqual(block.first);
    } finally {
      await pool.end();
    }
  }, 20_000);

  it('publishes inside a transaction on a pool of one connection while a claim waits for the connection', async () => {
    const schema = uniqueSchema();
    await current(schema, 'orders').setup();
    const pool = new Pool({ connectionString: database.connectionString, max: 1 });
    try {
      const store = new PostgresEventStore({ pool, schema, wakeUp: 'poll', laneScope: 'orders' });
      const event = createEnvelope({ key: 'order-1' });
      let claiming: Promise<ClaimedDelivery[]> | undefined;
      await store.transaction(
        async (transaction) => {
          // The claim looks the block up on its own connection, which this transaction holds.
          claiming = claim(store, ['orders.handler'], 'worker');
          await store.append(
            transaction,
            [{ event, deliveries: [createDelivery('orders.handler')] }],
            new Date(),
          );
        },
        { timeoutMs: 10_000 },
      );
      await claiming;
      const block = (await scopeBlocks(schema)).get('orders')!;
      expect((await deliveryLanes(schema)).get(event.id)).toBeGreaterThanOrEqual(block.first);
    } finally {
      await pool.end();
    }
  }, 20_000);

  it('publishes into the block from a repeatable read transaction that started before the block existed', async () => {
    const schema = uniqueSchema();
    await current(schema).setup();
    const store = current(schema, 'orders');
    const event = createEnvelope({ key: 'order-1' });
    await store.transaction(
      async (transaction) => {
        await transaction.query('SELECT 1');
        await current(schema, 'orders').setup();
        await store.append(
          transaction,
          [{ event, deliveries: [createDelivery('orders.handler')] }],
          new Date(),
        );
      },
      { timeoutMs: 10_000, isolationLevel: 'repeatable read' },
    );

    const block = (await scopeBlocks(schema)).get('orders')!;
    expect((await deliveryLanes(schema)).get(event.id)).toBeGreaterThanOrEqual(block.first);
    expect(await drain(store, ['orders.handler'], 'scoped-worker')).toEqual([event.id]);
  });

  it('leaves the deliveries of other handlers in the default lanes to their own service', async () => {
    const schema = uniqueSchema();
    const billing = current(schema);
    await billing.setup();
    await publish(billing, createEnvelope({ key: 'invoice-1' }), 'billing.handler');
    const orders = current(schema, 'orders');
    expect(await claim(orders, ['orders.handler'], 'orders-worker')).toHaveLength(0);
    expect(await claim(billing, ['billing.handler'], 'billing-worker')).toHaveLength(1);
  });

  it('rejects a scope that is not a plain name', () => {
    expect(() => current(uniqueSchema(), "orders'; DROP TABLE x; --")).toThrow();
    expect(() => current(uniqueSchema(), '')).toThrow();
  });
});
