import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createUuidV7, type EventEnvelope } from '@relaybus/core';
import { PostgresEventStore } from '../src/index.js';
import {
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

const BASE = 1_700_000_000_000;
const at = (offsetMs: number) => new Date(BASE + offsetMs);
const LEASE_MS = 60_000;

function envelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: createUuidV7(),
    type: 'test.event.v1',
    source: 'test',
    key: null,
    payload: {},
    headers: {},
    correlationId: null,
    causationId: null,
    occurredAt: at(0),
    ...overrides,
  };
}

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

function publish(
  store: PostgresEventStore,
  event: EventEnvelope,
  deliveryIds: readonly string[],
  now: Date = at(0),
): Promise<unknown> {
  return store.transaction((transaction) =>
    store.append(
      transaction,
      [
        {
          event,
          deliveries: deliveryIds.map((id) => ({ id, handler: 'h', availableAt: now })),
        },
      ],
      now,
    ),
  );
}

function lease(now: Date) {
  return {
    handlers: ['h'],
    limit: 10,
    leaseOwner: 'worker',
    leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
    now,
  };
}

describe('key that stalled behind an expired lease', () => {
  it('hands the requeued head out after the running successor lost its lease, then the successor', async () => {
    const store = new PostgresEventStore({ pool: database.pool, schema: uniqueSchema() });
    await store.setup();
    await publish(store, envelope({ key: 'k' }), ['d1']);
    await publish(store, envelope({ key: 'k' }), ['d2']);
    const [head] = await store.claim(lease(at(0)));
    expect(head!.deliveryId).toBe('d1');
    await store.bury(head!, { name: 'Error', message: 'x' }, at(1));
    const [successor] = await store.claim(lease(at(2)));
    expect(successor!.deliveryId).toBe('d2');
    expect(await store.requeue(['d1'], at(3))).toBe(1);

    // The successor's lease is still live: the requeued head must wait.
    expect(await store.claim(lease(at(LEASE_MS)))).toEqual([]);

    // Its worker crashed. The lease expired, so the head runs first and the successor waits for it.
    const [recovered] = await store.claim(lease(at(2 + LEASE_MS)));
    expect(recovered!.deliveryId).toBe('d1');
    expect(await store.claim(lease(at(3 + LEASE_MS)))).toEqual([]);
    expect(await store.complete(recovered!, at(4 + LEASE_MS))).toBe(true);
    const [retried] = await store.claim(lease(at(5 + LEASE_MS)));
    expect(retried).toMatchObject({ deliveryId: 'd2', attempt: 2 });
  });
});

describe('owned pool error handling', () => {
  it('survives a terminated idle connection, logs it and keeps working', async () => {
    const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const store = new PostgresEventStore({
      connectionString: database.connectionString,
      schema: uniqueSchema(),
      poolConfig: { max: 1, application_name: 'relaybus-idle-victim' },
      logger,
    });
    await store.setup();
    const uncaught = vi.fn();
    process.on('uncaughtException', uncaught);
    try {
      await database.pool.query(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'relaybus-idle-victim'`,
      );
      await vi.waitFor(() => expect(logger.error).toHaveBeenCalledTimes(1));
      expect(logger.error.mock.calls[0]![0]).toMatch(
        /^\[PostgresEventStore\]\(connect\) Idle connection failed/,
      );
      expect(uncaught).not.toHaveBeenCalled();
      await expect(store.statistics(at(0))).resolves.toMatchObject({ pending: 0 });
    } finally {
      process.off('uncaughtException', uncaught);
      await store.close();
    }
  });

  it('logs to the console when no logger is given', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const store = new PostgresEventStore({
      connectionString: database.connectionString,
      schema: uniqueSchema(),
      poolConfig: { max: 1, application_name: 'relaybus-console-victim' },
    });
    try {
      await store.setup();
      await database.pool.query(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'relaybus-console-victim'`,
      );
      await vi.waitFor(() => expect(consoleError).toHaveBeenCalledTimes(1));
    } finally {
      consoleError.mockRestore();
      await store.close();
    }
  });
});

describe('setup across table prefixes', () => {
  it('creates one new schema once when many stores with different prefixes race', async () => {
    const schema = uniqueSchema();
    const stores = Array.from(
      { length: 12 },
      (_, index) =>
        new PostgresEventStore({
          connectionString: database.connectionString,
          schema,
          tablePrefix: `bus${index}_`,
          poolConfig: { max: 1 },
        }),
    );
    await Promise.all(stores.map((store) => store.setup()));
    await Promise.all(stores.map((store) => store.close()));
    const result = await database.pool.query(
      `SELECT count(*)::int AS tables FROM information_schema.tables WHERE table_schema = $1`,
      [schema],
    );
    expect(result.rows[0]).toEqual({ tables: 60 });
  });
});

describe('purge of events without deliveries', () => {
  it('removes old delivery-less events in batches and keeps newer ones and referenced ones', async () => {
    const store = new PostgresEventStore({
      pool: database.pool,
      schema: uniqueSchema(),
      purgeBatchSize: 2,
    });
    await store.setup();
    for (let index = 0; index < 5; index += 1) {
      await publish(store, envelope(), [], at(index));
    }
    const referenced = envelope();
    await publish(store, referenced, ['d-ref'], at(0));
    const fresh = envelope();
    await publish(store, fresh, [], at(1_000));

    expect(await store.purge({})).toEqual({ deletedDeliveries: 0, deletedEvents: 0 });
    // The earliest cutoff decides, so the event created at 1000 survives.
    expect(await store.purge({ completedBefore: at(900), deadBefore: at(500) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 5,
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
    expect(await store.purge({ deadBefore: at(2_000) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 1,
    });
  });

  it('leaves an event alone when a delivery was attached before the sweep re-checks', async () => {
    const pool = createTestPool(database.connectionString, 2);
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool, schema });
    await store.setup();
    const event = envelope();
    await publish(store, event, [], at(0));
    const holder = await pool.connect();
    try {
      await holder.query('BEGIN');
      await holder.query(
        `INSERT INTO "${schema}".relaybus_deliveries (id, event_id, handler, lane, status, attempts, available_at, created_at, updated_at)
         VALUES ('late', $1, 'h', 0, 'pending', 0, now(), now(), now())`,
        [event.id],
      );
      // The uncommitted delivery holds a KEY SHARE lock on the event, so the sweep skips it.
      expect(await store.purge({ completedBefore: at(10) })).toEqual({
        deletedDeliveries: 0,
        deletedEvents: 0,
      });
      await holder.query('COMMIT');
    } finally {
      holder.release();
    }
    expect(await store.purge({ completedBefore: at(10) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 0,
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
    await pool.end();
  });
});

describe('claim behind a queue front clogged with blocked followers', () => {
  it('still leases ready work that sits behind more blocked followers than one scan window holds', async () => {
    const store = new PostgresEventStore({ pool: database.pool, schema: uniqueSchema() });
    await store.setup();
    const followerCount = 1_200;
    await store.transaction((transaction) =>
      store.append(
        transaction,
        Array.from({ length: followerCount }, (_, index) => ({
          event: envelope({ key: 'hot' }),
          deliveries: [
            { id: `hot-${String(index).padStart(5, '0')}`, handler: 'h', availableAt: at(0) },
          ],
        })),
        at(0),
      ),
    );
    const [runningHead] = await store.claim({ ...lease(at(1)), limit: 1 });
    expect(runningHead!.deliveryId).toBe('hot-00000');

    // Ready work due later than every follower, and one unkeyed delivery due before them.
    await publish(store, envelope({ key: 'other' }), ['other-1'], at(10));
    await publish(store, envelope(), ['plain-1'], at(11));
    await publish(store, envelope(), ['plain-0'], at(-5));

    const claimed = await store.claim({ ...lease(at(20)), limit: 10 });
    expect(claimed.map((delivery) => delivery.deliveryId)).toEqual([
      'plain-0',
      'other-1',
      'plain-1',
    ]);

    // Nothing else is claimable: every remaining follower is blocked by the running head.
    expect(await store.claim({ ...lease(at(21)), limit: 10 })).toEqual([]);
    expect(await store.complete(runningHead!, at(22))).toBe(true);
    const [next] = await store.claim({ ...lease(at(23)), limit: 10 });
    expect(next!.deliveryId).toBe('hot-00001');
  });
});
