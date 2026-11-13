import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { Pool } from 'pg';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { PostgresEventStore, resolvePostgresNames } from '../src/index.js';
import { LEASE_MS, claimWithLease } from './support/claim.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;
beforeAll(async () => {
  database = await startDatabase();
});
afterAll(async () => {
  await database.stop();
});

async function newStore(): Promise<{ store: PostgresEventStore; schema: string }> {
  const schema = uniqueSchema();
  const store = new PostgresEventStore({ pool: database.pool, schema });
  await store.setup();
  return { store, schema };
}

function publish(
  store: PostgresEventStore,
  event: ReturnType<typeof createEnvelope>,
  options: { handler?: string; count?: number } = {},
) {
  const { handler = 'h', count = 1 } = options;
  return store.transaction((transaction) =>
    store.append(
      transaction,
      Array.from({ length: count }, () => ({ event, deliveries: [createDelivery(handler)] })),
      at(0),
    ),
  );
}

const claim = (store: PostgresEventStore, owner = 'w', now = at(0), handler = 'h') =>
  claimWithLease(store, { handlers: [handler], limit: 10, owner, now });

describe('values stored as text', () => {
  it('rejects a NUL in the ordering key with an error and keeps working afterwards', async () => {
    const { store } = await newStore();
    await expect(publish(store, createEnvelope({ key: 'a\u0000b' }))).rejects.toThrow();
    await publish(store, createEnvelope({ key: 'ok' }));
    expect(await claim(store)).toHaveLength(1);
  });

  it('stores handler, type, source, key and ids with quotes and SQL text verbatim', async () => {
    const { store } = await newStore();
    const hostile = `x'); DROP TABLE relaybus_events;-- "q" \\ $1 é中😀`;
    await publish(
      store,
      createEnvelope({
        type: hostile,
        source: hostile,
        key: hostile,
        correlationId: hostile,
        causationId: hostile,
      }),
      { handler: hostile },
    );
    const [claimed] = await claim(store, 'w', at(0), hostile);
    expect(claimed!.handler).toBe(hostile);
    expect(claimed!.event.type).toBe(hostile);
    expect(claimed!.event.key).toBe(hostile);
    expect(claimed!.event.correlationId).toBe(hostile);
  });

  it.each([
    ['schema with a dot', { schema: 'a.b' }],
    ['schema with non-ASCII letters', { schema: 'schéma' }],
    ['schema with a trailing newline', { schema: 'abc\n' }],
    ['prefix with a trailing newline', { tablePrefix: 'p_\n' }],
  ])('rejects a %s', (_label, options) => {
    expect(() => resolvePostgresNames(options)).toThrow();
  });

  it('round-trips a 12 MB payload through publish and claim', async () => {
    const { store } = await newStore();
    const big = 'x'.repeat(12 * 1024 * 1024);
    await publish(store, createEnvelope({ payload: { big } }));
    const [claimed] = await claim(store);
    expect((claimed!.event.payload as { big: string }).big.length).toBe(big.length);
  });
});

describe('leases and purging', () => {
  it('fences a running owner whose lease looks expired to a claimer with a clock that runs ahead', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope({ key: 'k' }));
    const [running] = await claim(store, 'slow-clock', at(0));
    const [stolen] = await claim(store, 'fast-clock', at(LEASE_MS + 1000));
    expect(stolen?.deliveryId).toBe(running!.deliveryId);
    expect(await store.complete(running!, at(1000))).toBe(false);
  });

  it('purge keeps pending, running and not yet due deliveries whatever the cutoffs', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope(), { count: 3 });
    await publish(store, createEnvelope({ key: 'k' }), { count: 2 });
    const [running] = await claimWithLease(store, {
      handlers: ['h'],
      limit: 1,
      owner: 'w',
      now: at(0),
    });
    await store.reschedule(running!, at(10 * 60_000), { name: 'E', message: 'm' }, at(1));
    const before = await store.statistics(at(2));
    const cutoff = new Date(Date.UTC(2100, 0, 1));
    const result = await store.purge({ completedBefore: cutoff, deadBefore: cutoff });
    expect(result).toEqual({ deletedDeliveries: 0, deletedEvents: 0 });
    expect(await store.statistics(at(2))).toEqual(before);
  });
});

describe('connections', () => {
  it('wakes on a publish, survives a terminated LISTEN backend and wakes again after the reconnect', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ connectionString: database.connectionString, schema });
    await store.setup();
    const listener = vi.fn();
    const stop = await store.watchNewDeliveries(listener);
    try {
      await publish(store, createEnvelope());
      await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 3000 });
      listener.mockClear();
      const terminated = await database.pool.query(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname = current_database() AND query LIKE 'LISTEN %' AND pid <> pg_backend_pid()`,
      );
      expect(terminated.rowCount).toBeGreaterThan(0);
      await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 5000 });
      listener.mockClear();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await publish(store, createEnvelope());
      await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 3000 });
    } finally {
      await stop?.();
      await store.close?.();
    }
  }, 20_000);

  it('returns every connection after failing statements, failed commits and rejected work', async () => {
    const pool = new Pool({ connectionString: database.connectionString, max: 3 });
    pool.on('error', () => undefined);
    try {
      const store = new PostgresEventStore({ pool, schema: uniqueSchema() });
      await store.setup();
      for (let round = 0; round < 12; round += 1) {
        await expect(
          store.transaction((transaction) => transaction.query('SELECT 1/0')),
        ).rejects.toThrow();
        await expect(store.transaction(() => Promise.reject(new Error('boom')))).rejects.toThrow(
          'boom',
        );
        await expect(
          store.transaction(() => Promise.resolve(), { timeoutMs: Number.NaN }),
        ).rejects.toThrow();
      }
      await publish(store, createEnvelope());
      expect(pool.totalCount - pool.idleCount).toBe(0);
      expect(await claim(store)).toHaveLength(1);
    } finally {
      await pool.end();
    }
  }, 30_000);
});
