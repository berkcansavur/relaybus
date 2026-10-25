import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Pool, PoolClient } from 'pg';
import { RelayBus, defineEvent, type ClaimedDelivery, type EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import {
  skipIdentifiers,
  createPerfHelpers,
  startPerfDatabase,
  claim,
  publishMany,
  events,
} from './support/perf-design.js';
import { createTestPool, uniqueSchema, type TestDatabase } from './support/postgres-container.js';
import { LEASE_MS } from './support/claim.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startPerfDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { closePools, pools, createFixture } = createPerfHelpers(() => database);

afterAll(closePools);

/** The tests of this file used to run after 387404 ids had been handed out by the ones before them. */
skipIdentifiers(387404);

describe('lock waits', () => {
  /** A store whose connections give up on any lock wait longer than 50 ms (SQLSTATE 55P03). */
  function impatientStore(schema: string): { store: PostgresEventStore; pool: Pool } {
    const pool = createTestPool(`${database.connectionString}?options=-c%20lock_timeout%3D50`, 4);
    pools.push(pool);
    return { store: new PostgresEventStore({ pool, schema }), pool };
  }

  /** Far longer than any lock wait of a busy test database; a wait for a lock the other side holds lasts forever. */
  const HELD_LOCK_TIMEOUT_MS = 2_000;

  /**
   * A store whose connections give up on a lock wait after {@link HELD_LOCK_TIMEOUT_MS}. `holdNextClaim`
   * runs its next claim statement in a transaction that commits only on `release()`: the claim keeps
   * every lane lock it took while the other worker runs, so any lock the two share makes that worker
   * wait until it fails, however fast or slow the machine is.
   */
  function lockHoldingStore(schema: string): {
    store: PostgresEventStore;
    pool: Pool;
    holdNextClaim(): { locked: Promise<void>; release(): void };
  } {
    const pool = createTestPool(
      `${database.connectionString}?options=-c%20lock_timeout%3D${HELD_LOCK_TIMEOUT_MS}`,
      4,
    );
    pools.push(pool);
    const inner = new PgExecutor(pool);
    let hold: { locked(): void; released: Promise<void> } | undefined;
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: (sql, parameters, transaction, options) => {
        const held = hold;
        if (transaction !== undefined || held === undefined || !sql.startsWith('WITH held AS')) {
          return inner.query(sql, parameters, transaction, options);
        }
        hold = undefined;
        return inner.transaction(async (claimTransaction) => {
          const rows = await inner.query(sql, parameters, claimTransaction, options);
          held.locked();
          await held.released;
          return rows;
        });
      },
    };
    return {
      store: new PostgresEventStore({ executor, schema }),
      pool,
      holdNextClaim: () => {
        let locked!: () => void;
        let release!: () => void;
        const lockedPromise = new Promise<void>((resolve) => (locked = resolve));
        const released = new Promise<void>((resolve) => (release = resolve));
        hold = { locked, released };
        return { locked: lockedPromise, release };
      },
    };
  }

  it('lets claimers of disjoint lanes and their completions run without waiting for each other', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(2_000));
    const first = lockHoldingStore(fixture.schema);
    const second = lockHoldingStore(fixture.schema);
    const handled = new Set<string>();
    const claimAndComplete = async (store: PostgresEventStore, owner: string): Promise<number> => {
      const claimed = await claim(store, 25, ['h'], owner);
      expect(await store.completeMany(claimed, at(1))).toEqual(claimed.map(() => true));
      for (const delivery of claimed) {
        handled.add(delivery.deliveryId);
      }
      return claimed.length;
    };
    /** Completes `claimed` in a transaction that stays open, holding its locks, while `meanwhile` runs. */
    const completeHoldingLocks = async (
      worker: { store: PostgresEventStore; pool: Pool },
      claimed: readonly ClaimedDelivery[],
      meanwhile: () => Promise<unknown>,
    ): Promise<void> => {
      const client = await worker.pool.connect();
      try {
        await client.query('BEGIN');
        for (const delivery of claimed) {
          expect(await worker.store.complete(delivery, at(1), client)).toBe(true);
        }
        await meanwhile();
        await client.query('COMMIT');
      } finally {
        client.release();
      }
      for (const delivery of claimed) {
        handled.add(delivery.deliveryId);
      }
    };

    // One worker's claim holds its lane locks while the other claims and completes twice.
    const heldClaim = first.holdNextClaim();
    const firstClaim = claim(first.store, 25, ['h'], 'worker-a');
    await heldClaim.locked;
    expect(await claimAndComplete(second.store, 'worker-b')).toBe(25);
    expect(await claimAndComplete(second.store, 'worker-b')).toBe(25);
    heldClaim.release();
    const firstBatch = await firstClaim;
    expect(firstBatch).toHaveLength(25);
    // One worker's completion holds its locks while the other claims and completes, and the other way round.
    await completeHoldingLocks(first, firstBatch, () => claimAndComplete(second.store, 'worker-b'));
    const secondBatch = await claim(second.store, 25, ['h'], 'worker-b');
    await completeHoldingLocks(second, secondBatch, () =>
      claimAndComplete(first.store, 'worker-a'),
    );
    // Both drain the rest side by side.
    const drain = async (store: PostgresEventStore, owner: string): Promise<void> => {
      while ((await claimAndComplete(store, owner)) > 0) {
        // Claim and complete until nothing is left.
      }
    };
    await Promise.all([drain(first.store, 'worker-a'), drain(second.store, 'worker-b')]);
    expect(handled.size).toBe(2_000);
  });

  it("does not make a worker's claim and renewal wait for its own completion in progress", async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(20));
    const { store, pool } = impatientStore(fixture.schema);
    const claimed = await claim(store, 10);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // A transactional handler's completion holds its lane locks until the transaction ends.
      expect(await store.complete(claimed[0]!, at(1), client)).toBe(true);
      expect(await store.extendLease(claimed[1]!, at(LEASE_MS * 2))).toBe(true);
      expect((await claim(store, 10)).length).toBeGreaterThan(0);
      await client.query('COMMIT');
    } finally {
      client.release();
    }
  });

  it('lets publishers of different keys run side by side and serializes publishers of one key', async () => {
    const fixture = await createFixture();
    const { store, pool } = impatientStore(fixture.schema);
    const holder = await pool.connect();
    const other = await pool.connect();
    try {
      await holder.query('BEGIN');
      await store.append(
        holder,
        [{ event: createEnvelope({ key: 'a' }), deliveries: [createDelivery('h')] }],
        at(0),
      );
      await other.query('BEGIN');
      // Another key: no wait.
      await store.append(
        other,
        [{ event: createEnvelope({ key: 'b' }), deliveries: [createDelivery('h')] }],
        at(0),
      );
      // The same key: waits for the first publisher to commit, which is what keeps commit order.
      await expect(
        store.append(
          other,
          [{ event: createEnvelope({ key: 'a' }), deliveries: [createDelivery('h')] }],
          at(0),
        ),
      ).rejects.toMatchObject({ code: '55P03' });
    } finally {
      await holder.query('ROLLBACK');
      await other.query('ROLLBACK');
      holder.release();
      other.release();
    }
  });
});

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

describe('publish-to-handler latency at default settings', () => {
  const quietLogger = { log: () => undefined, warn: () => undefined, error: () => undefined };

  it('runs a handler soon after another process committed its event', async () => {
    const schema = uniqueSchema();
    const publisherPool = createTestPool(database.connectionString, 4);
    const consumerPool = createTestPool(database.connectionString, 10);
    pools.push(publisherPool, consumerPool);
    const publisher = new RelayBus<PoolClient>({
      store: new PostgresEventStore({ pool: publisherPool, schema }),
      dispatcher: { enabled: false },
      logger: quietLogger,
    });
    const consumer = new RelayBus<PoolClient>({
      store: new PostgresEventStore({ pool: consumerPool, schema }),
      logger: quietLogger,
    });
    const committedAt = new Map<string, number>();
    const startedAt = new Map<string, number>();
    const handler = (event: EventEnvelope<{ orderId: string }>) => {
      startedAt.set(event.payload.orderId, Date.now());
    };
    publisher.subscribe(OrderPlaced, handler, { id: 'h' });
    consumer.subscribe(OrderPlaced, handler, { id: 'h' });
    await consumer.start();
    await publisher.start();
    // Let the consumer fall asleep between two polls.
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    const total = 30;
    try {
      for (let index = 0; index < total; index += 1) {
        if (index === total / 2) {
          // A lost listening connection must not cost more than one poll interval.
          await database.pool.query(
            `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ILIKE 'LISTEN %'`,
          );
        }
        const orderId = String(index);
        await publisher.transaction(async (transaction) => {
          await publisher.publish(new OrderPlaced({ orderId }), { transaction });
        });
        committedAt.set(orderId, Date.now());
        const deadline = Date.now() + 5_000;
        while (!startedAt.has(orderId) && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        // Space the events out, so each one finds the consumer idle again.
        await new Promise((resolve) => setTimeout(resolve, 100 + ((index * 37) % 200)));
      }
    } finally {
      await publisher.stop();
      await consumer.stop();
    }
    expect(startedAt.size).toBe(total);
    // A handler can start a moment before the publisher saw its commit acknowledged: that is 0 ms.
    const sorted = [...committedAt]
      .map(([orderId, committed]) => Math.max(0, startedAt.get(orderId)! - committed))
      .sort((left, right) => left - right);
    expectWithinBudget('postgres.latency.defaults.p95Ms', sorted[Math.ceil(total * 0.95) - 1]!);
    expectWithinBudget('postgres.latency.defaults.maxMs', sorted[total - 1]!);
  });
});
