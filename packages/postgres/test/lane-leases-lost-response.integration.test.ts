import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PoolClient } from 'pg';
import { at, createEnvelope } from '@relaybus/testing';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import { LEASE_MS } from './support/claim.js';
import { createLaneHelpers } from './support/lane-helpers.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { append, claim } = createLaneHelpers(() => database);

describe('a claim whose response was lost', () => {
  /** A store whose statements can fail after the database executed them, like a connection reset before the answer. */
  async function createLossyStore() {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    const errors: string[] = [];
    const lossy = {
      loseNext: undefined as string | undefined,
      staleForever: false,
      failReconcile: undefined as unknown,
      failReconcileOnce: false,
      running: 0,
      maximumRunning: 0,
      late: undefined as (() => Promise<unknown>) | undefined,
      delayNext: false,
      delayReconcile: false,
      delayTransaction: false,
      reconciled: [] as unknown[],
    };
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => {
        if (lossy.delayTransaction) {
          lossy.delayTransaction = false;
          lossy.late = () => inner.transaction(work);
          return Promise.reject(new Error('query timeout'));
        }
        return inner.transaction(work);
      },
      query: async (sql, parameters, transaction) => {
        if (lossy.failReconcile !== undefined && sql.includes('SET in_flight = ARRAY(')) {
          const failure = lossy.failReconcile as Error;
          if (lossy.failReconcileOnce) {
            lossy.failReconcile = undefined;
          }
          throw failure;
        }
        if (sql.includes('SET in_flight = ARRAY(')) {
          lossy.reconciled.push(JSON.parse(parameters[1] as string));
        }
        if (lossy.delayReconcile && sql.includes('SET in_flight = ARRAY(')) {
          lossy.delayReconcile = false;
          lossy.late = () => inner.query(sql, parameters, transaction);
          throw new Error('query timeout');
        }
        if (lossy.staleForever && sql.includes('stale AS')) {
          return [
            {
              stale: true,
              delivery_id: null,
              held_lanes: '[]',
              examined_lanes: '[]',
              running_ids: '[]',
              window_saturated: false,
            },
          ];
        }
        if (lossy.delayNext && sql.includes('held AS MATERIALIZED')) {
          // The client gives up (a timeout); the statement is still on its way to the server.
          lossy.delayNext = false;
          lossy.late = () => inner.query(sql, parameters, transaction);
          throw new Error('query timeout');
        }
        const claimingHeld = sql.includes('held AS MATERIALIZED');
        if (claimingHeld) {
          lossy.running += 1;
          lossy.maximumRunning = Math.max(lossy.maximumRunning, lossy.running);
        }
        try {
          const rows = await inner.query(sql, parameters, transaction);
          if (lossy.loseNext !== undefined && sql.includes(lossy.loseNext)) {
            lossy.loseNext = undefined;
            throw new Error('connection reset before the answer');
          }
          return rows;
        } finally {
          if (claimingHeld) {
            lossy.running -= 1;
          }
        }
      },
    };
    const store = new PostgresEventStore({
      executor,
      schema,
      logger: {
        log: () => undefined,
        warn: () => undefined,
        error: (message) => void errors.push(message),
      },
    });
    await store.setup();
    return { store, lossy, errors };
  }

  it('does not keep the deliveries it recorded but never handed out', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    lossy.loseNext = 'held AS MATERIALIZED';
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // The statement recorded h2 in the lane; nobody received it, so it must run again from the next claim.
    const [again] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(again).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('keeps what a claim handed out when it reconciles', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    lossy.loseNext = 'held AS MATERIALIZED';
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // h1 was handed out and is still running: the reconciliation must not release it.
    expect(await claim(store, ['h1'], 'worker-1', at(2))).toEqual([]);
    expect(await store.complete(running!, at(3))).toBe(true);
  });

  it('reports a reconciliation that fails too and still raises the original error', async () => {
    const { store, lossy, errors } = await createLossyStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.failReconcile = new Error('still down');
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('connection reset');
    lossy.failReconcile = 'plain text failure';
    // The reconciliation owed from the first failure is retried first and fails too: no claim is made.
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('no claim is made');
    expect(errors).toEqual(
      expect.arrayContaining([
        expect.stringContaining('error=still down'),
        expect.stringContaining('error=plain text failure'),
      ]),
    );
  });

  it('does not forget the claim of a newer attempt when a stale lease of the delivery is settled', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [stale] = await claim(store, ['h1'], 'worker-1');
    expect(await store.release(stale!, at(1))).toBe(true);
    const [current] = await claim(store, ['h1'], 'worker-1', at(2));
    expect(await store.complete(stale!, at(3))).toBe(false);
    lossy.loseNext = 'held AS MATERIALIZED';
    await expect(claim(store, ['h2'], 'worker-1', at(4))).rejects.toThrow('connection reset');
    // The newer attempt is still handed out: the reconciliation must keep it listed and its lease valid.
    expect(await claim(store, ['h1'], 'worker-1', at(5))).toEqual([]);
    expect(await store.complete(current!, at(6))).toBe(true);
  });

  it('keeps the claim of a transactional completion that may still roll back', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    await expect(
      store.transaction(async (transaction) => {
        expect(await store.complete(running!, at(1), transaction)).toBe(true);
        lossy.loseNext = 'held AS MATERIALIZED';
        await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toThrow('connection reset');
        throw new Error('handler transaction rolled back');
      }),
    ).rejects.toThrow('rolled back');
    // The delivery is still running under its lease: the reconciliation must not have unlisted it.
    expect(await store.complete(running!, at(3))).toBe(true);
  });

  it('retries a failed reconciliation before the next claim of the owner', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.failReconcile = new Error('still down');
    lossy.failReconcileOnce = true;
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // h2 is recorded in the lane but nobody has it; the next claim reconciles first and then takes it.
    const [again] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(again).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('runs the claims of one owner one at a time', async () => {
    const { store, lossy } = await createLossyStore();
    await append(
      store,
      Array.from({ length: 6 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const batches = await Promise.all([
      claim(store, ['h'], 'worker-1', at(0), 2),
      claim(store, ['h'], 'worker-1', at(0), 2),
      claim(store, ['h'], 'worker-1', at(0), 2),
    ]);
    expect(lossy.maximumRunning).toBe(1);
    const ids = batches.flat().map((delivery) => delivery.deliveryId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toHaveLength(6);
  });

  it('ignores a claim statement that reaches the server after its caller gave up and the owner reconciled', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    lossy.delayNext = true;
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('query timeout');
    // The abandoned statement arrives only now, after the reconciliation of the owner.
    await lossy.late!();
    const [again] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(again).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('ignores the lane a free-lane claim takes when it reaches the server after its caller gave up', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    // The claim statement takes the free lane; it is what reaches the server late.
    lossy.delayNext = true;
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('query timeout');
    // The abandoned transaction takes the lane and records the delivery after the owner reconciled.
    await lossy.late!();
    // Nobody was given that delivery: the owner's next claim takes the lane over, like one of a crashed worker.
    const [again] = await claim(store, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ handler: 'h', attempt: 2 });
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    expect(await store.complete(again!, at(3))).toBe(true);
  });

  it('charges the attempt of a delivery its previous incarnation held when an owner restarts under the same name', async () => {
    const schema = uniqueSchema();
    const first = new PostgresEventStore({ executor: new PgExecutor(database.pool), schema });
    await first.setup();
    await append(first, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [poison] = await claim(first, ['h'], 'worker-1');
    expect(poison).toMatchObject({ attempt: 1 });
    // The process crashed with the delivery running and came back before its lease expired.
    const second = new PostgresEventStore({ executor: new PgExecutor(database.pool), schema });
    const [again] = await claim(second, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ deliveryId: poison!.deliveryId, attempt: 2 });
    expect(await first.complete(poison!, at(2))).toBe(false);
  });

  it('holds a taken-over lane for its new owner whatever claim floor the previous owner left', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ executor: new PgExecutor(database.pool), schema });
    await store.setup();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [first] = await claim(store, ['h'], 'worker-1');
    await database.pool.query(`UPDATE "${schema}".relaybus_lanes SET claim_floor = $1`, [
      String(Date.now() * 2),
    ]);
    // worker-1 dies; worker-2 takes the lane after its lease expired.
    const [taken] = await claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
    expect(taken).toMatchObject({ deliveryId: first!.deliveryId, attempt: 2 });
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['g'] }]);
    const [other] = await claim(store, ['g'], 'worker-2', at(LEASE_MS + 2));
    expect(other).toMatchObject({ handler: 'g', attempt: 1 });
  });

  it('does not retake a live lane of a newer generation when a free-lane claim arrives late', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    // The claim statement takes the free lane; it is what reaches the server late.
    lossy.delayNext = true;
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('query timeout');
    // The owner reconciled and claims again; the abandoned transaction arrives while that delivery runs.
    const [running] = await claim(store, ['h'], 'worker-1', at(1));
    expect(running).toBeDefined();
    await lossy.late!();
    expect(await store.extendLease(running!, at(LEASE_MS * 2))).toBe(true);
    expect(await claim(store, ['h'], 'worker-2', at(2))).toEqual([]);
    expect(await store.complete(running!, at(3))).toBe(true);
  });

  it('does not claim again while the reconciliation of a failed claim still fails', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.failReconcile = new Error('still down');
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // The generation moved on but the lanes were not reconciled: claiming now would treat the live lane as foreign.
    await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toThrow('no claim is made');
    await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toMatchObject({
      name: 'ReconcileFailedError',
      code: 'RELAYBUS_POSTGRES_RECONCILE_FAILED',
    });
    expect(await store.complete(running!, at(3))).toBe(true);
    lossy.failReconcile = undefined;
    const [next] = await claim(store, ['h2'], 'worker-1', at(4));
    expect(next).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('forgets a claim completed in the caller transaction once the next claim freed its lane', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [running] = await claim(store, ['h'], 'worker-1');
    await store.transaction((transaction) => store.complete(running!, at(1), transaction));
    // Nothing runs in the lane any more: this claim frees it.
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    lossy.loseNext = 'held AS MATERIALIZED';
    await append(store, [{ event: createEnvelope({ key: 'two' }), handlers: ['h'] }]);
    await expect(claim(store, ['h'], 'worker-1', at(3))).rejects.toThrow('connection reset');
    expect(lossy.reconciled).toEqual([[]]);
  });

  it('ignores a reconciliation that reaches the server after a newer reconciliation and a new claim', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [first] = await claim(store, ['h1'], 'worker-1');
    expect(first).toBeDefined();
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.delayReconcile = true;
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // The next claim reconciles again, then hands out the delivery the lost claim had recorded.
    const [second] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(second).toMatchObject({ handler: 'h2', attempt: 1 });
    // The first reconciliation arrives only now with its old picture of what was handed out.
    await lossy.late!();
    expect(await claim(store, ['h2'], 'worker-1', at(3))).toEqual([]);
    expect(await store.complete(second!, at(4))).toBe(true);
  });

  it('gives up a held-lane claim that stays stale and still takes free lanes', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    lossy.staleForever = true;
    expect(await claim(store, ['h'], 'worker-1')).toHaveLength(1);
  });
});
