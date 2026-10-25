import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PoolClient } from 'pg';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import { LEASE_MS } from './support/claim.js';
import { createLaneHelpers, error } from './support/lane-helpers.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { append, claim, createStore } = createLaneHelpers(() => database);

describe('takeover of a lane that changed while the statement waited', () => {
  it('charges only the deliveries the locked version of the lane still lists', async () => {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    let gated = false;
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: (sql, parameters, transaction) =>
        inner.query(
          gated && sql.includes('FOR UPDATE OF free SKIP LOCKED')
            ? // Waits, after the statement took its snapshot and before it locks the lane, for the test.
              sql
                .replace(
                  'WHERE free.lease_expires_at IS NULL',
                  'WHERE (SELECT pg_advisory_xact_lock_shared(7777) IS NOT NULL) AND (free.lease_expires_at IS NULL',
                )
                .replace(
                  'ORDER BY oldest.available_at, oldest.id',
                  ') ORDER BY oldest.available_at, oldest.id',
                )
            : sql,
          parameters,
          transaction,
        ),
    };
    const store = new PostgresEventStore({ executor, schema });
    await store.setup();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [crashed] = await claim(store, ['h'], 'worker-1');
    const blocker = await database.pool.connect();
    try {
      await blocker.query('SELECT pg_advisory_lock(7777)');
      gated = true;
      const takeover = claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
      for (;;) {
        const waiting = await database.pool.query(
          "SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        );
        if (waiting.rowCount !== 0) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      // The previous owner records its failed attempt after the takeover statement took its snapshot.
      expect(await store.reschedule(crashed!, at(0), error, at(LEASE_MS + 1))).toBe(true);
      await blocker.query('SELECT pg_advisory_unlock(7777)');
      const [taken] = await takeover;
      // One attempt failed (charged by the reschedule) and this is the second: it was not charged twice.
      expect(taken).toMatchObject({ deliveryId: crashed!.deliveryId, attempt: 2 });
    } finally {
      blocker.release();
    }
  });
});

describe('claim behind a saturated fast window', () => {
  it('claims the head of every key of a lane whose front is full of followers, one per key, in commit order', async () => {
    const { store, schema } = await createStore();
    const head = createEnvelope({ key: 'blocked' });
    await append(store, [{ event: head, handlers: ['h'] }], at(0));
    const [running] = await claim(store, ['h'], 'worker-0', at(0), 1);
    expect(running!.event.id).toBe(head.id);
    // More due followers of the key than one lane window reads, so they fill the front of the lane.
    for (let index = 0; index < 80; index += 1) {
      await append(store, [{ event: createEnvelope({ key: 'blocked' }), handlers: ['h'] }], at(2));
    }
    const lane = await database.pool.query(
      `SELECT lane FROM "${schema}".relaybus_deliveries WHERE id = $1`,
      [running!.deliveryId],
    );
    const laneId = lane.rows[0]!.lane;
    // Keys and event ids hash to lanes at random (256 lanes), so keep adding until the lane of
    // 'blocked' holds two other keys; a fixed count would miss the lane on some runs.
    const probes = new Map<string, string>();
    let probeIndex = 0;
    let sameLane: { event_id: string }[] = [];
    while (sameLane.length < 2 && probeIndex < 20_000) {
      for (let batch = 0; batch < 200; batch += 1, probeIndex += 1) {
        const event = createEnvelope({ key: `probe-${probeIndex}` });
        probes.set(event.id, `probe-${probeIndex}`);
        await append(store, [{ event, handlers: ['h'] }], at(100));
      }
      sameLane = (
        await database.pool.query<{ event_id: string }>(
          `SELECT event_id FROM "${schema}".relaybus_deliveries WHERE lane = $1 AND key LIKE 'probe-%' ORDER BY sequence`,
          [laneId],
        )
      ).rows;
    }
    const [first, second] = sameLane.map((row) => row.event_id);
    expect(second).toBeDefined();
    // And unkeyed deliveries, which have no ordering to keep: at least one must land in the lane.
    let unkeyedInLane = 0;
    for (let batch = 0; batch < 100 && unkeyedInLane === 0; batch += 1) {
      await append(
        store,
        Array.from({ length: 1_000 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
        at(100),
      );
      unkeyedInLane = (
        await database.pool.query(
          `SELECT 1 FROM "${schema}".relaybus_deliveries WHERE lane = $1 AND key IS NULL`,
          [laneId],
        )
      ).rowCount!;
    }
    const laneUnkeyedRows = await database.pool.query<{ event_id: string }>(
      `SELECT event_id FROM "${schema}".relaybus_deliveries WHERE lane = $1 AND key IS NULL`,
      [laneId],
    );
    const laneUnkeyed = new Set(laneUnkeyedRows.rows.map((row) => row.event_id));
    expect(laneUnkeyed.size).toBeGreaterThan(0);
    const firstFollower = createEnvelope({ key: probes.get(first!)! });
    await append(store, [{ event: firstFollower, handlers: ['h'] }], at(101));
    // The first worker lost its lease: the lane has to be taken over, in the second step of the claim.
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 1), 5_000);
    const inLane = claimed
      .map((delivery) => delivery.event.id)
      .filter((id) => id === head.id || id === first || id === second || id === firstFollower.id);
    expect(inLane).toEqual([head.id, first, second]);
    expect(claimed.filter((delivery) => delivery.event.key === 'blocked')).toHaveLength(1);
    expect(claimed.filter((delivery) => laneUnkeyed.has(delivery.event.id))).toHaveLength(
      laneUnkeyed.size,
    );
  });
});

describe('interleavings of two statements of one call', () => {
  async function hookedStore(
    hook: (sql: string, schema: string) => Promise<void>,
  ): Promise<{ store: PostgresEventStore; schema: string }> {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: async (sql, parameters, transaction) => {
        const rows = await inner.query(sql, parameters, transaction);
        await hook(sql, schema);
        return rows;
      },
    };
    const store = new PostgresEventStore({ executor, schema });
    await store.setup();
    return { store, schema };
  }

  it('hands out only one delivery of a key when a dead letter is requeued while the claim runs', async () => {
    let requeue: (() => Promise<void>) | undefined;
    const { store, schema } = await hookedStore(async (sql) => {
      if (requeue !== undefined && sql.includes('held AS MATERIALIZED')) {
        const run = requeue;
        requeue = undefined;
        await run();
      }
    });
    const events = Array.from({ length: 72 }, () => createEnvelope({ key: 'one-key' }));
    for (const event of events) {
      await append(store, [{ event, handlers: ['h'] }]);
    }
    const [head] = await claim(store, ['h'], 'worker-0', at(0), 1);
    expect(head!.event.id).toBe(events[0]!.id);
    expect(await store.bury(head!, error, at(1))).toBe(true);
    requeue = async () => {
      await database.pool.query(
        `UPDATE "${schema}".relaybus_deliveries SET status = 'pending', attempts = 0 WHERE id = $1`,
        [head!.deliveryId],
      );
    };
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 2), 200);
    // The claim is one statement with one snapshot: the dead letter requeued after it took the snapshot
    // is not seen, so exactly one delivery of the key runs, and the requeued one waits behind it.
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([events[1]!.id]);
    expect(await claim(store, ['h'], 'worker-1', at(LEASE_MS + 3), 200)).toEqual([]);
  });

  it('keeps an event whose completion time moved after the purge selected it', async () => {
    let touch: (() => Promise<void>) | undefined;
    const { store, schema } = await hookedStore(async (sql) => {
      if (touch !== undefined && sql.includes('ORDER BY completion.completed_at')) {
        const run = touch;
        touch = undefined;
        await run();
      }
    });
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1', at(0));
    expect(await store.complete(delivery!, at(10))).toBe(true);
    // A newer completion of the same event is recorded after the sweep selected the event.
    touch = async () => {
      await database.pool.query(
        `UPDATE "${schema}".relaybus_event_completions SET completed_at = $1 WHERE event_id = $2`,
        [at(1_000), event.id],
      );
    };
    expect(await store.purge({ completedBefore: at(100) })).toMatchObject({ deletedEvents: 0 });
    expect(await store.purge({ completedBefore: at(2_000) })).toMatchObject({ deletedEvents: 1 });
  });

  it('appends deliveries of an existing event next to an existing event without deliveries', async () => {
    const { store } = await createStore();
    const first = createEnvelope();
    const second = createEnvelope();
    await append(store, [
      { event: first, handlers: [] },
      { event: second, handlers: [] },
    ]);
    await append(
      store,
      [
        { event: first, handlers: ['h'] },
        { event: second, handlers: [] },
      ],
      at(1),
    );
    expect(await store.listDeliveries({ eventId: first.id })).toHaveLength(1);
    expect(await store.listDeliveries({ eventId: second.id })).toHaveLength(0);
  });

  it('keeps an event that a purge removed between the insert and the lock of an append', async () => {
    let purgeEvent: (() => Promise<void>) | undefined;
    const { store, schema } = await hookedStore(async (sql) => {
      if (purgeEvent !== undefined && sql.includes('ON CONFLICT (id) DO NOTHING')) {
        const run = purgeEvent;
        purgeEvent = undefined;
        await run();
      }
    });
    const event = createEnvelope();
    await append(store, [{ event, handlers: [] }]);
    purgeEvent = async () => {
      await database.pool.query(`DELETE FROM "${schema}".relaybus_events WHERE id = $1`, [
        event.id,
      ]);
    };
    const result = await store.transaction((transaction) =>
      store.append(transaction, [{ event, deliveries: [createDelivery('h')] }], at(1)),
    );
    expect(result.appendedDeliveryCount).toBe(1);
    expect(await store.listDeliveries({ eventId: event.id })).toHaveLength(1);
  });
});
