import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createUuidV7 } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { PostgresEventStore } from '../src/index.js';
import { createVersion010Schema } from './support/version-0.1.0-schema.js';
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

const { append, claim, createStore } = createLaneHelpers(() => database);

describe('upgrade from 0.1.0', () => {
  async function seedVersion010(schema: string) {
    await createVersion010Schema(database.pool, schema);
    const events = Array.from({ length: 5 }, (_, index) =>
      createEnvelope({ key: index < 2 ? 'order-1' : null }),
    );
    for (const [index, event] of events.entries()) {
      await database.pool.query(
        `INSERT INTO "${schema}".relaybus_events (id, type, source, key, sequence, payload, headers, occurred_at, created_at)
         VALUES ($1, 'contract.event.v1', 's', $2, $3, '{}', '{}', now(), $4)`,
        [event.id, event.key, event.key === null ? null : index + 1, at(0)],
      );
    }
    const rows = [
      {
        id: createUuidV7(),
        event: events[0]!,
        status: 'processing',
        attempts: 2,
        key: 'order-1',
        sequence: 1,
      },
      {
        id: createUuidV7(),
        event: events[1]!,
        status: 'pending',
        attempts: 0,
        key: 'order-1',
        sequence: 2,
      },
      {
        id: createUuidV7(),
        event: events[2]!,
        status: 'completed',
        attempts: 1,
        key: null,
        sequence: null,
      },
      {
        id: createUuidV7(),
        event: events[3]!,
        status: 'dead',
        attempts: 5,
        key: null,
        sequence: null,
      },
      {
        id: createUuidV7(),
        event: events[4]!,
        status: 'pending',
        attempts: 1,
        key: null,
        sequence: null,
      },
    ];
    for (const row of rows) {
      await database.pool.query(
        `INSERT INTO "${schema}".relaybus_deliveries
           (id, event_id, handler, status, attempts, available_at, lease_owner, lease_token, lease_expires_at, key, sequence, created_at, updated_at, completed_at)
         VALUES ($1, $2, 'h', $3, $4, $5, $6, $7, $8, $9, $10, $5, $5, $11)`,
        [
          row.id,
          row.event.id,
          row.status,
          row.attempts,
          at(0),
          row.status === 'processing' ? 'old-worker' : null,
          row.status === 'processing' ? 'old-token' : null,
          row.status === 'processing' ? at(LEASE_MS) : null,
          row.key,
          row.sequence,
          row.status === 'completed' ? at(1) : null,
        ],
      );
    }
    return rows;
  }

  it('converts the tables in place, keeps every delivery and can run again', async () => {
    const schema = uniqueSchema();
    const rows = await seedVersion010(schema);
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    await store.setup();

    const columns = await database.pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = 'relaybus_deliveries'`,
      [schema],
    );
    const names = columns.rows.map((row) => row.column_name);
    expect(names).toContain('lane');
    expect(names).not.toEqual(expect.arrayContaining(['lease_token']));
    expect(names).not.toEqual(expect.arrayContaining(['lease_owner']));
    expect(names).not.toEqual(expect.arrayContaining(['lease_expires_at']));
    const indexes = await database.pool.query<{ indexname: string }>(
      `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 'relaybus_deliveries'`,
      [schema],
    );
    expect(indexes.rows.map((row) => row.indexname).sort()).toEqual([
      'relaybus_deliveries_completed_idx',
      'relaybus_deliveries_dead_idx',
      'relaybus_deliveries_event_handler_unique',
      'relaybus_deliveries_key_pending_idx',
      'relaybus_deliveries_lane_due_idx',
      'relaybus_deliveries_pkey',
    ]);

    // Every event remembers its handlers, so a handler that completed under 0.1.0 never runs again.
    const known = await database.pool.query<{ handlers: string[] }>(
      `SELECT handlers FROM "${schema}".relaybus_events WHERE id = $1`,
      [rows[2]!.event.id],
    );
    expect(known.rows[0]!.handlers).toEqual(['h']);
    const republished = await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: rows[2]!.event, deliveries: [createDelivery('h'), createDelivery('other')] }],
        at(LEASE_MS),
      ),
    );
    expect(republished).toMatchObject({
      duplicateEventIds: [rows[2]!.event.id],
      appendedDeliveryCount: 1,
    });

    // The two deliveries of one key share a lane, as new deliveries of that key would.
    const lanes = await database.pool.query<{ id: string; lane: number }>(
      `SELECT id, lane FROM "${schema}".relaybus_deliveries WHERE key = 'order-1' ORDER BY sequence`,
    );
    expect(new Set(lanes.rows.map((row) => row.lane)).size).toBe(1);

    expect(await store.statistics(at(LEASE_MS * 2))).toMatchObject({
      pending: 4,
      processing: 0,
      completed: 1,
      dead: 1,
    });
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS * 2));
    // The delivery that was processing keeps the attempt it was already charged.
    expect(claimed.map((delivery) => [delivery.deliveryId, delivery.attempt])).toEqual(
      expect.arrayContaining([
        [rows[0]!.id, 3],
        [rows[4]!.id, 2],
      ]),
    );
    expect(claimed.map((delivery) => delivery.deliveryId)).not.toContain(rows[1]!.id);
    expect(await store.completeMany(claimed, at(LEASE_MS * 2 + 1))).toEqual(
      claimed.map(() => true),
    );
    // The follower of the key is next, and legacy completed rows are purged as before.
    const [follower] = await claim(store, ['h'], 'worker-1', at(LEASE_MS * 2 + 2));
    expect(follower!.deliveryId).toBe(rows[1]!.id);
    expect(await store.purge({ completedBefore: at(LEASE_MS * 3) })).toMatchObject({
      deletedDeliveries: 1,
    });
  });

  it('upgrades a schema that has a custom prefix and no deliveries', async () => {
    const schema = uniqueSchema();
    await createVersion010Schema(database.pool, schema);
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    expect(await store.statistics(at(0))).toMatchObject({ pending: 0, oldestDueAt: null });
  });
});

describe('retention after an upgrade from 0.1.0', () => {
  it('keeps the event of a recently completed delivery when an old completed row of 0.1.0 is purged', async () => {
    const { store, schema } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['legacy', 'current'] }]);
    // The 0.1.0 delivery: kept as a completed row.
    await database.pool.query(
      `UPDATE "${schema}".relaybus_deliveries SET status = 'completed', completed_at = $1 WHERE handler = 'legacy'`,
      [at(10)],
    );
    const [current] = await claim(store, ['current'], 'worker-1', at(100));
    expect(await store.complete(current!, at(100))).toBe(true);
    expect(await store.purge({ completedBefore: at(50) })).toEqual({
      deletedDeliveries: 1,
      deletedEvents: 0,
    });
    const again = await store.transaction((transaction) =>
      store.append(transaction, [{ event, deliveries: [createDelivery('current')] }], at(101)),
    );
    expect(again.appendedDeliveryCount).toBe(0);
    expect(await store.purge({ completedBefore: at(200) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 1,
    });
  });
});
