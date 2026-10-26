import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import type { EventEnvelope } from '@relaybus/core';
import { PostgresEventStore, createPostgresSchemaStatements } from '../src/index.js';
import { version040SchemaMarker } from '../src/schema.js';
import { claimWithLease } from './support/claim.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

/** Turns the payload columns back into the `jsonb` of 0.4.0 and forgets the schema marker. */
async function downgradeToVersion040(schema: string): Promise<void> {
  await database.pool.query(`ALTER TABLE "${schema}".relaybus_events
  ALTER COLUMN headers DROP DEFAULT,
  ALTER COLUMN payload TYPE jsonb USING payload::jsonb,
  ALTER COLUMN headers TYPE jsonb USING headers::jsonb,
  ALTER COLUMN headers SET DEFAULT '{}'::jsonb`);
  await database.pool.query(`COMMENT ON TABLE "${schema}".relaybus_lanes IS NULL`);
}

async function columnTypes(schema: string): Promise<Record<string, string>> {
  const { rows } = await database.pool.query<{ column_name: string; column_default: string }>(
    `SELECT column_name, data_type || ' ' || coalesce(column_default, '') AS column_default
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = 'relaybus_events' AND column_name IN ('payload', 'headers')`,
    [schema],
  );
  return Object.fromEntries(rows.map((row) => [row.column_name, row.column_default]));
}

async function publish(store: PostgresEventStore, event: EventEnvelope): Promise<void> {
  await store.transaction((transaction) =>
    store.append(transaction, [{ event, deliveries: [createDelivery('handler-a')] }], at(0)),
  );
}

describe('upgrade of the jsonb payload columns', () => {
  it('turns them into json in place, keeps old rows and keeps the order of new ones', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    const payload = { zulu: 1, a: { yy: 2, b: 3 } };
    const old = createEnvelope({ payload, headers: { zz: 'z', a: 'a' } });
    await publish(store, old);
    await downgradeToVersion040(schema);
    expect(await columnTypes(schema)).toEqual({
      payload: 'jsonb ',
      headers: "jsonb '{}'::jsonb",
    });
    // Before the upgrade runs (a rolling deploy, or setupStore: false without the DDL) publishing
    // still works: PostgreSQL casts json to jsonb on assignment.
    const beforeUpgrade = createEnvelope({ payload: { zz: 1, a: 2 } });
    await publish(store, beforeUpgrade);

    await store.setup();
    await store.setup();
    expect(await columnTypes(schema)).toEqual({ payload: 'json ', headers: "json '{}'::json" });
    const fresh = createEnvelope({ payload, headers: { zz: 'z', a: 'a' } });
    await publish(store, fresh);
    const claimed = await claimWithLease(store, {
      handlers: ['handler-a'],
      limit: 10,
      owner: 'worker-1',
      now: at(0),
    });
    const byId = new Map(claimed.map((delivery) => [delivery.event.id, delivery.event]));
    // Rows stored before the upgrade keep the key order jsonb gave them; the values are intact.
    expect(byId.get(old.id)).toEqual(old);
    expect(byId.get(beforeUpgrade.id)).toEqual(beforeUpgrade);
    expect(JSON.stringify(byId.get(old.id)!.payload)).toBe('{"a":{"b":3,"yy":2},"zulu":1}');
    expect(JSON.stringify(byId.get(fresh.id)!.payload)).toBe(JSON.stringify(payload));
    expect(JSON.stringify(byId.get(fresh.id)!.headers)).toBe('{"zz":"z","a":"a"}');
  });
});

describe('upgrade next to a running completion', () => {
  /** Whether a backend other than the test's own waits for a lock. */
  async function someoneWaitsForALock(): Promise<boolean> {
    const { rows } = await database.pool.query<{ waiting: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock'
       ) AS waiting`,
    );
    return rows[0]!.waiting;
  }

  it('does not deadlock with a completion that locked deliveries before the event rows', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    const event = createEnvelope();
    await publish(store, event);
    await downgradeToVersion040(schema);
    // A completion: it deletes its delivery first, then records the event as completed, whose
    // foreign key check locks the event row.
    const completion = await database.pool.connect();
    try {
      await completion.query('BEGIN');
      await completion.query(`DELETE FROM "${schema}".relaybus_deliveries WHERE event_id = $1`, [
        event.id,
      ]);
      const upgrading = store.setup();
      await vi.waitFor(async () => expect(await someoneWaitsForALock()).toBe(true), {
        timeout: 60_000,
        interval: 20,
      });
      await completion.query(
        `INSERT INTO "${schema}".relaybus_event_completions (event_id, completed_at) VALUES ($1, now())`,
        [event.id],
      );
      await completion.query('COMMIT');
      await upgrading;
    } finally {
      completion.release();
    }
    expect(await columnTypes(schema)).toEqual({ payload: 'json ', headers: "json '{}'::json" });
  });
});

describe('upgrade of a 0.4.0 schema', () => {
  it('converts the columns and moves the marker on without locking the other tables', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    await downgradeToVersion040(schema);
    await database.pool.query(
      `COMMENT ON TABLE "${schema}".relaybus_lanes IS '${version040SchemaMarker({ schema })}'`,
    );
    // A claim reading deliveries and lanes: any schema statement on those tables would wait for it.
    const claim = await database.pool.connect();
    try {
      await claim.query('BEGIN');
      await claim.query(`SELECT 1 FROM "${schema}".relaybus_deliveries LIMIT 1`);
      await claim.query(`SELECT 1 FROM "${schema}".relaybus_lanes LIMIT 1`);
      await store.setup();
      await claim.query('COMMIT');
    } finally {
      claim.release();
    }
    expect(await columnTypes(schema)).toEqual({ payload: 'json ', headers: "json '{}'::json" });
    const { rows } = await database.pool.query<{ marker: string }>(
      `SELECT obj_description('"${schema}".relaybus_lanes'::regclass, 'pg_class') AS marker`,
    );
    expect(rows[0]!.marker).toBe(
      createPostgresSchemaStatements({ schema })
        .at(-1)!
        .match(/'(relaybus schema [0-9a-f]+)'/)![1],
    );
  });
});
