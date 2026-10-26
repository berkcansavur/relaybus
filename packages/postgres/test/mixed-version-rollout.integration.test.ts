import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PoolClient } from 'pg';
import { createDelivery, createEnvelope } from '@relaybus/testing';
import type { ClaimedDelivery, EventEnvelope } from '@relaybus/core';
// The published 0.4.0 package: its own setup statements, marker and append statements.
import { PostgresEventStore as Version040Store } from 'relaybus-postgres-0.4.0';
import { PostgresEventStore, resolvePostgresNames } from '../src/index.js';
import { version040SchemaMarker } from '../src/schema.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

/** The store API both versions share and the test uses. */
type Store = Pick<PostgresEventStore, 'setup' | 'transaction' | 'append' | 'claim'>;

function current(schema: string): Store {
  return new PostgresEventStore({ pool: database.pool, schema });
}

function version040(schema: string): Store {
  return new Version040Store({ pool: database.pool, schema });
}

async function publish(store: Store, event: EventEnvelope, handler: string): Promise<void> {
  await store.transaction((transaction) =>
    store.append(transaction, [{ event, deliveries: [createDelivery(handler)] }], new Date()),
  );
}

async function claim(store: Store, handler: string): Promise<ClaimedDelivery[]> {
  const now = new Date();
  return store.claim({
    handlers: [handler],
    limit: 10,
    leaseOwner: handler,
    leaseExpiresAt: new Date(now.getTime() + 60_000),
    now,
  });
}

async function payloadTypes(schema: string): Promise<string[]> {
  const { rows } = await database.pool.query<{ type: string }>(
    `SELECT data_type AS type FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = 'relaybus_events' AND column_name IN ('payload', 'headers')
     ORDER BY column_name`,
    [schema],
  );
  return rows.map((row) => row.type);
}

async function marker(schema: string): Promise<string | null> {
  const { rows } = await database.pool.query<{ marker: string | null }>(
    `SELECT obj_description('"${schema}".relaybus_lanes'::regclass, 'pg_class') AS marker`,
  );
  return rows[0]!.marker;
}

async function someoneWaitsForALock(): Promise<boolean> {
  const { rows } = await database.pool.query<{ waiting: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'
     ) AS waiting`,
  );
  return rows[0]!.waiting;
}

/**
 * Runs `setup` while another transaction holds a row exclusive lock on every table, as running
 * publishes, claims and completions do. Any schema statement would wait for it. Resolves whether the
 * setup had to wait.
 */
async function setupWaitsForWriters(schema: string, setup: () => Promise<void>): Promise<boolean> {
  const names = resolvePostgresNames({ schema });
  const writer: PoolClient = await database.pool.connect();
  try {
    await writer.query('BEGIN');
    await writer.query(
      `LOCK TABLE ${names.events}, ${names.keySequences}, ${names.deliveries}, ${names.lanes}, ${names.eventCompletions} IN ROW EXCLUSIVE MODE`,
    );
    const running = setup();
    const waited = await Promise.race([
      running.then(() => false),
      (async () => {
        for (let attempt = 0; attempt < 250; attempt += 1) {
          if (await someoneWaitsForALock()) {
            return true;
          }
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        throw new Error('setup neither finished nor waited for a lock');
      })(),
    ]);
    await writer.query('COMMIT');
    await running;
    return waited;
  } finally {
    writer.release();
  }
}

/** Publishes with `from` and claims with `to`: the payload and headers arrive intact. */
async function handOver(from: Store, to: Store, handler: string): Promise<void> {
  const event = createEnvelope({ payload: { zulu: 1, a: { yy: 2, b: 3 } }, headers: { z: 'z' } });
  await publish(from, event, handler);
  const [delivery] = await claim(to, handler);
  expect(delivery!.event).toEqual(event);
}

describe('0.4.0 and 0.4.1 instances on one database', () => {
  it('0.4.1 creates a schema 0.4.0 recognizes as current, and both publish and claim on it', async () => {
    const schema = uniqueSchema();
    await current(schema).setup();
    expect(await payloadTypes(schema)).toEqual(['json', 'json']);
    expect(await marker(schema)).toBe(version040SchemaMarker({ schema }));

    // A 0.4.0 instance with setupStore: true starting next to running writers (a mixed rollout or a
    // rollback) finds its marker and runs no schema statement.
    expect(await setupWaitsForWriters(schema, () => version040(schema).setup())).toBe(false);
    expect(await setupWaitsForWriters(schema, () => current(schema).setup())).toBe(false);

    // 0.4.0 writes jsonb parameters into the json columns (assignment cast).
    await handOver(version040(schema), current(schema), 'old-to-new');
    await handOver(current(schema), version040(schema), 'new-to-old');
    await handOver(version040(schema), version040(schema), 'old-to-old');
    expect(await payloadTypes(schema)).toEqual(['json', 'json']);

    // The check above detects schema statements: without its marker 0.4.0 runs them and waits.
    await database.pool.query(`COMMENT ON TABLE "${schema}".relaybus_lanes IS NULL`);
    expect(await setupWaitsForWriters(schema, () => version040(schema).setup())).toBe(true);
    expect(await marker(schema)).toBe(version040SchemaMarker({ schema }));
    expect(await payloadTypes(schema)).toEqual(['json', 'json']);
  });

  it('0.4.1 converts a schema 0.4.0 created, and 0.4.0 instances keep running on it', async () => {
    const schema = uniqueSchema();
    await version040(schema).setup();
    expect(await payloadTypes(schema)).toEqual(['jsonb', 'jsonb']);
    await handOver(version040(schema), current(schema), 'before-upgrade');

    await current(schema).setup();
    expect(await payloadTypes(schema)).toEqual(['json', 'json']);
    expect(await marker(schema)).toBe(version040SchemaMarker({ schema }));

    expect(await setupWaitsForWriters(schema, () => version040(schema).setup())).toBe(false);
    expect(await setupWaitsForWriters(schema, () => current(schema).setup())).toBe(false);
    await handOver(version040(schema), current(schema), 'old-to-new');
    await handOver(current(schema), version040(schema), 'new-to-old');
  });
});
