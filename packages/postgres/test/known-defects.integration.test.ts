import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { PostgresEventStore } from '../src/index.js';
import { claimWithLease } from './support/claim.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

// These tests assert the correct behaviour of open runtime defects. They are written as `it.fails`
// so the suite stays green while a defect exists and turns red once it is fixed: when a fix lands,
// change `it.fails` to `it`.

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

function publish(store: PostgresEventStore, event: ReturnType<typeof createEnvelope>) {
  return store.transaction((transaction) =>
    store.append(transaction, [{ event, deliveries: [createDelivery('h')] }], at(0)),
  );
}

const claim = (store: PostgresEventStore, owner = 'w', now = at(0)) =>
  claimWithLease(store, { handlers: ['h'], limit: 10, owner, now });

describe('transactions', () => {
  // pg-executor.ts (transaction): a statement error caught inside the work aborts the transaction,
  // COMMIT then answers ROLLBACK, and the call still resolves although nothing was committed.
  it.fails('rejects a transaction whose COMMIT was answered with ROLLBACK', async () => {
    const { store, schema } = await newStore();
    let outcome: 'resolved' | 'rejected' = 'resolved';
    try {
      await store.transaction(async (transaction) => {
        await store.append(
          transaction,
          [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
          at(0),
        );
        await transaction.query('SELECT 1/0').catch(() => undefined);
      });
    } catch {
      outcome = 'rejected';
    }
    const { rows } = await database.pool.query(
      `SELECT count(*)::int AS n FROM "${schema}".relaybus_events`,
    );
    expect({ outcome, events: rows[0].n }).toEqual({ outcome: 'rejected', events: 0 });
  });
});

describe('text the json and jsonb columns cannot hold', () => {
  // postgres-append.ts: the payload is sent as JSON text, and PostgreSQL rejects the escape \u0000.
  it.fails('round-trips a NUL inside the payload', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope({ payload: { s: 'a\u0000b' } }));
    const [claimed] = await claim(store);
    expect(claimed!.event.payload).toEqual({ s: 'a\u0000b' });
  });

  // postgres-append.ts: a lone surrogate is serialized as an escape PostgreSQL rejects.
  it.fails('round-trips a lone surrogate inside the payload', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope({ payload: { lone: 'x\ud800y' } }));
    const [claimed] = await claim(store);
    expect(claimed!.event.payload).toEqual({ lone: 'x\ud800y' });
  });

  // postgres-completion.ts: last_error is jsonb, so a handler that echoes untrusted input with a
  // NUL into its error message cannot be rescheduled.
  it.fails('reschedules a delivery whose handler error message contains a NUL', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope());
    const [claimed] = await claim(store);
    const rescheduled = await store.reschedule(
      claimed!,
      at(1000),
      { name: 'Error', message: 'bad input: a\u0000b' },
      at(1),
    );
    expect(rescheduled).toBe(true);
  });

  // postgres-completion.ts: same as above for a lone surrogate when the delivery is buried.
  it.fails('buries a delivery whose handler error message contains a lone surrogate', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope());
    const [claimed] = await claim(store);
    const buried = await store.bury(claimed!, { name: 'Error', message: 'x\ud800y' }, at(1));
    expect(buried).toBe(true);
  });
});

describe('retry times', () => {
  // postgres-completion.ts: available_at is sent as an ISO string, and PostgreSQL cannot read the
  // year 10000 form that an overflowing exponential backoff produces.
  it.fails('accepts a retry time past the year 9999', async () => {
    const { store } = await newStore();
    await publish(store, createEnvelope());
    const [claimed] = await claim(store);
    const farFuture = new Date(Date.UTC(10000, 0, 1));
    expect(await store.reschedule(claimed!, farFuture, { name: 'E', message: 'm' }, at(1))).toBe(
      true,
    );
    const [record] = await store.listDeliveries({ status: 'pending' });
    expect(record!.availableAt.getTime()).toBe(farFuture.getTime());
  });
});

describe('lease owners', () => {
  // statements-lanes.ts (claim_floor): a second process that uses the same lease owner is handed
  // the delivery the first process is still running.
  it.fails(
    'does not hand a delivery that is still running to a second process using the same lease owner',
    async () => {
      const schema = uniqueSchema();
      const first = new PostgresEventStore({ pool: database.pool, schema });
      await first.setup();
      await publish(first, createEnvelope({ key: 'k' }));
      const [running] = await claim(first, 'same-owner', at(0));
      await new Promise((resolve) => setTimeout(resolve, 5));
      const second = new PostgresEventStore({ pool: database.pool, schema });
      const taken = await claim(second, 'same-owner', at(1));
      expect(taken.map((delivery) => delivery.deliveryId)).not.toContain(running!.deliveryId);
    },
  );
});
