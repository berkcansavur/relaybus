import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, TransactionRolledBackError, defineEvent } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { PostgresEventStore } from '../src/index.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

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

async function eventCount(schema: string): Promise<number> {
  const { rows } = await database.pool.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM "${schema}".relaybus_events`,
  );
  return rows[0]!.n;
}

describe('a transaction whose COMMIT PostgreSQL answers with ROLLBACK (PgExecutor)', () => {
  it('rejects store.transaction and commits nothing', async () => {
    const { store, schema } = await newStore();

    await expect(
      store.transaction(async (transaction) => {
        await store.append(
          transaction,
          [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
          at(0),
        );
        await transaction.query('SELECT 1/0').catch(() => undefined);
      }),
    ).rejects.toBeInstanceOf(TransactionRolledBackError);
    expect(await eventCount(schema)).toBe(0);
    await store.close();
  });

  it.each([undefined, 'serializable'] as const)(
    'rejects bus.transaction (isolation level %s) and reports no committed publish',
    async (isolationLevel) => {
      const { store, schema } = await newStore();
      const committed: string[] = [];
      const bus = new RelayBus({
        store,
        setupStore: false,
        dispatcher: { enabled: false },
        observers: [
          {
            onPublishCommitted: ({ envelopes }) =>
              committed.push(...envelopes.map((envelope) => envelope.id)),
          },
        ],
      });
      bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });

      await expect(
        bus.transaction(
          async (transaction) => {
            await bus.publish(new OrderPlaced({ orderId: 'o' }));
            await transaction.query('SELECT 1/0').catch(() => undefined);
          },
          isolationLevel === undefined ? undefined : { isolationLevel },
        ),
      ).rejects.toBeInstanceOf(TransactionRolledBackError);
      expect(await eventCount(schema)).toBe(0);
      expect(committed).toEqual([]);
      await store.close();
    },
  );

  it('keeps committing a transaction whose work caught nothing', async () => {
    const { store, schema } = await newStore();

    await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );

    expect(await eventCount(schema)).toBe(1);
    await store.close();
  });
});
