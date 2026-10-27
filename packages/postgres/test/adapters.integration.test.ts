import { CompiledQuery, Kysely, PostgresDialect, type Transaction } from 'kysely';
import { drizzle } from 'drizzle-orm/node-postgres';
import { sql as drizzleSql } from 'drizzle-orm';
import knexFactory, { type Knex } from 'knex';
import type { Pool } from 'pg';
import { DataSource, type EntityManager } from 'typeorm';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from './prisma/generated/client.js';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  RelayBus,
  defineEvent,
  type EventStore,
  type TransactionIsolationLevel,
} from '@relaybus/core';
import { eventStoreContract } from '@relaybus/testing';
import { PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import { createDrizzleExecutor } from '../src/drizzle/index.js';
import { createKnexExecutor } from '../src/knex/index.js';
import { createKyselyExecutor } from '../src/kysely/index.js';
import { createPrismaExecutor } from '../src/prisma/index.js';
import { createTypeOrmExecutor } from '../src/typeorm/index.js';
import {
  dropSchema,
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

class LimitReachedError extends Error {}

/** Everything the shared tests need from one ORM, on one schema. */
interface Harness<TTransaction> {
  readonly executor: PostgresExecutor<TTransaction>;
  /** A `pg` pool for the statements the store sends outside a transaction, when the store gets one. */
  readonly pool?: Pool;
  /** The ORM's own transaction API, as application code uses it. */
  begin<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T>;
  /** A business write through the ORM transaction. */
  insertOrder(transaction: TTransaction, orderId: string): Promise<void>;
  destroy(): Promise<void>;
}

interface Adapter<TTransaction> {
  readonly name: string;
  create(connectionString: string, schema: string): Promise<Harness<TTransaction>>;
}

const orderInsertSql = (schema: string): string =>
  `INSERT INTO "${schema}".orders (id) VALUES ($1)`;

const kyselyAdapter: Adapter<Transaction<unknown>> = {
  name: 'Kysely',
  async create(connectionString, schema) {
    const db = new Kysely<unknown>({
      dialect: new PostgresDialect({ pool: createTestPool(connectionString, 10) }),
    });
    return {
      executor: createKyselyExecutor(db) as unknown as PostgresExecutor<Transaction<unknown>>,
      begin: (work) => db.transaction().execute(work),
      async insertOrder(transaction, orderId) {
        await transaction.executeQuery(CompiledQuery.raw(orderInsertSql(schema), [orderId]));
      },
      destroy: () => db.destroy(),
    };
  },
};

const knexAdapter: Adapter<Knex.Transaction> = {
  name: 'Knex',
  async create(connectionString, schema) {
    const knex = knexFactory({ client: 'pg', connection: connectionString, pool: { max: 10 } });
    return {
      executor: createKnexExecutor(knex) as unknown as PostgresExecutor<Knex.Transaction>,
      begin: (work) => knex.transaction(work),
      async insertOrder(transaction, orderId) {
        await transaction.raw(orderInsertSql(schema).replace('$1', '?'), [orderId]);
      },
      destroy: () => knex.destroy(),
    };
  },
};

const typeOrmAdapter: Adapter<EntityManager> = {
  name: 'TypeORM',
  async create(connectionString, schema) {
    const dataSource = await new DataSource({
      type: 'postgres',
      url: connectionString,
      extra: { max: 10 },
    }).initialize();
    return {
      executor: createTypeOrmExecutor(dataSource) as unknown as PostgresExecutor<EntityManager>,
      begin: (work) => dataSource.transaction(work),
      async insertOrder(transaction, orderId) {
        await transaction.query(orderInsertSql(schema), [orderId]);
      },
      destroy: () => dataSource.destroy(),
    };
  },
};

type DrizzleDatabase = ReturnType<typeof drizzle>;
type DrizzleTransaction = Parameters<Parameters<DrizzleDatabase['transaction']>[0]>[0];

const drizzleAdapter: Adapter<DrizzleTransaction> = {
  name: 'Drizzle',
  async create(connectionString, schema) {
    const pool = createTestPool(connectionString, 10);
    const db = drizzle(pool);
    return {
      executor: createDrizzleExecutor(db) as unknown as PostgresExecutor<DrizzleTransaction>,
      begin: (work) => db.transaction(work),
      async insertOrder(transaction, orderId) {
        await transaction.execute(
          drizzleSql`INSERT INTO ${drizzleSql.raw(`"${schema}"`)}.orders (id) VALUES (${orderId})`,
        );
      },
      destroy: () => pool.end(),
    };
  },
};

type PrismaTransaction = Parameters<Parameters<PrismaClient['$transaction']>[0]>[0];

/** A real Prisma Client (generated from test/prisma/schema.prisma) on the `pg` driver adapter. */
describe('TypeORM subscribers and isolation levels', () => {
  it('sets the level before a subscriber that queries right after the transaction begins', async () => {
    const connectionString = database.connectionString;
    const schema = uniqueSchema();
    const seen: string[] = [];
    const dataSource = await new DataSource({
      type: 'postgres',
      url: connectionString,
      extra: { max: 5 },
    }).initialize();
    dataSource.subscribers.push({
      async afterTransactionStart(event) {
        await event.queryRunner.query('SELECT 1');
        seen.push('subscriber');
      },
    });
    try {
      const store = new PostgresEventStore<EntityManager>({
        executor: createTypeOrmExecutor(dataSource) as unknown as PostgresExecutor<EntityManager>,
        schema,
      });
      await store.setup();
      seen.length = 0;
      const level = await store.transaction(
        async (manager) => {
          const rows = await manager.query<Array<{ level: string }>>(
            "SELECT current_setting('transaction_isolation') AS level",
          );
          return rows[0]!.level;
        },
        { timeoutMs: 5_000, isolationLevel: 'serializable' },
      );
      expect(level).toBe('serializable');
      expect(seen).toEqual(['subscriber']);
    } finally {
      await dataSource.destroy();
      await dropSchema(database.pool, schema);
    }
  });
});

const prismaAdapter = (withPool: boolean): Adapter<PrismaTransaction> => ({
  name: withPool ? 'Prisma with pool' : 'Prisma',
  async create(connectionString, schema) {
    const pool = createTestPool(connectionString, 10);
    const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
    return {
      ...(withPool ? { pool } : {}),
      executor: createPrismaExecutor(prisma, {
        timeout: 20_000,
      }) as unknown as PostgresExecutor<PrismaTransaction>,
      begin: (work) => prisma.$transaction(work),
      async insertOrder(transaction, orderId) {
        await transaction.$executeRawUnsafe(orderInsertSql(schema), orderId);
      },
      async destroy() {
        await prisma.$disconnect();
        await pool.end();
      },
    };
  },
});

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

async function countOrders(schema: string): Promise<number> {
  const result = await database.pool.query<{ count: string }>(
    `SELECT count(*) FROM "${schema}".orders`,
  );
  return Number(result.rows[0]!.count);
}

function defineAdapterTests<TTransaction>(adapter: Adapter<TTransaction>): void {
  describe(`${adapter.name} adapter`, () => {
    const harnesses = new Map<
      EventStore<TTransaction>,
      { schema: string; harness: Harness<TTransaction> }
    >();

    async function createStore(completionHistory = false): Promise<{
      store: PostgresEventStore<TTransaction>;
      schema: string;
      harness: Harness<TTransaction>;
    }> {
      const schema = uniqueSchema();
      const harness = await adapter.create(database.connectionString, schema);
      const store = new PostgresEventStore<TTransaction>({
        executor: harness.executor,
        ...(harness.pool === undefined ? {} : { pool: harness.pool }),
        schema,
        completionHistory,
      });
      await store.setup();
      await database.pool.query(`CREATE TABLE "${schema}".orders (id text PRIMARY KEY)`);
      harnesses.set(store, { schema, harness });
      return { store, schema, harness };
    }

    async function destroy(store: EventStore<TTransaction>): Promise<void> {
      const entry = harnesses.get(store)!;
      harnesses.delete(store);
      await entry.harness.destroy();
      await dropSchema(database.pool, entry.schema);
    }

    eventStoreContract<TTransaction>(`PostgresEventStore through ${adapter.name}`, {
      createStore: async () => (await createStore()).store,
      destroyStore: destroy,
    });

    describe('with an isolation level', () => {
      async function createBus() {
        const created = await createStore();
        const bus = new RelayBus<TTransaction>({
          store: created.store,
          setupStore: false,
          dispatcher: { enabled: false },
        });
        bus.subscribe(OrderPlaced, () => undefined, { id: 'listener' });
        return { ...created, bus };
      }

      const levelOf = (
        harness: Harness<TTransaction>,
        transaction: TTransaction,
      ): Promise<unknown> =>
        harness.executor
          .query("SELECT current_setting('transaction_isolation') AS level", [], transaction)
          .then((rows) => rows[0]!['level']);

      it.each(['read committed', 'repeatable read', 'serializable'] as const)(
        'opens the transaction of the ORM at %s',
        async (isolationLevel) => {
          const { bus, harness, store } = await createBus();

          const level = await bus.transaction((transaction) => levelOf(harness, transaction), {
            isolationLevel,
          });
          const outside = await bus.transaction((transaction) => levelOf(harness, transaction));

          expect(level).toBe(isolationLevel);
          expect(outside).toBe('read committed');
          await destroy(store);
        },
      );

      it('stops the write skew of two concurrent reservations by rerunning the loser', async () => {
        const { bus, harness, schema, store } = await createBus();
        let arrived = 0;
        let release!: () => void;
        const open = new Promise<void>((resolve) => (release = resolve));
        const attempts: string[] = [];
        const reserve = (id: string, isolationLevel?: TransactionIsolationLevel): Promise<void> =>
          bus.transaction(
            async (transaction) => {
              attempts.push(id);
              const rows = await harness.executor.query(
                `SELECT count(*)::int AS n FROM "${schema}".orders`,
                [],
                transaction,
              );
              arrived += 1;
              if (arrived >= 2) {
                release();
              }
              await open;
              if ((rows[0]!['n'] as number) >= 1) {
                throw new LimitReachedError(`limit reached for ${id}`);
              }
              await harness.insertOrder(transaction, id);
              await bus.publish(new OrderPlaced({ orderId: id }));
            },
            isolationLevel === undefined ? undefined : { isolationLevel },
          );

        const outcomes = await Promise.allSettled([
          reserve('a', 'serializable'),
          reserve('b', 'serializable'),
        ]);

        const rejected = outcomes.filter((outcome) => outcome.status === 'rejected');
        expect(rejected).toHaveLength(1);
        expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(LimitReachedError);
        // The loser ran twice: the serialization failure, whichever shape the ORM gave it, then the rerun.
        expect(attempts).toHaveLength(3);
        expect(await countOrders(schema)).toBe(1);
        expect(await store.listDeliveries({})).toHaveLength(1);
        await destroy(store);
      });
    });

    describe('with the application transaction', () => {
      it('publishes into the ORM transaction and commits with the business write', async () => {
        const { store, schema, harness } = await createStore();
        const bus = new RelayBus<TTransaction>({
          store,
          setupStore: false,
          dispatcher: { enabled: false },
        });
        bus.subscribe(OrderPlaced, () => undefined, { id: 'listener' });
        await harness.begin(async (transaction) => {
          await harness.insertOrder(transaction, 'order-1');
          await bus.publish(new OrderPlaced({ orderId: 'order-1' }), { transaction });
        });
        expect(await countOrders(schema)).toBe(1);
        expect(await store.listDeliveries({})).toHaveLength(1);
        await destroy(store);
      });

      it('rolls the event back together with the business write', async () => {
        const { store, schema, harness } = await createStore();
        const bus = new RelayBus<TTransaction>({
          store,
          setupStore: false,
          dispatcher: { enabled: false },
        });
        bus.subscribe(OrderPlaced, () => undefined, { id: 'listener' });
        await expect(
          harness.begin(async (transaction) => {
            await harness.insertOrder(transaction, 'order-1');
            await bus.publish(new OrderPlaced({ orderId: 'order-1' }), { transaction });
            throw new Error('business failure');
          }),
        ).rejects.toThrow('business failure');
        expect(await countOrders(schema)).toBe(0);
        expect(await store.listDeliveries({})).toEqual([]);
        await destroy(store);
      });

      it('runs a transactional handler and completes the delivery in one transaction', async () => {
        const { store, schema, harness } = await createStore(true);
        const bus = new RelayBus<TTransaction>({
          store,
          setupStore: false,
          dispatcher: { enabled: false },
          retry: { attempts: 2 },
        });
        let failNext = true;
        bus.subscribe(
          OrderPlaced,
          async (event, context) => {
            await harness.insertOrder(context.transaction!, event.payload.orderId);
            if (event.payload.orderId === 'order-2' && failNext) {
              failNext = false;
              throw new Error('handler failure after write');
            }
          },
          { id: 'writer', transactional: true },
        );
        await bus.transaction(async (transaction) => {
          await bus.publish(new OrderPlaced({ orderId: 'order-1' }), { transaction });
        });
        await bus.drain();
        expect(await countOrders(schema)).toBe(1);
        expect((await store.listDeliveries({}))[0]!.status).toBe('completed');

        await bus.transaction(async (transaction) => {
          await bus.publish(new OrderPlaced({ orderId: 'order-2' }), { transaction });
        });
        await bus.runOnce();
        // The failed attempt wrote order-2 inside the handler transaction, which rolled back.
        expect(await countOrders(schema)).toBe(1);
        const [retrying] = await store.listDeliveries({ status: 'pending' });
        expect(retrying!.attempts).toBe(1);
        expect(retrying!.lastError!.message).toBe('handler failure after write');
        await destroy(store);
      });
    });
  });
}

defineAdapterTests(kyselyAdapter);
defineAdapterTests(knexAdapter);
defineAdapterTests(typeOrmAdapter);
defineAdapterTests(drizzleAdapter);
defineAdapterTests(prismaAdapter(false));
defineAdapterTests(prismaAdapter(true));

describe('Prisma adapter with a generated client', () => {
  it('publishes next to typed model writes in concurrent interactive transactions', async () => {
    const schema = uniqueSchema();
    const pool = createTestPool(database.connectionString, 10);
    const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
    await database.pool.query('CREATE TABLE IF NOT EXISTS prisma_orders (id text PRIMARY KEY)');
    const store = new PostgresEventStore({ executor: createPrismaExecutor(prisma), schema });
    await store.setup();
    const bus = new RelayBus({ store, setupStore: false, dispatcher: { enabled: false } });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'listener' });
    const orderIds = Array.from({ length: 20 }, (_, index) => `${schema}-${index}`);
    const results = await Promise.allSettled(
      orderIds.map((orderId, index) =>
        prisma.$transaction(async (transaction) => {
          await transaction.order.create({ data: { id: orderId } });
          await bus.publish(new OrderPlaced({ orderId }), { transaction, key: 'one-key' });
          if (index % 4 === 3) {
            throw new Error('business failure');
          }
        }),
      ),
    );
    const committed = orderIds.filter((_, index) => results[index]!.status === 'fulfilled');
    expect(committed).toHaveLength(15);
    const stored = await prisma.order.findMany({ where: { id: { startsWith: `${schema}-` } } });
    expect(stored.map((order) => order.id).sort()).toEqual([...committed].sort());
    const deliveries = await store.listDeliveries({});
    expect(new Set(deliveries.map((delivery) => delivery.event.id)).size).toBe(15);
    // One key: the dispatcher hands the deliveries out one at a time, in commit order.
    expect(await bus.drain()).toBe(15);
    await prisma.$disconnect();
    await pool.end();
    await dropSchema(database.pool, schema);
  });
});
