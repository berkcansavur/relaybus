import type { PoolClient } from 'pg';
import { afterAll, beforeAll, describe } from 'vitest';
import { eventStoreContract } from '@relaybus/testing';
import { PostgresEventStore } from '../src/index.js';
import {
  POSTGRES_IMAGES,
  dropSchema,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

describe.each(POSTGRES_IMAGES)('PostgresEventStore on %s', (image) => {
  let database: TestDatabase;
  const schemas = new Map<PostgresEventStore, string>();
  const peers = new Set<PostgresEventStore>();

  beforeAll(async () => {
    database = await startDatabase(image);
  });

  afterAll(async () => {
    await Promise.all([...peers].map((peer) => peer.close()));
    await database.stop();
  });

  eventStoreContract<PoolClient>(`PostgresEventStore (${image})`, {
    async createStore() {
      const schema = uniqueSchema();
      const store = new PostgresEventStore({ pool: database.pool, schema });
      await store.setup();
      schemas.set(store, schema);
      return store;
    },
    async destroyStore(store) {
      const schema = schemas.get(store as PostgresEventStore)!;
      if (peers.has(store as PostgresEventStore)) {
        await store.close?.();
      } else {
        await dropSchema(database.pool, schema);
      }
    },
    async createPeerStore(primary) {
      const peer = new PostgresEventStore({
        connectionString: database.connectionString,
        schema: schemas.get(primary as PostgresEventStore)!,
      });
      schemas.set(peer, schemas.get(primary as PostgresEventStore)!);
      peers.add(peer);
      return peer;
    },
  });
});
