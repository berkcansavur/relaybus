import { AsyncLocalStorage } from 'node:async_hooks';
import { Collection, type MongoClient } from 'mongodb';
import { afterAll, beforeAll, describe, it, vi } from 'vitest';
import type { ClaimRequest } from '@relaybus/core';
import { runStoreStress } from '../../../test-support/store-stress.js';
import { MongoEventStore } from '../src/index.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

let replicaSet: MongoReplicaSet;
const clients: MongoClient[] = [];

beforeAll(async () => {
  replicaSet = await startReplicaSet();
});

afterAll(async () => {
  vi.restoreAllMocks();
  await Promise.all(clients.map((client) => client.close()));
  await replicaSet.stop();
});

describe('many dispatchers on shared lanes', () => {
  it('never deadlocks, loses, duplicates or reorders a delivery', async () => {
    const database = uniqueDatabaseName();
    const claiming = new AsyncLocalStorage<boolean>();
    const lost = new Error('claim answer lost after the database ran it');
    const admin = await replicaSet.connect();
    clients.push(admin);
    await runStoreStress({
      async createStore() {
        const client = await replicaSet.connect();
        clients.push(client);
        const store = new MongoEventStore({ client, database });
        await store.setup();
        const claim = store.claim.bind(store);
        // Lane writes inside a claim are its record of the batch (see loseClaimAnswers).
        store.claim = (request: ClaimRequest) => claiming.run(true, () => claim(request));
        return store;
      },
      loseClaimAnswers(lose) {
        const original = Collection.prototype.bulkWrite;
        vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
          this: Collection,
          ...args: Parameters<Collection['bulkWrite']>
        ) {
          const result = await original.apply(this, args);
          if (claiming.getStore() === true && this.collectionName.endsWith('_lanes') && lose()) {
            throw lost;
          }
          return result;
        });
        return lost;
      },
      async openTransactions() {
        const status = await admin.db('admin').command({ serverStatus: 1 });
        return (status['transactions'] as { currentOpen: number }).currentOpen;
      },
    });
  });
});
