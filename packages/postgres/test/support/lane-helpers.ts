import type { ClaimedDelivery, EventEnvelope } from '@relaybus/core';
import { at, createDelivery } from '@relaybus/testing';
import { PostgresEventStore } from '../../src/index.js';
import { claimWithLease } from './claim.js';
import { uniqueSchema, type TestDatabase } from './postgres-container.js';

export const error = { name: 'Error', message: 'x' };

export interface LaneHelpers {
  createStore(options?: {
    completionHistory?: boolean;
    schema?: string;
    purgeBatchSize?: number;
  }): Promise<{ store: PostgresEventStore; schema: string }>;
  append(
    store: PostgresEventStore,
    entries: Array<{ event: EventEnvelope; handlers: string[] }>,
    now?: Date,
  ): Promise<void>;
  claim(
    store: PostgresEventStore,
    handlers: string[],
    owner: string,
    now?: Date,
    limit?: number,
  ): Promise<ClaimedDelivery[]>;
}

/** Store, append and claim helpers of the lane-lease tests; `getDatabase` is read when a helper runs. */
export function createLaneHelpers(getDatabase: () => TestDatabase): LaneHelpers {
  return {
    async createStore(options = {}) {
      const schema = options.schema ?? uniqueSchema();
      const store = new PostgresEventStore({ pool: getDatabase().pool, ...options, schema });
      await store.setup();
      return { store, schema };
    },
    async append(store, entries, now = at(0)) {
      await store.transaction((transaction) =>
        store.append(
          transaction,
          entries.map(({ event, handlers }) => ({
            event,
            deliveries: handlers.map((handler) => createDelivery(handler)),
          })),
          now,
        ),
      );
    },
    claim(store, handlers, owner, now = at(0), limit = 100) {
      return claimWithLease(store, { handlers, limit, owner, now });
    },
  };
}
