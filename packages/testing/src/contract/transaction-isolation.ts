import type { EventStore } from '@relaybus/core';
import { beforeEach, describe, expect, it } from 'vitest';
import { createDelivery, createEnvelope } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/**
 * Isolation levels of `transaction`: optional on the port, so a store that lists none only has to
 * recognize no error as a serialization failure. A store that lists levels must run a transaction
 * at each, committing and rolling back as it does without one.
 */
export function transactionIsolationContract<TTransaction>(
  suite: ContractSuite<TTransaction>,
): void {
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
  });

  describe('transaction isolation levels', () => {
    it('recognizes no unrelated error as a serialization failure', () => {
      for (const error of [new Error('boom'), undefined, null, 'text', { code: '23505' }]) {
        expect(store.isSerializationFailure?.(error) ?? false).toBe(false);
      }
    });

    it('commits and rolls back at every level it lists', async () => {
      for (const isolationLevel of store.isolationLevels ?? []) {
        const committed = createEnvelope();
        await store.transaction(
          (transaction) =>
            store.append(
              transaction,
              [{ event: committed, deliveries: [createDelivery('h')] }],
              new Date(0),
            ),
          { timeoutMs: 5_000, isolationLevel },
        );
        const abandoned = createEnvelope();
        await expect(
          store.transaction(
            async (transaction) => {
              await store.append(
                transaction,
                [{ event: abandoned, deliveries: [createDelivery('h')] }],
                new Date(0),
              );
              throw new Error(`rolled back at ${isolationLevel}`);
            },
            { timeoutMs: 5_000, isolationLevel },
          ),
        ).rejects.toThrow(`rolled back at ${isolationLevel}`);
        const stored = (await store.listDeliveries({})).map((delivery) => delivery.event.id);
        expect(stored).toContain(committed.id);
        expect(stored).not.toContain(abandoned.id);
      }
    });
  });
}
