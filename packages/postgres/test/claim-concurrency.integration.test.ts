import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createUuidV7 } from '@relaybus/core';
import { PostgresEventStore } from '../src/index.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

const KEY_COUNT = 30;
const EVENTS_PER_KEY = 12;
const UNKEYED_COUNT = 40;
const CLAIMER_COUNT = 12;

describe('claim under contention', () => {
  it('never has two deliveries of one (handler, key) in flight and keeps publish order', async () => {
    const schema = uniqueSchema();
    const stores = Array.from(
      { length: 3 },
      () =>
        new PostgresEventStore({
          connectionString: database.connectionString,
          schema,
          poolConfig: { max: 6 },
        }),
    );
    await stores[0]!.setup();
    const publisher = stores[0]!;
    const now = () => new Date();

    // Publish every key concurrently from several transactions, interleaving keys.
    const expectedTotal = KEY_COUNT * EVENTS_PER_KEY + UNKEYED_COUNT;
    await Promise.all(
      Array.from({ length: KEY_COUNT }, async (_, keyIndex) => {
        for (let position = 0; position < EVENTS_PER_KEY; position += 1) {
          await publisher.transaction((transaction) =>
            publisher.append(
              transaction,
              [
                {
                  event: {
                    id: createUuidV7(),
                    type: 'stress.v1',
                    source: 'stress',
                    key: `key-${keyIndex}`,
                    payload: { position },
                    headers: {},
                    correlationId: null,
                    causationId: null,
                    occurredAt: now(),
                  },
                  deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: now() }],
                },
              ],
              now(),
            ),
          );
        }
      }),
    );
    await publisher.transaction((transaction) =>
      publisher.append(
        transaction,
        Array.from({ length: UNKEYED_COUNT }, () => ({
          event: {
            id: createUuidV7(),
            type: 'stress.v1',
            source: 'stress',
            key: null,
            payload: { position: 0 },
            headers: {},
            correlationId: null,
            causationId: null,
            occurredAt: now(),
          },
          deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: now() }],
        })),
        now(),
      ),
    );

    const inFlightKeys = new Set<string>();
    const nextPosition = new Map<string, number>();
    const violations: string[] = [];
    let completed = 0;
    let retried = 0;

    async function claimer(index: number): Promise<void> {
      const store = stores[index % stores.length]!;
      const deadline = Date.now() + 90_000;
      while (completed < expectedTotal && Date.now() < deadline) {
        const claimedAt = now();
        const batch = await store.claim({
          handlers: ['h'],
          limit: 1 + (index % 5),
          leaseOwner: `claimer-${index}`,
          leaseExpiresAt: new Date(claimedAt.getTime() + 60_000),
          now: claimedAt,
        });
        if (batch.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, 2));
          continue;
        }
        for (const delivery of batch) {
          const key = delivery.event.key;
          const position = (delivery.event.payload as { position: number }).position;
          if (key !== null) {
            if (inFlightKeys.has(key)) {
              violations.push(`two in flight for ${key}`);
            }
            inFlightKeys.add(key);
            if (position !== (nextPosition.get(key) ?? 0)) {
              violations.push(
                `${key}: expected ${nextPosition.get(key) ?? 0}, claimed ${position}`,
              );
            }
          }
          await new Promise((resolve) => setTimeout(resolve, Math.random() * 3));
          // Leave the in-flight set before the database state changes, so a legitimate
          // next claim can never be mistaken for an overlap.
          if (key !== null) {
            inFlightKeys.delete(key);
          }
          if (delivery.attempt === 1 && Math.random() < 0.1) {
            retried += 1;
            await store.reschedule(delivery, now(), { name: 'Error', message: 'retry' }, now());
          } else {
            if (key !== null) {
              nextPosition.set(key, position + 1);
            }
            expect(await store.complete(delivery, now())).toBe(true);
            completed += 1;
          }
        }
      }
    }

    await Promise.all(Array.from({ length: CLAIMER_COUNT }, (_, index) => claimer(index)));

    expect(violations).toEqual([]);
    expect(completed).toBe(expectedTotal);
    expect(retried).toBeGreaterThan(0);
    expect(await stores[0]!.statistics(now())).toMatchObject({
      pending: 0,
      processing: 0,
      completed: expectedTotal,
    });
    await Promise.all(stores.map((store) => store.close()));
  });
});
