import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { at } from '@relaybus/testing';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import type { TestDatabase } from './support/postgres-container.js';
import {
  skipIdentifiers,
  createPerfHelpers,
  startPerfDatabase,
  claim,
  publish,
  publishMany,
  events,
  difference,
  drain,
} from './support/perf-design.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startPerfDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { closePools, createFixture, tupleCounters, walPosition, walBytesSince } = createPerfHelpers(
  () => database,
);

afterAll(closePools);

/** The tests of this file used to run after 6396 ids had been handed out by the ones before them. */
skipIdentifiers(6396);

describe('tuples written per delivery', () => {
  it('inserts and deletes each delivery once and updates lanes only in place', async () => {
    const fixture = await createFixture();
    const total = 500;
    await publishMany(fixture.store, events(total));
    const before = await tupleCounters(fixture);
    expect(await drain(fixture.store, 50)).toBe(total);
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget(
      'postgres.tuples.deliveries.updatesPerDelivery',
      written.deliveries.updated / total,
    );
    expectWithinBudget(
      'postgres.tuples.deliveries.deletesPerDelivery',
      written.deliveries.deleted / total,
    );
    expectWithinBudget('postgres.tuples.events.updatesPerEvent', written.events.updated / total);
    expectWithinBudget(
      'postgres.tuples.lanes.nonHotUpdatesPercent',
      ((written.lanes.updated - written.lanes.hotUpdated) / written.lanes.updated) * 100,
    );
    expectWithinBudget('postgres.tuples.lanes.updatesPerDelivery', written.lanes.updated / total);
    expectWithinBudget(
      'postgres.tuples.eventCompletions.updatesPerEvent.oneHandler',
      written.event_completions.updated / total,
    );
  });

  it('stamps an event with five handlers without rewriting its completion row per handler', async () => {
    const fixture = await createFixture();
    const total = 200;
    const handlers = ['a', 'b', 'c', 'd', 'e'];
    await publishMany(fixture.store, events(total), handlers);
    const before = await tupleCounters(fixture);
    // Completions arrive in small groups and a little later each, as a dispatcher with a short
    // completion window writes them.
    let completedAt = 1;
    for (;;) {
      const claimed = await claim(fixture.store, 50, handlers);
      if (claimed.length === 0) {
        break;
      }
      for (let offset = 0; offset < claimed.length; offset += 3) {
        completedAt += 1;
        await fixture.store.completeMany(claimed.slice(offset, offset + 3), at(completedAt));
      }
    }
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget(
      'postgres.tuples.eventCompletions.updatesPerEvent.fiveHandlers',
      written.event_completions.updated / total,
    );
  });

  it('updates the counters of keys in place', async () => {
    const fixture = await createFixture();
    const before = await tupleCounters(fixture);
    for (let index = 0; index < 200; index += 1) {
      await publish(
        fixture.store,
        events(1, () => `k${index % 20}`),
      );
    }
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget(
      'postgres.tuples.keySequences.nonHotUpdatesPercent',
      ((written.key_sequences.updated - written.key_sequences.hotUpdated) /
        written.key_sequences.updated) *
        100,
    );
  });
});

describe('WAL bytes', () => {
  it('writes little WAL per delivery when dispatching', async () => {
    for (const batch of [10, 200]) {
      const fixture = await createFixture();
      const total = 1_000;
      await publishMany(fixture.store, events(total));
      const start = await walPosition();
      expect(await drain(fixture.store, batch)).toBe(total);
      expectWithinBudget(
        `postgres.wal.dispatchBatch${batch}.bytesPerDelivery`,
        Math.round((await walBytesSince(start)) / total),
      );
    }
  });

  it('writes little WAL per event published alone in its transaction', async () => {
    const fixture = await createFixture();
    await publish(fixture.store, events(1));
    const total = 200;
    const start = await walPosition();
    for (const event of events(total)) {
      // More than the notification interval apart: each publish notifies in its own commit, and no
      // trailing notification (a commit record of its own, at a time that depends on the machine) lands.
      await new Promise((resolve) => setTimeout(resolve, 11));
      await publish(fixture.store, [event]);
    }
    expectWithinBudget(
      'postgres.wal.publishSingle.bytesPerEvent',
      Math.round((await walBytesSince(start)) / total),
    );
  });

  it('writes little WAL per published event', async () => {
    const fixture = await createFixture();
    const total = 1_000;
    const all = events(total);
    const start = await walPosition();
    for (let offset = 0; offset < total; offset += 10) {
      await publish(fixture.store, all.slice(offset, offset + 10));
    }
    expectWithinBudget(
      'postgres.wal.publish.bytesPerEvent',
      Math.round((await walBytesSince(start)) / total),
    );
  });
});

describe('schema', () => {
  it('keeps the number of indexes every delivery pays for small', async () => {
    const fixture = await createFixture();
    const { rows } = await database.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM pg_index
       WHERE indrelid = (quote_ident($1) || '.relaybus_deliveries')::regclass`,
      [fixture.schema],
    );
    expectWithinBudget('postgres.schema.deliveryIndexes', rows[0]!.count);
  });
});
