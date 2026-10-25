import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, defineEvent, type ClaimedDelivery } from '@relaybus/core';
import { at } from '@relaybus/testing';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import { PostgresEventStore } from '../src/index.js';
import type { TestDatabase } from './support/postgres-container.js';
import {
  createPerfHelpers,
  startPerfDatabase,
  claim,
  publish,
  publishMany,
  publishWith,
  events,
  difference,
} from './support/perf-design.js';

class Probe extends defineEvent<{ index: number }>('perf.probe.v1') {}

const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };

let database: TestDatabase;

beforeAll(async () => {
  database = await startPerfDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { closePools, createFixture, tupleCounters } = createPerfHelpers(() => database);

afterAll(closePools);

describe('round trips per claim', () => {
  it('claims nothing in one statement on an idle store, without writing a lane', async () => {
    const fixture = await createFixture();
    const before = await tupleCounters(fixture);
    expectWithinBudget(
      'postgres.claim.idleCold.statements',
      await fixture.executor.count(() => claim(fixture.store, 10)),
    );
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget('postgres.claim.idle.laneWrites', written.lanes.updated);
  });

  it('claims nothing in one statement while the worker still holds an idle lane', async () => {
    const fixture = await createFixture();
    await publish(fixture.store, events(1));
    const claimed = await claim(fixture.store, 10);
    await fixture.store.completeMany(claimed, at(1));
    expectWithinBudget(
      'postgres.claim.idleWarm.statements',
      await fixture.executor.count(() => claim(fixture.store, 10)),
    );
  });

  it('claims a backlog in one statement, cold and warm', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(1_000));
    let first: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.backlogCold.statements',
      await fixture.executor.count(async () => {
        first = await claim(fixture.store, 50);
      }),
    );
    expect(first).toHaveLength(50);
    let second: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.backlogWarm.statements',
      await fixture.executor.count(async () => {
        second = await claim(fixture.store, 50);
      }),
    );
    expect(second).toHaveLength(50);
  });

  it('claims the next deliveries of few hot keys in one statement', async () => {
    const fixture = await createFixture();
    const keys = 10;
    await publishMany(
      fixture.store,
      events(keys * 100, (index) => `hot-${index % keys}`),
    );
    const heads = await claim(fixture.store, keys);
    expect(heads).toHaveLength(keys);
    await fixture.store.completeMany(heads, at(1));
    let successors: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.hotKeysWarm.statements',
      await fixture.executor.count(async () => {
        successors = await claim(fixture.store, keys);
      }),
    );
    expect(successors).toHaveLength(keys);
    // Every key is in flight now: nothing is claimable, which must stay cheap too.
    expectWithinBudget(
      'postgres.claim.hotKeysAllInFlight.statements',
      await fixture.executor.count(async () => {
        expect(await claim(fixture.store, keys)).toHaveLength(0);
      }),
    );
  });

  it('claims many distinct keys in one statement', async () => {
    const fixture = await createFixture();
    await publishMany(
      fixture.store,
      events(1_000, (index) => `key-${index}`),
    );
    let claimed: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.distinctKeysCold.statements',
      await fixture.executor.count(async () => {
        claimed = await claim(fixture.store, 200);
      }),
    );
    expect(claimed).toHaveLength(200);
  });
});

describe('round trips per publish and completion', () => {
  it('publishes fresh events with one statement per call, whatever the batch size', async () => {
    const fixture = await createFixture();
    // A publish within 10 ms of the one before leaves the wake-up to one trailing notification per
    // interval, a round trip of its own; waiting the interval out keeps it out of the next count.
    const count = async (operation: () => Promise<unknown>): Promise<number> => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return fixture.executor.count(operation);
    };
    expectWithinBudget(
      'postgres.publish.unkeyedSingle.statements',
      await count(() => publish(fixture.store, events(1))),
    );
    expectWithinBudget(
      'postgres.publish.keyedSingle.statements',
      await count(() =>
        publish(
          fixture.store,
          events(1, () => 'k'),
        ),
      ),
    );
    expectWithinBudget(
      'postgres.publish.keyedBatch100.statements',
      await count(() =>
        publish(
          fixture.store,
          events(100, (index) => `k${index % 7}`),
        ),
      ),
    );
    const [event] = events(1);
    await publish(fixture.store, [event!], ['h']);
    expectWithinBudget(
      'postgres.publish.repeatedEventNewHandler.statements',
      await count(() => publish(fixture.store, [event!], ['h', 'other'])),
    );
  });

  it('publishes in a transaction held to a time budget with no more statements or round trips', async () => {
    const fixture = await createFixture();
    // bus.transaction passes its transactionTimeout; the store must not pay for it with a round trip.
    const budgeted = { timeoutMs: 5_000 };
    const count = async (
      key: string | null,
    ): Promise<{ readonly statements: number; readonly roundTrips: number }> => {
      // Past the interval of the previous publish, so no trailing wake-up lands in the count.
      await new Promise((resolve) => setTimeout(resolve, 20));
      return fixture.executor.countBoth(() =>
        publishWith(fixture.store, {
          events: events(1, () => key),
          options: budgeted,
        }),
      );
    };
    const unkeyed = await count(null);
    expectWithinBudget('postgres.publish.unkeyedSingle.statements', unkeyed.statements);
    expectWithinBudget('postgres.publish.unkeyedSingle.statements', unkeyed.roundTrips);
    const keyed = await count('k');
    expectWithinBudget('postgres.publish.keyedSingle.statements', keyed.statements);
    expectWithinBudget('postgres.publish.keyedSingle.statements', keyed.roundTrips);
  });

  it('keeps each bus.transaction publish within the budgeted round trips on a saturated pool', async () => {
    const fixture = await createFixture();
    // The pool has one connection and 20 transactions queue for it, as the publishers of a loaded
    // service do: every round trip a transaction adds holds the connection longer, so the pool sheds
    // load at a lower rate. Polling (no notifications) keeps wake-ups out of the count.
    const store = new PostgresEventStore({
      executor: fixture.executor,
      schema: fixture.schema,
      wakeUp: 'poll',
    });
    const bus = new RelayBus({ store, dispatcher: { enabled: false }, logger: quiet });
    bus.subscribe(Probe, () => undefined, { id: 'h' });
    const transactions = 20;
    const measured = await fixture.executor.countBoth(() =>
      Promise.all(
        Array.from({ length: transactions }, (_, index) =>
          bus.transaction((transaction) =>
            bus.publish(new Probe({ index }), { transaction, key: `k${index % 4}` }),
          ),
        ),
      ),
    );
    expectWithinBudget(
      'postgres.publish.keyedSingle.statements',
      measured.statements / transactions,
    );
    expectWithinBudget(
      'postgres.publish.keyedSingle.statements',
      measured.roundTrips / transactions,
    );
  });

  it('parses and plans the statements of a publish and of a dispatch once per connection', async () => {
    const fixture = await createFixture();
    // The first publishes and dispatches on the connection prepare their statements: a first claim
    // takes free lanes, the next ones claim from the lanes the worker holds.
    await publish(fixture.store, events(30));
    for (let round = 0; round < 2; round += 1) {
      await fixture.store.completeMany(await claim(fixture.store, 10), at(1));
    }
    expectWithinBudget(
      'postgres.publish.unkeyedSingle.parsedStatements',
      await fixture.executor.countParsed(() => publish(fixture.store, events(1))),
    );
    expectWithinBudget(
      'postgres.publish.keyedSingle.parsedStatements',
      await fixture.executor.countParsed(() =>
        publish(
          fixture.store,
          events(1, () => 'k'),
        ),
      ),
    );
    await publish(fixture.store, events(10));
    // The trailing wake-up of that publish is sent within 10 ms; it is not part of the dispatch.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expectWithinBudget(
      'postgres.dispatch.parsedStatementsPerBatch',
      await fixture.executor.countParsed(async () => {
        const claimed = await claim(fixture.store, 10);
        expect(claimed.length).toBeGreaterThan(0);
        await fixture.store.completeMany(claimed, at(1));
      }),
    );
  });

  it('completes one lease or fifty with one statement', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(51));
    const claimed = await claim(fixture.store, 51);
    expectWithinBudget(
      'postgres.complete.single.statements',
      await fixture.executor.count(() => fixture.store.completeMany(claimed.slice(0, 1), at(1))),
    );
    expectWithinBudget(
      'postgres.complete.batch50.statements',
      await fixture.executor.count(() => fixture.store.completeMany(claimed.slice(1), at(1))),
    );
  });
});
