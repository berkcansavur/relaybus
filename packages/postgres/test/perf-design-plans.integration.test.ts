import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { at, createDelivery } from '@relaybus/testing';
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
  nextId,
  drain,
  explain,
  expectPlanWithinBudget,
  type Fixture,
} from './support/perf-design.js';
import { LEASE_MS } from './support/claim.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startPerfDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { closePools, createFixture, analyze } = createPerfHelpers(() => database);

afterAll(closePools);

/** The tests of this file used to run after 15398 ids had been handed out by the ones before them. */
skipIdentifiers(15398);

describe('plan shape and buffers', () => {
  /** A store with `pending` deliveries over 1,000 keys and every lane, analyzed. */
  async function seeded(pending: number): Promise<Fixture> {
    const fixture = await createFixture();
    await publishMany(
      fixture.store,
      events(pending, (index) => `key-${index % 1_000}`),
    );
    await analyze(fixture);
    return fixture;
  }

  it('claims, completes and renews through indexes, with buffers that do not grow with the backlog', async () => {
    const small = await seeded(5_000);
    const large = await seeded(50_000);
    const claimBuffers: number[] = [];
    for (const fixture of [small, large]) {
      // Warm: the worker already holds the lanes it claims from.
      const held = await claim(fixture.store, 50);
      await fixture.store.completeMany(held, at(1));
      const warm = await explain(fixture, () => claim(fixture.store, 50));
      claimBuffers.push(warm.buffers);
      if (fixture === large) {
        expectPlanWithinBudget('claimBacklogWarm', warm);
      }
    }
    // What ten times the backlog adds to a claim: a claim must not pay for the depth of the queue.
    expectWithinBudget(
      'postgres.explain.claimBacklogWarm.extraBuffers50kVs5k',
      Math.max(0, claimBuffers[1]! - claimBuffers[0]!),
    );

    const claimed = await claim(large.store, 50);
    expect(claimed).toHaveLength(50);
    const completion = await explain(large, () => large.store.completeMany(claimed, at(1)));
    expectPlanWithinBudget('complete50', completion);

    const [running] = await claim(large.store, 1);
    const renewal = await explain(large, () => large.store.extendLease(running!, at(LEASE_MS * 2)));
    expectPlanWithinBudget('extendLease', renewal);

    const publication = await explain(large, () =>
      publish(
        large.store,
        events(1, () => 'k'),
      ),
    );
    expectPlanWithinBudget('publishKeyedSingle', publication);
  });

  it('claims cheaply on an idle store and on one whose due work is all in flight', async () => {
    const idle = await createFixture();
    // Idle, but not empty: retries and delayed events wait for later.
    await publishMany(idle.store, events(5_000), ['h']);
    await idle.store.transaction((transaction) =>
      idle.store.append(
        transaction,
        events(5_000).map((event) => ({
          event,
          deliveries: [{ ...createDelivery('h', at(3_600_000)), id: nextId('delivery') }],
        })),
        at(0),
      ),
    );
    await drain(idle.store, 500);
    // What autovacuum does after such a drain: the dead index entries of the completed deliveries go.
    await database.pool.query(`VACUUM "${idle.schema}".relaybus_deliveries`);
    await analyze(idle);
    const nothing = await explain(idle, () => claim(idle.store, 10));
    expectPlanWithinBudget('claimIdle', nothing);

    const busy = await createFixture();
    await publishMany(
      busy.store,
      events(1_000, (index) => `hot-${index % 10}`),
    );
    await analyze(busy);
    expect(await claim(busy.store, 10)).toHaveLength(10);
    const blocked = await explain(busy, () => claim(busy.store, 10));
    expectPlanWithinBudget('claimHotKeysAllInFlight', blocked);
  });
});

describe('generic plans of prepared statements', () => {
  // A prepared statement switches to a generic plan (one plan for any parameters) after five runs when
  // it is not estimated to cost more than the plans for the parameters. Even forced, those plans must
  // keep to the indexes and stay far below the JIT threshold.
  it('claims, completes, renews and publishes through indexes with generic plans', async () => {
    const fixture = await createFixture();
    await publishMany(
      fixture.store,
      events(50_000, (index) => (index % 2 === 0 ? `key-${index % 1_000}` : null)),
    );
    await analyze(fixture);
    const held = await claim(fixture.store, 50);
    await fixture.store.completeMany(held, at(1));
    const plans = [
      await explain(fixture, () => claim(fixture.store, 50), true),
      await explain(fixture, () => claim(fixture.store, 50, ['h'], 'worker-2'), true),
    ];
    const claimed = await claim(fixture.store, 50);
    expect(claimed).toHaveLength(50);
    plans.push(await explain(fixture, () => fixture.store.completeMany(claimed, at(1)), true));
    const [running] = await claim(fixture.store, 1);
    plans.push(
      await explain(fixture, () => fixture.store.extendLease(running!, at(LEASE_MS * 2)), true),
      await explain(fixture, () => publish(fixture.store, events(1)), true),
      await explain(
        fixture,
        () =>
          publish(
            fixture.store,
            events(1, () => 'k'),
          ),
        true,
      ),
    );
    const nodes = plans.flatMap((plan) => plan.nodes);
    expectWithinBudget(
      'postgres.explain.generic.largeTableSequentialScans',
      nodes.filter(
        (node) =>
          node['Node Type'] === 'Seq Scan' && /deliveries|events/.test(node['Relation Name'] ?? ''),
      ).length,
    );
    expectWithinBudget(
      'postgres.explain.generic.maximumEstimatedCost',
      Math.max(...plans.map((plan) => plan.maximumCost)),
    );
  });
});

describe('planner cost estimates', () => {
  // PostgreSQL compiles a statement with JIT when its estimated cost passes jit_above_cost (100,000 by
  // default). For these short statements compiling took about 100 ms, while running them takes 1-3 ms.
  const EIGHT_HANDLERS = ['h', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8'];

  it('keeps every claim statement far below the JIT threshold, unkeyed or keyed, whatever the backlog', async () => {
    // Unkeyed only: the key column is all null in the statistics, which is what misled the estimates.
    const unkeyed = await createFixture();
    await publishMany(unkeyed.store, events(50_000));
    await analyze(unkeyed);
    const cold = await explain(unkeyed, () => claim(unkeyed.store, 10, EIGHT_HANDLERS));
    const warm = await explain(unkeyed, () => claim(unkeyed.store, 10, EIGHT_HANDLERS));

    // Hot keys whose heads are all in flight: the claim looks behind the blocked followers.
    const keyed = await createFixture();
    await publishMany(
      keyed.store,
      events(20_000, (index) => `hot-${index % 10}`),
    );
    await analyze(keyed);
    expect(await claim(keyed.store, 10, EIGHT_HANDLERS)).toHaveLength(10);
    const blocked = await explain(keyed, () => claim(keyed.store, 10, EIGHT_HANDLERS));
    const other = await explain(keyed, () => claim(keyed.store, 10, EIGHT_HANDLERS, 'worker-2'));

    expectWithinBudget(
      'postgres.explain.claim.maximumEstimatedCost',
      Math.max(cold.maximumCost, warm.maximumCost, blocked.maximumCost, other.maximumCost),
    );
  });
});
