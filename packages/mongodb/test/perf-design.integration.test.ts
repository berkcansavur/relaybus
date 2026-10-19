import type { ClientSession, CommandStartedEvent, MongoClient } from 'mongodb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, defineEvent, type ClaimedDelivery, type EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import { MongoEventStore } from '../src/index.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

/**
 * Deterministic performance design tests for MongoDB: commands per operation (command monitoring),
 * documents examined per document returned (`explain`), oplog entries per delivery and the index
 * budget. Counts, not timings, so a budget in perf-budgets.json fails the same way everywhere.
 */

const LEASE_MS = 30_000;

let replicaSet: MongoReplicaSet;
let client: MongoClient;

beforeAll(async () => {
  replicaSet = await startReplicaSet();
  client = await replicaSet.connect();
});

afterAll(async () => {
  await client.close();
  await replicaSet.stop();
});

interface Fixture {
  readonly store: MongoEventStore;
  readonly database: string;
}

async function createFixture(): Promise<Fixture> {
  const database = uniqueDatabaseName();
  const store = new MongoEventStore({ client, database });
  await store.setup();
  return { store, database };
}

/** Commands `work` sends to the fixture's database, plus the commits and aborts of its transactions. */
async function recordCommands(
  fixture: Fixture,
  work: () => Promise<unknown>,
): Promise<CommandStartedEvent[]> {
  const started: CommandStartedEvent[] = [];
  const onStarted = (event: CommandStartedEvent): void => {
    if (
      event.databaseName === fixture.database ||
      event.commandName === 'commitTransaction' ||
      event.commandName === 'abortTransaction'
    ) {
      started.push(event);
    }
  };
  client.on('commandStarted', onStarted);
  try {
    await work();
  } finally {
    client.off('commandStarted', onStarted);
  }
  return started;
}

async function countCommands(fixture: Fixture, work: () => Promise<unknown>): Promise<number> {
  return (await recordCommands(fixture, work)).length;
}

let identifiers = 0;

/**
 * UUID-shaped ids in creation order: as long as the ids relaybus generates, but the same in every run
 * (random ids would change lanes and scan orders, and with them the counts).
 */
function nextId(kind: 'event' | 'delivery'): string {
  identifiers += 1;
  return `00000000-0000-7000-${kind === 'event' ? '8000' : '9000'}-${String(identifiers).padStart(12, '0')}`;
}

function events(count: number, key: (index: number) => string | null = () => null) {
  return Array.from({ length: count }, (_, index) =>
    createEnvelope({ id: nextId('event'), key: key(index) }),
  );
}

async function publish(
  store: MongoEventStore,
  all: readonly EventEnvelope[],
  handlers: readonly string[] = ['h'],
): Promise<void> {
  await store.transaction((session) =>
    store.append(
      session,
      all.map((event) => ({
        event,
        deliveries: handlers.map((handler) => ({
          ...createDelivery(handler),
          id: nextId('delivery'),
        })),
      })),
      at(0),
    ),
  );
}

async function publishMany(store: MongoEventStore, all: readonly EventEnvelope[]): Promise<void> {
  for (let offset = 0; offset < all.length; offset += 500) {
    await publish(store, all.slice(offset, offset + 500));
  }
}

function claim(
  store: MongoEventStore,
  limit: number,
  owner = 'worker-1',
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers: ['h'],
    limit,
    leaseOwner: owner,
    leaseExpiresAt: at(LEASE_MS),
    now: at(0),
  });
}

async function drain(store: MongoEventStore, batch: number): Promise<number> {
  let total = 0;
  for (;;) {
    const claimed = await claim(store, batch);
    if (claimed.length === 0) {
      return total;
    }
    expect(await store.completeMany(claimed, at(1))).toEqual(claimed.map(() => true));
    total += claimed.length;
  }
}

describe('commands per claim', () => {
  it('claims nothing on an idle store with few commands', async () => {
    const fixture = await createFixture();
    expectWithinBudget(
      'mongodb.claim.idleCold.commands',
      await countCommands(fixture, () => claim(fixture.store, 10)),
    );
  });

  it('claims a backlog with a fixed number of commands, cold and warm', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(1_000));
    let first: ClaimedDelivery[] = [];
    expectWithinBudget(
      'mongodb.claim.backlogCold.commands',
      await countCommands(fixture, async () => {
        first = await claim(fixture.store, 50);
      }),
    );
    expect(first).toHaveLength(50);
    await fixture.store.completeMany(first, at(1));
    expectWithinBudget(
      'mongodb.claim.backlogWarm.commands',
      await countCommands(fixture, async () => {
        expect(await claim(fixture.store, 50)).toHaveLength(50);
      }),
    );
  });

  it('claims the next deliveries of few hot keys with few commands', async () => {
    const fixture = await createFixture();
    const keys = 10;
    await publishMany(
      fixture.store,
      events(keys * 100, (index) => `hot-${index % keys}`),
    );
    const heads = await claim(fixture.store, keys);
    expect(heads).toHaveLength(keys);
    await fixture.store.completeMany(heads, at(1));
    expectWithinBudget(
      'mongodb.claim.hotKeysWarm.commands',
      await countCommands(fixture, async () => {
        expect(await claim(fixture.store, keys)).toHaveLength(keys);
      }),
    );
    expectWithinBudget(
      'mongodb.claim.hotKeysAllInFlight.commands',
      await countCommands(fixture, async () => {
        expect(await claim(fixture.store, keys)).toHaveLength(0);
      }),
    );
  });
});

describe('commands per publish and completion', () => {
  it('publishes with a number of commands that does not grow with the batch', async () => {
    const fixture = await createFixture();
    expectWithinBudget(
      'mongodb.publish.single.commands',
      await countCommands(fixture, () => publish(fixture.store, events(1))),
    );
    expectWithinBudget(
      'mongodb.publish.batch100.commands',
      await countCommands(fixture, () => publish(fixture.store, events(100))),
    );
  });

  it('completes one lease or fifty with the same commands', async () => {
    const fixture = await createFixture();
    await publish(fixture.store, events(51));
    const claimed = await claim(fixture.store, 51);
    expectWithinBudget(
      'mongodb.complete.single.commands',
      await countCommands(fixture, () => fixture.store.completeMany(claimed.slice(0, 1), at(1))),
    );
    expectWithinBudget(
      'mongodb.complete.batch50.commands',
      await countCommands(fixture, () => fixture.store.completeMany(claimed.slice(1), at(1))),
    );
  });
});

/**
 * Oplog operations on the fixture's database and the bytes of the oplog entries holding them, written
 * while `work` runs. Operations of a transaction are nested in one applyOps entry: they are counted one by one.
 */
async function oplogWrittenBy(
  fixture: Fixture,
  work: () => Promise<unknown>,
): Promise<{ readonly entries: number; readonly bytes: number }> {
  const oplog = client.db('local').collection('oplog.rs');
  const namespace = new RegExp(`^${fixture.database}\\.`);
  const last = await oplog.find().sort({ $natural: -1 }).limit(1).next();
  await work();
  const [written] = await oplog
    .aggregate<{ entries: number; bytes: number }>([
      { $match: { ts: { $gt: last!['ts'] } } },
      {
        $project: {
          bytes: { $bsonSize: '$$ROOT' },
          operations: {
            $cond: [
              { $eq: ['$o.applyOps', null] },
              { $cond: [{ $regexMatch: { input: '$ns', regex: namespace } }, 1, 0] },
              {
                $size: {
                  $filter: {
                    input: { $ifNull: ['$o.applyOps', []] },
                    cond: { $regexMatch: { input: '$$this.ns', regex: namespace } },
                  },
                },
              },
            ],
          },
        },
      },
      { $match: { operations: { $gt: 0 } } },
      { $group: { _id: null, entries: { $sum: '$operations' }, bytes: { $sum: '$bytes' } } },
    ])
    .toArray();
  return written ?? { entries: 0, bytes: 0 };
}

describe('documents examined, oplog and indexes', () => {
  it('reads claim candidates through an index, examining few documents per document returned', async () => {
    const fixture = await createFixture();
    await publishMany(
      fixture.store,
      events(20_000, (index) => (index % 2 === 0 ? `key-${index % 1_000}` : null)),
    );
    const warmUp = await claim(fixture.store, 50);
    await fixture.store.completeMany(warmUp, at(1));
    const commands = await recordCommands(fixture, () => claim(fixture.store, 50));
    const finds = commands.filter(
      (command) =>
        command.commandName === 'find' && command.command['find'] === 'relaybus_deliveries',
    );
    expect(finds.length).toBeGreaterThan(0);
    let excessExamined = 0;
    let collectionScans = 0;
    for (const { command } of finds) {
      const { find, filter, sort, limit, projection } = command;
      const query = Object.fromEntries(
        Object.entries({ find, filter, sort, limit, projection }).filter(
          ([, value]) => value !== undefined,
        ),
      );
      const explained = await client
        .db(fixture.database)
        .command({ explain: query, verbosity: 'executionStats' });
      const stats = explained['executionStats'] as { totalDocsExamined: number; nReturned: number };
      excessExamined = Math.max(excessExamined, stats.totalDocsExamined - 2 * stats.nReturned);
      collectionScans += JSON.stringify(explained['queryPlanner']).includes('COLLSCAN') ? 1 : 0;
    }
    expectWithinBudget('mongodb.explain.claimCandidates.excessDocsExamined', excessExamined);
    expectWithinBudget('mongodb.explain.claimCandidates.collectionScans', collectionScans);
  });

  it('writes few oplog entries per delivery when dispatching', async () => {
    const fixture = await createFixture();
    const total = 1_000;
    await publishMany(fixture.store, events(total));
    const written = await oplogWrittenBy(fixture, async () => {
      expect(await drain(fixture.store, 50)).toBe(total);
    });
    expectWithinBudget('mongodb.oplog.dispatch.entriesPerDelivery', written.entries / total);
    expectWithinBudget(
      'mongodb.oplog.dispatch.bytesPerDelivery',
      Math.round(written.bytes / total),
    );
  });

  it('writes few oplog entries and bytes per published event', async () => {
    const fixture = await createFixture();
    const total = 1_000;
    const all = events(total, (index) => (index % 2 === 0 ? `key-${index % 50}` : null));
    const written = await oplogWrittenBy(fixture, async () => {
      for (let offset = 0; offset < total; offset += 10) {
        await publish(fixture.store, all.slice(offset, offset + 10));
      }
    });
    expectWithinBudget('mongodb.oplog.publish.entriesPerEvent', written.entries / total);
    expectWithinBudget('mongodb.oplog.publish.bytesPerEvent', Math.round(written.bytes / total));
  });

  it('keeps the number of indexes every delivery pays for small', async () => {
    const fixture = await createFixture();
    const indexes = await client
      .db(fixture.database)
      .collection('relaybus_deliveries')
      .listIndexes()
      .toArray();
    expectWithinBudget('mongodb.schema.deliveryIndexes', indexes.length);
    expectWithinBudget(
      'mongodb.schema.deliveryIndexesWithoutPartialFilter',
      indexes.filter((index) => index['partialFilterExpression'] === undefined).length,
    );
  });
});

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

describe('publish-to-handler latency at default settings', () => {
  const quietLogger = { log: () => undefined, warn: () => undefined, error: () => undefined };

  it('runs a handler soon after another process committed its event', async () => {
    const database = uniqueDatabaseName();
    const consumerClient = await replicaSet.connect();
    const publisher = new RelayBus<ClientSession>({
      store: new MongoEventStore({ client, database }),
      dispatcher: { enabled: false },
      logger: quietLogger,
    });
    const consumer = new RelayBus<ClientSession>({
      store: new MongoEventStore({ client: consumerClient, database }),
      logger: quietLogger,
    });
    const committedAt = new Map<string, number>();
    const startedAt = new Map<string, number>();
    const handler = (event: EventEnvelope<{ orderId: string }>) => {
      startedAt.set(event.payload.orderId, Date.now());
    };
    publisher.subscribe(OrderPlaced, handler, { id: 'h' });
    consumer.subscribe(OrderPlaced, handler, { id: 'h' });
    await publisher.start();
    await consumer.start();
    // Let the consumer fall asleep between two polls.
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    const total = 30;
    try {
      for (let index = 0; index < total; index += 1) {
        const orderId = String(index);
        await publisher.transaction(async (session) => {
          await publisher.publish(new OrderPlaced({ orderId }), { transaction: session });
        });
        committedAt.set(orderId, Date.now());
        const deadline = Date.now() + 5_000;
        while (!startedAt.has(orderId) && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        // Space the events out, so each one finds the consumer idle again.
        await new Promise((resolve) => setTimeout(resolve, 100 + ((index * 37) % 200)));
      }
    } finally {
      await publisher.stop();
      await consumer.stop();
      await consumerClient.close();
    }
    expect(startedAt.size).toBe(total);
    // A handler can start a moment before the publisher saw its commit acknowledged: that is 0 ms.
    const sorted = [...committedAt]
      .map(([orderId, committed]) => Math.max(0, startedAt.get(orderId)! - committed))
      .sort((left, right) => left - right);
    expectWithinBudget('mongodb.latency.defaults.p95Ms', sorted[Math.ceil(total * 0.95) - 1]!);
    expectWithinBudget('mongodb.latency.defaults.maxMs', sorted[total - 1]!);
  });
});
