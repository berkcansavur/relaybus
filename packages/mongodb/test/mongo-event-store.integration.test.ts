import { Collection, type ClientSession, type Document, type MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  EventValidationError,
  LeaseLostError,
  RelayBus,
  defineEvent,
  toErrorInfo,
  type ClaimedDelivery,
  type RelayBusLogger,
} from '@relaybus/core';
import { FakeClock, at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import {
  startReplicaSet,
  startStandalone,
  uniqueDatabaseName,
  type MongoReplicaSet,
} from './support.js';

const silentLogger: RelayBusLogger = { log: () => {}, warn: () => {}, error: () => {} };

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

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

afterEach(() => {
  vi.restoreAllMocks();
});

async function createStore(
  options: Partial<ConstructorParameters<typeof MongoEventStore>[0]> = {},
): Promise<{ store: MongoEventStore; database: string }> {
  const database = uniqueDatabaseName();
  const store = new MongoEventStore({ client, database, ...options });
  await store.setup();
  return { store, database };
}

describe('setup', () => {
  it('creates the indexes once and tolerates repeated and concurrent runs', async () => {
    const { store, database } = await createStore();
    await Promise.all([store.setup(), store.setup()]);
    const names = (await client.db(database).collection('relaybus_deliveries').indexes()).map(
      (index) => index.name,
    );
    expect(names.sort()).toEqual(
      [
        '_id_',
        'claim',
        'completed',
        'due',
        'event_handler',
        'handler_created',
        'ordering',
        'parked',
        'status_created',
        'status_updated',
        'type_created',
      ].sort(),
    );
  });

  it('honours the collection prefix', async () => {
    const { store, database } = await createStore({ collectionPrefix: 'outbox_' });
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const names = (await client.db(database).listCollections().toArray()).map(
      (entry) => entry.name,
    );
    expect(names).toEqual(expect.arrayContaining(['outbox_events', 'outbox_deliveries']));
    expect(names.some((name) => name.startsWith('relaybus_'))).toBe(false);
  });

  it('uses the default database of the client when none is given', async () => {
    const defaultClient = await replicaSet.connect();
    try {
      const store = new MongoEventStore({ client: defaultClient, collectionPrefix: 'default_db_' });
      await store.setup();
      expect(await store.statistics(at(0))).toMatchObject({ pending: 0 });
    } finally {
      await defaultClient.close();
    }
  });

  it('never claims for a non-positive limit', async () => {
    const { store } = await createStore();
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const request = { handlers: ['h'], leaseOwner: 'w', leaseExpiresAt: at(1_000), now: at(0) };
    expect(await store.claim({ ...request, limit: 0 })).toEqual([]);
  });
});

describe('unsupported deployments', () => {
  it('explains that a replica set is needed, from setup and from transactions', async () => {
    const standalone = await startStandalone();
    const standaloneClient = await standalone.connect();
    try {
      const store = new MongoEventStore({ client: standaloneClient, database: 'relaybus_test' });
      await expect(store.setup()).rejects.toThrow(/replica set/);
      await expect(store.setup()).rejects.toBeInstanceOf(ConfigurationError);
      const failure = await store
        .transaction((session) =>
          store.append(
            session,
            [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
            at(0),
          ),
        )
        .catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(ConfigurationError);
      expect((failure as ConfigurationError).message).toMatch(/replica set/);
      expect((failure as ConfigurationError).cause).toBeDefined();
    } finally {
      await standaloneClient.close();
      await standalone.stop();
    }
  });
});

describe('transactions', () => {
  it('rethrows errors of the work unchanged and writes nothing', async () => {
    const { store, database } = await createStore();
    const failure = new Error('boom');
    await expect(
      store.transaction(async (session) => {
        await client.db(database).collection('business').insertOne({ value: 1 }, { session });
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(await client.db(database).collection('business').countDocuments()).toBe(0);
  });

  it('restarts the work after a write conflict and keeps only the last attempt', async () => {
    const { store } = await createStore();
    const first = createEnvelope({ key: 'k' });
    const second = createEnvelope({ key: 'k' });
    let attempts = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let opened!: () => void;
    const hasWritten = new Promise<void>((resolve) => {
      opened = resolve;
    });
    const holder = store.transaction(async (session) => {
      await store.append(session, [{ event: first, deliveries: [createDelivery('h')] }], at(0));
      opened();
      await gate;
    });
    await hasWritten;
    const contender = store.transaction(async (session) => {
      attempts += 1;
      return store.append(session, [{ event: second, deliveries: [createDelivery('h')] }], at(0));
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    release();
    await holder;
    const result = await contender;
    expect(attempts).toBeGreaterThanOrEqual(2);
    expect(result.appendedEventIds).toEqual([second.id]);
    const records = await store.listDeliveries({ limit: 10 });
    expect(records.map((record) => record.event.id)).toEqual([first.id, second.id]);
  });

  it('accepts custom transaction options', async () => {
    const { store } = await createStore({
      transactionOptions: { readConcern: { level: 'majority' }, writeConcern: { w: 1 } },
    });
    await expect(store.transaction(async () => 'ok')).resolves.toBe('ok');
  });
});

describe('stored data', () => {
  it('stores the payload as JSON would, so Dates become strings like in other stores', async () => {
    const { store, database } = await createStore();
    const when = new Date('2026-05-06T07:08:09.010Z');
    const event = createEnvelope({
      payload: { when, missing: undefined, nested: [{ at: when }], 'dotted.key': 1, $dollar: 2 },
    });
    await store.transaction((session) =>
      store.append(session, [{ event, deliveries: [createDelivery('h')] }], at(0)),
    );
    const [claimed] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    expect(claimed!.event.payload).toEqual({
      when: when.toISOString(),
      nested: [{ at: when.toISOString() }],
      'dotted.key': 1,
      $dollar: 2,
    });
    const stored = await client
      .db(database)
      .collection('relaybus_events')
      .findOne({ _id: event.id as never });
    expect(stored?.['occurredAt']).toBeInstanceOf(Date);
  });

  it('stores an event that has no deliveries and ignores an append of nothing', async () => {
    const { store, database } = await createStore();
    const event = createEnvelope();
    const result = await store.transaction(async (session) => {
      await store.append(session, [], at(0));
      return store.append(session, [{ event, deliveries: [] }], at(0));
    });
    expect(result).toEqual({
      appendedEventIds: [event.id],
      duplicateEventIds: [],
      appendedDeliveryCount: 0,
    });
    expect(await client.db(database).collection('relaybus_events').countDocuments()).toBe(1);
  });

  it('keeps an undefined payload absent', async () => {
    const { store } = await createStore();
    const event = createEnvelope({ payload: undefined });
    await store.transaction((session) =>
      store.append(session, [{ event, deliveries: [createDelivery('h')] }], at(0)),
    );
    const [record] = await store.listDeliveries({});
    expect(record!.event.payload).toBeUndefined();
  });

  it('leaves null fields and the unchanged update time out of new documents and reads them back', async () => {
    const { store, database } = await createStore();
    const event = createEnvelope();
    await store.transaction((session) =>
      store.append(session, [{ event, deliveries: [createDelivery('h')] }], at(0)),
    );
    const db = client.db(database);
    const storedEvent = await db.collection('relaybus_events').findOne({ _id: event.id as never });
    for (const field of ['key', 'sequence', 'correlationId', 'causationId', 'revision']) {
      expect(storedEvent).not.toHaveProperty(field);
    }
    const storedDelivery = await db.collection('relaybus_deliveries').findOne({});
    expect(storedDelivery).not.toHaveProperty('updatedAt');
    expect(storedDelivery).toMatchObject({ attempts: 0 });
    const [record] = await store.listDeliveries({});
    expect(record).toMatchObject({ updatedAt: at(0), createdAt: at(0), attempts: 0 });
    expect(record!.event).toMatchObject({ key: null, correlationId: null, causationId: null });
    // An event that gets another handler later counts its revision up from nothing.
    await store.transaction((session) =>
      store.append(session, [{ event, deliveries: [createDelivery('other')] }], at(1)),
    );
    expect(
      await db.collection('relaybus_events').findOne({ _id: event.id as never }),
    ).toMatchObject({
      revision: 1,
    });
  });

  it('records an error without a stack as such', async () => {
    const { store } = await createStore();
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const [delivery] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    await store.reschedule(delivery!, at(5), { name: 'Error', message: 'no stack' }, at(1));
    const [record] = await store.listDeliveries({});
    expect(record!.lastError).toStrictEqual({ name: 'Error', message: 'no stack' });
  });

  it('keeps the error code of a dead delivery', async () => {
    const { store } = await createStore();
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const [delivery] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    const error = toErrorInfo(new LeaseLostError('gone'));
    await store.bury(delivery!, error, at(1));
    const [record] = await store.listDeliveries({ status: 'dead' });
    expect(record!.lastError).toStrictEqual({
      name: 'LeaseLostError',
      message: 'gone',
      stack: error.stack,
      code: 'RELAYBUS_LEASE_LOST',
    });
  });
});

describe('purge', () => {
  it('works through more than one batch', async () => {
    const { store } = await createStore({ purgeBatchSize: 3, completionHistory: true });
    const entries = Array.from({ length: 8 }, () => ({
      event: createEnvelope(),
      deliveries: [createDelivery('h')],
    }));
    await store.transaction((session) => store.append(session, entries, at(0)));
    const claimed = await store.claim({
      handlers: ['h'],
      limit: 8,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    for (const delivery of claimed) {
      await store.complete(delivery, at(1));
    }
    expect(await store.purge({ completedBefore: at(2) })).toEqual({
      deletedDeliveries: 8,
      deletedEvents: 8,
    });
  });

  it('deletes deliveries and their events in one transaction', async () => {
    const { store } = await createStore({ completionHistory: true });
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const [delivery] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    await store.complete(delivery!, at(1));

    const original = Collection.prototype.deleteMany;
    vi.spyOn(Collection.prototype, 'deleteMany').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['deleteMany']>
    ) {
      if (this.collectionName.endsWith('_events')) {
        throw new Error('crash before the events are deleted');
      }
      return original.apply(this, args);
    } as never);
    await expect(store.purge({ completedBefore: at(10) })).rejects.toThrow(/crash before/);
    vi.restoreAllMocks();

    expect(await store.listDeliveries({})).toHaveLength(1);
    expect(await store.purge({ completedBefore: at(10) })).toEqual({
      deletedDeliveries: 1,
      deletedEvents: 1,
    });
  });

  it('keeps an event whose deliveries are extended while it is being purged', async () => {
    const { store } = await createStore();
    const event = createEnvelope();
    await store.transaction((session) =>
      store.append(session, [{ event, deliveries: [createDelivery('a')] }], at(0)),
    );
    const [delivery] = await store.claim({
      handlers: ['a'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    await store.complete(delivery!, at(1));

    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let written!: () => void;
    const hasWritten = new Promise<void>((resolve) => {
      written = resolve;
    });
    const appending = store.transaction(async (session) => {
      await store.append(
        session,
        [{ event, deliveries: [createDelivery('a'), createDelivery('b')] }],
        at(2),
      );
      written();
      await gate;
    });
    await hasWritten;
    const purging = store.purge({ completedBefore: at(10) });
    await new Promise((resolve) => setTimeout(resolve, 150));
    release();
    await appending;
    const result = await purging;
    expect(result.deletedEvents).toBe(0);
    const records = await store.listDeliveries({});
    expect(records.map((record) => record.handler)).toEqual(['b']);
    expect(records[0]!.event.id).toBe(event.id);
  });
});

describe('sessions without a transaction', () => {
  it('refuses to append through a session that is not in a transaction', async () => {
    const { store, database } = await createStore();
    const session = client.startSession();
    try {
      const failure = await store
        .append(session, [{ event: createEnvelope(), deliveries: [createDelivery('h')] }], at(0))
        .catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(ConfigurationError);
      expect((failure as ConfigurationError).message).toMatch(/inside a started transaction/);
      expect(await client.db(database).collection('relaybus_events').countDocuments()).toBe(0);

      session.startTransaction();
      await store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      );
      await session.commitTransaction();
      expect(await client.db(database).collection('relaybus_events').countDocuments()).toBe(1);
    } finally {
      await session.endSession();
    }
  });

  it('refuses to complete through a session that is not in a transaction', async () => {
    const { store } = await createStore();
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const [delivery] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    const session = client.startSession();
    try {
      await expect(store.complete(delivery!, at(1), session)).rejects.toBeInstanceOf(
        ConfigurationError,
      );
      expect((await store.listDeliveries({}))[0]).toMatchObject({ status: 'processing' });
    } finally {
      await session.endSession();
    }
  });
});

describe('fenced updates', () => {
  it('confirms a renewal that repeats the stored values and still rejects a stale lease', async () => {
    const { store } = await createStore();
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const [delivery] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    expect(await store.extendLease(delivery!, at(5_000), at(10))).toBe(true);
    expect(await store.extendLease(delivery!, at(5_000), at(10))).toBe(true);
    expect(await store.extendLease({ ...delivery!, leaseToken: 'stale' }, at(5_000), at(10))).toBe(
      false,
    );
  });
});

describe('text that BSON cannot store', () => {
  const lone = '\ud800';

  async function appendPayload(
    store: MongoEventStore,
    overrides: Parameters<typeof createEnvelope>[0],
  ): Promise<unknown> {
    return store
      .transaction((session) =>
        store.append(
          session,
          [{ event: createEnvelope(overrides), deliveries: [createDelivery('h')] }],
          at(0),
        ),
      )
      .catch((error: unknown) => error);
  }

  it.each([
    ['a payload value', { payload: { text: `a${lone}b` } }, 'payload'],
    ['a payload key', { payload: { [`k${lone}`]: 1 } }, 'payload'],
    ['a nested payload value', { payload: { list: [{ text: lone }] } }, 'payload'],
    ['a header value', { headers: { name: `x${lone}` } }, 'headers'],
  ])('rejects %s containing a lone surrogate', async (_name, overrides, field) => {
    const { store, database } = await createStore();
    const failure = await appendPayload(store, overrides);
    expect(failure).toBeInstanceOf(EventValidationError);
    expect((failure as EventValidationError).issues[0]!.path).toEqual([field]);
    expect(await client.db(database).collection('relaybus_events').countDocuments()).toBe(0);
  });

  it('stores well-formed pairs and text that merely looks like an escape', async () => {
    const { store } = await createStore();
    const payload = {
      emoji: 'launch \u{1F680}',
      escaped: '\\ud800',
      doubleEscaped: '\\\\ud800',
      other: '\u0000 ',
    };
    const outcome = await appendPayload(store, { payload });
    expect(outcome).toMatchObject({ appendedEventIds: [expect.any(String)] });
    const [claimed] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    expect(claimed!.event.payload).toEqual(payload);
  });
});

describe('queries built from untrusted input', () => {
  it('treats operator objects as plain strings in delivery filters and requeue ids', async () => {
    const { store } = await createStore();
    await store.transaction((session) =>
      store.append(
        session,
        [{ event: createEnvelope(), deliveries: [createDelivery('h')] }],
        at(0),
      ),
    );
    const [delivery] = await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    await store.bury(delivery!, { name: 'Error', message: 'x' }, at(1));
    const operator = { $ne: 'nothing' } as never;

    expect(await store.listDeliveries({ status: operator })).toEqual([]);
    expect(await store.listDeliveries({ handler: operator })).toEqual([]);
    expect(await store.listDeliveries({ eventType: operator })).toEqual([]);
    expect(await store.listDeliveries({ eventId: operator })).toEqual([]);
    expect(await store.requeue([operator], at(2))).toBe(0);
    expect(await store.listDeliveries({ status: 'dead', handler: 'h' })).toHaveLength(1);
  });
});

describe('claim under contention', () => {
  it('never runs two deliveries of one handler and key at once, never claims one twice, keeps publish order', async () => {
    const { store } = await createStore({ completionHistory: true });
    const handlers = ['handler-a', 'handler-b'];
    const keys = Array.from({ length: 8 }, (_, index) => `key-${index}`);
    const eventsPerKey = 20;
    const publishOrder = new Map<string, string[]>();
    const total = keys.length * eventsPerKey * handlers.length + 30 * handlers.length;

    const publishers = keys.map(async (key) => {
      for (let index = 0; index < eventsPerKey; index += 1) {
        const event = createEnvelope({ key, payload: { index } });
        await store.transaction((session) =>
          store.append(
            session,
            [{ event, deliveries: handlers.map((handler) => createDelivery(handler)) }],
            at(0),
          ),
        );
        for (const handler of handlers) {
          const slot = `${handler}/${key}`;
          publishOrder.set(slot, [...(publishOrder.get(slot) ?? []), event.id]);
        }
      }
    });
    const unkeyed = (async () => {
      for (let index = 0; index < 30; index += 1) {
        await store.transaction((session) =>
          store.append(
            session,
            [
              {
                event: createEnvelope(),
                deliveries: handlers.map((handler) => createDelivery(handler)),
              },
            ],
            at(0),
          ),
        );
      }
    })();

    const inFlight = new Set<string>();
    const completionOrder = new Map<string, string[]>();
    const completedIds = new Set<string>();
    const violations: string[] = [];
    let finished = 0;
    let retries = 0;

    async function handle(delivery: ClaimedDelivery): Promise<void> {
      const slot =
        delivery.event.key === null ? undefined : `${delivery.handler}/${delivery.event.key}`;
      if (slot !== undefined) {
        if (inFlight.has(slot)) {
          violations.push(`two in flight for ${slot}`);
        }
        inFlight.add(slot);
      }
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 4));
      const shouldRetry = retries < 40 && Math.random() < 0.1;
      if (slot !== undefined) {
        inFlight.delete(slot);
      }
      if (shouldRetry) {
        retries += 1;
        expect(
          await store.reschedule(delivery, at(0), { name: 'Error', message: 'retry' }, at(0)),
        ).toBe(true);
        return;
      }
      expect(await store.complete(delivery, at(0))).toBe(true);
      if (completedIds.has(delivery.deliveryId)) {
        violations.push(`completed twice ${delivery.deliveryId}`);
      }
      completedIds.add(delivery.deliveryId);
      if (slot !== undefined) {
        completionOrder.set(slot, [...(completionOrder.get(slot) ?? []), delivery.event.id]);
      }
      finished += 1;
    }

    async function worker(index: number): Promise<void> {
      const deadline = Date.now() + 90_000;
      while (finished < total && Date.now() < deadline) {
        const claimed = await store.claim({
          handlers,
          limit: 5,
          leaseOwner: `worker-${index}`,
          leaseExpiresAt: at(10 ** 9),
          now: at(0),
        });
        await Promise.all(claimed.map(handle));
        if (claimed.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      }
    }

    await Promise.all(
      [publishers, unkeyed, ...Array.from({ length: 10 }, (_, i) => worker(i))].flat(),
    );
    expect(violations).toEqual([]);
    expect(finished).toBe(total);
    for (const [slot, expected] of publishOrder) {
      expect(completionOrder.get(slot)).toEqual(expected);
    }
    expect((await store.statistics(at(0))).completed).toBe(total);
  });
});

describe('RelayBus on MongoDB', () => {
  async function createBus(
    clock: FakeClock,
    database: string,
    completionHistory = false,
  ): Promise<{ bus: RelayBus<ClientSession>; store: MongoEventStore }> {
    const store = new MongoEventStore({ client, database, completionHistory });
    const bus = new RelayBus<ClientSession>({
      store,
      clock,
      logger: silentLogger,
      random: () => 0.5,
      retry: { attempts: 3, initialDelay: 1_000, jitter: 'none' },
    });
    await store.setup();
    return { bus, store };
  }

  it('publishes with a business write and runs the handler to completion', async () => {
    const database = uniqueDatabaseName();
    const { bus } = await createBus(new FakeClock(), database, true);
    const received: string[] = [];
    bus.subscribe(OrderPlaced, (event) => void received.push(event.payload.orderId), {
      id: 'audit',
    });
    await bus.transaction(async (session) => {
      await client.db(database).collection('orders').insertOne({ orderId: '1' }, { session });
      await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'order-1' });
    });
    await bus.drain();
    expect(received).toEqual(['1']);
    expect(await client.db(database).collection('orders').countDocuments()).toBe(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'completed', attempts: 1 });
  });

  it('stores neither the business write nor the event when the transaction fails', async () => {
    const database = uniqueDatabaseName();
    const { bus } = await createBus(new FakeClock(), database);
    bus.subscribe(OrderPlaced, () => {}, { id: 'audit' });
    await expect(
      bus.transaction(async (session) => {
        await client.db(database).collection('orders').insertOne({ orderId: '2' }, { session });
        await bus.publish(new OrderPlaced({ orderId: '2' }));
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    expect(await client.db(database).collection('orders').countDocuments()).toBe(0);
    expect(await bus.listDeliveries()).toEqual([]);
  });

  it('commits a transactional handler write together with the completion and rolls both back on failure', async () => {
    const database = uniqueDatabaseName();
    const clock = new FakeClock();
    const { bus } = await createBus(clock, database, true);
    const projections = client.db(database).collection<Document>('projections');
    let failNext = true;
    bus.subscribe(
      OrderPlaced,
      async (event, context) => {
        await projections.insertOne(
          { orderId: event.payload.orderId },
          { session: context.transaction! },
        );
        if (failNext) {
          failNext = false;
          throw new Error('after the write');
        }
      },
      { id: 'projector', transactional: true },
    );
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '3' })));
    await bus.runOnce();
    expect(await projections.countDocuments()).toBe(0);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'pending', attempts: 1 });
    clock.advance(1_000);
    await bus.runOnce();
    expect(await projections.countDocuments()).toBe(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'completed', attempts: 2 });
  });

  it('reclaims a delivery whose worker disappeared once the lease expired', async () => {
    const database = uniqueDatabaseName();
    const clock = new FakeClock();
    const { bus, store } = await createBus(clock, database);
    const attempts: number[] = [];
    bus.subscribe(OrderPlaced, (_event, context) => void attempts.push(context.attempt), {
      id: 'audit',
    });
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '4' })));
    const [lost] = await store.claim({
      handlers: ['audit'],
      limit: 1,
      leaseOwner: 'crashed-worker',
      leaseExpiresAt: new Date(clock.now().getTime() + 30_000),
      now: clock.now(),
    });
    await bus.runOnce();
    expect(attempts).toEqual([]);
    clock.advance(30_000);
    await bus.runOnce();
    expect(attempts).toEqual([2]);
    expect(await store.complete(lost!, clock.now())).toBe(false);
  });
});
