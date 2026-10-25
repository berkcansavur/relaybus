import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { startStandalone } from './support.js';
import { replicaSet, client, useReplicaSet, createStore } from './store-support.js';

useReplicaSet();

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
