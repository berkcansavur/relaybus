import { Collection } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { EventValidationError, LeaseLostError, toErrorInfo } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { type MongoEventStore } from '../src/index.js';
import { client, useReplicaSet, createStore } from './store-support.js';

useReplicaSet();

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
