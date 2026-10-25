import { describe, expect, it } from 'vitest';
import { createUuidV7, type EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { LANE_COUNT, laneOf } from '../src/lanes.js';
import { uniqueDatabaseName } from './support.js';
import {
  LEASE_MS,
  type RawDocument,
  client,
  useReplicaSet,
  claim,
  collections,
} from './store-support.js';

useReplicaSet();

function bound(target: object, member: string | symbol): unknown {
  const value: unknown = Reflect.get(target, member, target);
  return typeof value === 'function'
    ? (value as (...parameters: unknown[]) => unknown).bind(target)
    : value;
}

describe('upgrade from 0.1.0', () => {
  const LEGACY_INDEXES = [
    { key: { eventId: 1, handler: 1 }, name: 'event_handler', unique: true },
    {
      key: { handler: 1, dueAt: 1, _id: 1 },
      name: 'claim',
      partialFilterExpression: { lane: 'ready' },
    },
    {
      key: { handler: 1, key: 1, dueAt: 1 },
      name: 'running',
      partialFilterExpression: { status: 'processing', key: { $exists: true } },
    },
  ];

  function legacyEvent(event: EventEnvelope, sequence: number | null): RawDocument {
    return {
      _id: event.id,
      type: event.type,
      source: event.source,
      key: event.key,
      sequence,
      payload: event.payload,
      headers: event.headers,
      correlationId: null,
      causationId: null,
      occurredAt: event.occurredAt,
      revision: 0,
    };
  }

  async function createLegacyDatabase(): Promise<string> {
    const database = uniqueDatabaseName();
    const db = client.db(database);
    for (const name of ['relaybus_events', 'relaybus_deliveries', 'relaybus_key_sequences']) {
      await db.createCollection(name);
    }
    await db.collection('relaybus_deliveries').createIndexes(LEGACY_INDEXES);
    return database;
  }

  it('converts the documents in place, keeps every delivery and can run again', async () => {
    const database = await createLegacyDatabase();
    const { deliveries, events, lanes } = collections(database);
    const keyed = [createEnvelope({ key: 'order-1' }), createEnvelope({ key: 'order-1' })];
    const unkeyed = [createEnvelope(), createEnvelope(), createEnvelope()];
    const silent = createEnvelope({ occurredAt: at(-5) });
    await events.insertMany([
      legacyEvent(keyed[0]!, 1),
      legacyEvent(keyed[1]!, 2),
      ...unkeyed.map((event) => legacyEvent(event, null)),
      legacyEvent(silent, null),
    ]);
    const base = (event: EventEnvelope, sequence: number | null) => ({
      _id: createUuidV7(),
      eventId: event.id,
      eventType: event.type,
      handler: 'h',
      ...(sequence === null ? {} : { key: event.key, sequence }),
      availableAt: at(0),
      createdAt: at(-1),
      updatedAt: at(-1),
    });
    const processing = {
      ...base(keyed[0]!, 1),
      status: 'processing',
      lane: 'ready',
      attempts: 2,
      dueAt: at(LEASE_MS),
      leaseOwner: 'old-worker',
      leaseToken: createUuidV7(),
      leaseExpiresAt: at(LEASE_MS),
    };
    const follower = {
      ...base(keyed[1]!, 2),
      status: 'pending',
      lane: 'ready',
      attempts: 0,
      dueAt: at(0),
    };
    const completed = {
      ...base(unkeyed[0]!, null),
      status: 'completed',
      attempts: 1,
      completedAt: at(1),
    };
    const dead = { ...base(unkeyed[1]!, null), status: 'dead', attempts: 5 };
    const retrying = {
      ...base(unkeyed[2]!, null),
      status: 'pending',
      lane: 'ready',
      attempts: 1,
      dueAt: at(0),
    };
    await deliveries.insertMany([processing, follower, completed, dead, retrying]);

    const store = new MongoEventStore({ client, database });
    await store.setup();
    await store.setup();

    const indexNames = (await deliveries.indexes()).map((index) => index.name).sort();
    expect(indexNames).not.toContain('running');
    expect((await deliveries.indexes()).find((index) => index.name === 'claim')!.key).toEqual({
      laneId: 1,
      dueAt: 1,
      _id: 1,
      handler: 1,
    });
    expect(await lanes.countDocuments()).toBe(LANE_COUNT);
    expect(
      await deliveries.countDocuments({
        $or: [
          { leaseOwner: { $exists: true } },
          { leaseToken: { $exists: true } },
          { leaseExpiresAt: { $exists: true } },
          { laneId: { $exists: false } },
        ],
      }),
    ).toBe(0);
    expect(await deliveries.findOne({ _id: processing._id })).toMatchObject({
      status: 'pending',
      lane: 'ready',
      attempts: 2,
      dueAt: at(0),
      laneId: laneOf('order-1', 'unused'),
    });
    expect(await deliveries.findOne({ _id: dead._id })).toMatchObject({
      laneId: laneOf(null, unkeyed[1]!.id),
    });
    // Every event remembers its handlers and when it was created, so a completed handler never runs again.
    expect(await events.findOne({ _id: unkeyed[0]!.id })).toMatchObject({
      handlers: ['h'],
      createdAt: at(-1),
    });
    expect(await events.findOne({ _id: silent.id })).toMatchObject({
      handlers: [],
      createdAt: at(-5),
    });
    const republished = await store.transaction((session) =>
      store.append(
        session,
        [{ event: unkeyed[0]!, deliveries: [createDelivery('h'), createDelivery('other')] }],
        at(LEASE_MS),
      ),
    );
    expect(republished).toMatchObject({
      duplicateEventIds: [unkeyed[0]!.id],
      appendedDeliveryCount: 1,
    });

    expect(await store.statistics(at(LEASE_MS * 2))).toMatchObject({
      pending: 4,
      processing: 0,
      completed: 1,
      dead: 1,
    });
    const claimed = await claim(store, ['h', 'other'], 'worker-1', at(LEASE_MS * 2));
    // The delivery that was processing keeps the attempt 0.1.0 already charged to it.
    expect(claimed.map((delivery) => [delivery.deliveryId, delivery.attempt])).toEqual(
      expect.arrayContaining([
        [processing._id, 3],
        [retrying._id, 2],
      ]),
    );
    expect(claimed).toHaveLength(3);
    expect(claimed.map((delivery) => delivery.deliveryId)).not.toContain(follower._id);
    expect(await store.completeMany(claimed, at(LEASE_MS * 2 + 1))).toEqual(
      claimed.map(() => true),
    );
    const [next] = await claim(store, ['h'], 'worker-1', at(LEASE_MS * 2 + 2));
    expect(next!.deliveryId).toBe(follower._id);
    // Legacy completed documents are purged as before, and so is the event nobody subscribed to.
    const purged = await store.purge({ completedBefore: at(LEASE_MS * 3) });
    expect(purged.deletedDeliveries).toBe(1);
    expect(await events.findOne({ _id: silent.id })).toBeNull();
  });

  it('does not overwrite the handlers another process recorded while this one converted the same events', async () => {
    const database = await createLegacyDatabase();
    const { deliveries, events } = collections(database);
    const event = createEnvelope();
    await events.insertOne(legacyEvent(event, null));
    await deliveries.insertOne({
      _id: createUuidV7(),
      eventId: event.id,
      eventType: event.type,
      handler: 'h',
      status: 'completed',
      attempts: 1,
      availableAt: at(0),
      createdAt: at(-1),
      updatedAt: at(-1),
      completedAt: at(1),
    });
    // A slower upgrader: another process finishes the conversion and subscribes a new handler between this
    // one reading the events and writing them.
    let faster = true;
    const racing = new Proxy(client, {
      get: (target, property) =>
        property === 'db'
          ? (name?: string) =>
              new Proxy(target.db(name), {
                get: (db, member) =>
                  member === 'collection'
                    ? (collectionName: string, options?: object) => {
                        const collection = db.collection(collectionName, options);
                        return collectionName.endsWith('deliveries')
                          ? new Proxy(collection, {
                              get: (inner, method) =>
                                method === 'aggregate'
                                  ? (...parameters: Parameters<typeof inner.aggregate>) => {
                                      const cursor = inner.aggregate(...parameters);
                                      return {
                                        toArray: async () => {
                                          if (faster) {
                                            faster = false;
                                            await events.updateOne(
                                              { _id: event.id },
                                              { $set: { handlers: ['h', 'added'] } },
                                            );
                                          }
                                          return cursor.toArray();
                                        },
                                      };
                                    }
                                  : bound(inner, method),
                            })
                          : collection;
                      }
                    : bound(db, member),
              })
          : bound(target, property),
    });
    const store = new MongoEventStore({ client: racing, database });
    await store.setup();
    expect(faster).toBe(false);
    expect(await events.findOne({ _id: event.id })).toMatchObject({ handlers: ['h', 'added'] });
  });

  it('converts large collections in batches and skips the conversion once the lanes exist', async () => {
    const database = await createLegacyDatabase();
    const { deliveries, events } = collections(database);
    const legacy = Array.from({ length: 1_001 }, () => createEnvelope());
    await events.insertMany(legacy.map((event) => legacyEvent(event, null)));
    await deliveries.insertMany(
      legacy.map((event) => ({
        _id: createUuidV7(),
        eventId: event.id,
        eventType: event.type,
        handler: 'h',
        status: 'pending',
        lane: 'ready',
        attempts: 0,
        availableAt: at(0),
        dueAt: at(0),
        createdAt: at(0),
        updatedAt: at(0),
      })),
    );
    const store = new MongoEventStore({ client, database });
    await store.setup();
    expect(await deliveries.countDocuments({ laneId: { $exists: true } })).toBe(1_001);
    expect(await events.countDocuments({ handlers: ['h'] })).toBe(1_001);

    // A later setup() does not look at the documents again.
    const marker = legacy[0]!.id;
    await deliveries.updateOne({ eventId: marker }, { $unset: { laneId: '' } });
    await store.setup();
    expect(await deliveries.countDocuments({ laneId: { $exists: false } })).toBe(1);
  });
});
