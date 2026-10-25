import { Collection, type CommandStartedEvent, type Document, type MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeAll, vi } from 'vitest';
import type { ClaimedDelivery, EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore, type MongoEventStoreOptions } from '../src/index.js';
import { laneOf } from '../src/lanes.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

export const LEASE_MS = 30_000;
export const error = { name: 'Error', message: 'x' };

/** A raw stored document: ids are strings (lanes: numbers), never ObjectIds. */
export type RawDocument = { _id: string | number } & Document;

/** The shared replica set and client; assigned by `useReplicaSet()` before the first test runs. */
export let replicaSet: MongoReplicaSet;
export let client: MongoClient;

/** Registers the replica set / client lifecycle and mock restoration in the calling file. */
export function useReplicaSet(): void {
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
}

export interface TestStore {
  readonly store: MongoEventStore;
  readonly database: string;
  readonly deliveries: Collection<RawDocument>;
  readonly lanes: Collection<RawDocument>;
  readonly events: Collection<RawDocument>;
}

export function collections(database: string): Omit<TestStore, 'store' | 'database'> {
  const db = client.db(database);
  return {
    deliveries: db.collection('relaybus_deliveries'),
    lanes: db.collection('relaybus_lanes'),
    events: db.collection('relaybus_events'),
  };
}

export async function createStore(
  options: Partial<Omit<MongoEventStoreOptions, 'client'>> = {},
): Promise<TestStore> {
  const database = options.database ?? uniqueDatabaseName();
  const store = new MongoEventStore({ client, ...options, database });
  await store.setup();
  return { store, database, ...collections(database) };
}

export async function append(
  store: MongoEventStore,
  entries: Array<{ event: EventEnvelope; handlers: string[] }>,
  now: Date = at(0),
): Promise<void> {
  await store.transaction((session) =>
    store.append(
      session,
      entries.map(({ event, handlers }) => ({
        event,
        deliveries: handlers.map((handler) => createDelivery(handler)),
      })),
      now,
    ),
  );
}

export function claim(
  store: MongoEventStore,
  handlers: string[],
  owner: string,
  now: Date = at(0),
  limit = 100,
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers,
    limit,
    leaseOwner: owner,
    leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
    now,
  });
}

/** One unkeyed event in each of the given lanes. */
export function eventsInLanes(lanes: readonly number[]): EventEnvelope[] {
  return lanes.map((lane) => {
    for (;;) {
      const event = createEnvelope();
      if (laneOf(null, event.id) === lane) {
        return event;
      }
    }
  });
}

/** Records the commands the shared client sends while `work` runs. */
export async function recordCommands(work: () => Promise<unknown>): Promise<CommandStartedEvent[]> {
  const started: CommandStartedEvent[] = [];
  const onStarted = (event: CommandStartedEvent): void => void started.push(event);
  client.on('commandStarted', onStarted);
  try {
    await work();
  } finally {
    client.off('commandStarted', onStarted);
  }
  return started;
}

export function sortById(documents: Document[]): Document[] {
  return [...documents].sort((left, right) => String(left['_id']).localeCompare(right['_id']));
}

/**
 * Wraps `method` on the collections whose name ends with `suffix`: `before`
 * runs ahead of the original call for every call that `matches` accepts.
 */
export function intercept<M extends 'find' | 'deleteMany' | 'bulkWrite'>(
  method: M,
  suffix: string,
  before: (args: unknown[]) => Promise<void>,
  matches: (args: unknown[]) => boolean = () => true,
  afterRead = false,
): void {
  const original = Collection.prototype[method] as (...args: unknown[]) => unknown;
  vi.spyOn(Collection.prototype, method).mockImplementation(function (
    this: Collection,
    ...args: unknown[]
  ) {
    if (!this.collectionName.endsWith(suffix) || !matches(args)) {
      return original.apply(this, args);
    }
    if (method === 'find') {
      // find returns a cursor synchronously: run `before` when the cursor is read.
      const cursor = original.apply(this, args) as { toArray(): Promise<unknown[]> };
      const toArray = cursor.toArray.bind(cursor);
      cursor.toArray = async () => {
        if (afterRead) {
          // Runs `before` once the documents were read, ahead of whatever the caller does next.
          const documents = await toArray();
          await before(args);
          return documents;
        }
        await before(args);
        return toArray();
      };
      return cursor;
    }
    return before(args).then(() => original.apply(this, args));
  } as never);
}
