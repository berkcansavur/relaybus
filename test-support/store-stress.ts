import { expect } from 'vitest';
import { createUuidV7, type ClaimedDelivery, type EventStore } from '@relaybus/core';

/** What a backend gives the stress run. */
export interface StressBackend {
  /** A store instance of its own (its own pool or client), on the one shared schema or database. */
  createStore(): Promise<EventStore<unknown>>;
  /**
   * From now on, a claim whose answer the database sent is reported as failed when `lose()` says so,
   * which makes the store reconcile the owner's lanes. Returns the error the store then throws.
   */
  loseClaimAnswers(lose: () => boolean): Error;
  /** Transactions or sessions the run left open on the server. */
  openTransactions(): Promise<number>;
}

export interface StressOptions {
  readonly dispatchers?: number;
  readonly publishMs?: number;
  readonly drainMs?: number;
}

const HANDLERS = ['h1', 'h2'] as const;
const KEYS_PER_PUBLISHER = 12;
const PUBLISHERS = 2;
const LEASE_MS = 1_500;

interface Published {
  readonly eventId: string;
  readonly key: string | null;
  readonly position: number;
}

/**
 * Runs dispatchers (one store instance each) and publishers against one database for a bounded time:
 * keyed and unkeyed events with two handlers, completions single and batched, retries, releases,
 * workers that die with their batch (their lane leases expire and are taken over) and come back as a
 * new store instance that runs its setup next to the others, relinquished lanes, and claims whose
 * answer is lost so that the store reconciles. Then drains and checks that no operation failed except
 * the lost answers, every (event, handler) completed exactly once, the deliveries of a key completed in
 * publish order, and nothing is left running, pending or open.
 */
export async function runStoreStress(
  backend: StressBackend,
  options: StressOptions = {},
): Promise<void> {
  const dispatcherCount = options.dispatchers ?? 10;
  const publishMs = options.publishMs ?? 12_000;
  const drainMs = options.drainMs ?? 30_000;
  const stores = await Promise.all(
    Array.from({ length: dispatcherCount }, () => backend.createStore()),
  );
  const lost = backend.loseClaimAnswers(() => Math.random() < 0.03);

  const retired: EventStore<unknown>[] = [];
  const published = new Map<string, Published>();
  const completed = new Map<string, number>();
  const lastPosition = new Map<string, number>();
  const failures: unknown[] = [];
  const orderViolations: string[] = [];
  let lostAnswers = 0;
  const abandoned = new Set<string>();
  let takeovers = 0;
  let publishing = true;

  async function publisher(index: number): Promise<void> {
    const store = stores[index]!;
    const nextPosition = new Map<string, number>();
    while (publishing) {
      const now = new Date();
      const entries = Array.from({ length: 1 + Math.floor(Math.random() * 5) }, () => {
        const key =
          Math.random() < 0.6
            ? `p${index}-k${Math.floor(Math.random() * KEYS_PER_PUBLISHER)}`
            : null;
        const position = key === null ? 0 : (nextPosition.get(key) ?? 0);
        if (key !== null) {
          nextPosition.set(key, position + 1);
        }
        const eventId = createUuidV7();
        published.set(eventId, { eventId, key, position });
        return {
          event: {
            id: eventId,
            type: 'stress.v1',
            source: 'stress',
            key,
            payload: { position },
            headers: {},
            correlationId: null,
            causationId: null,
            occurredAt: now,
          },
          deliveries: HANDLERS.map((handler) => ({
            id: createUuidV7(),
            handler,
            availableAt: now,
          })),
        };
      });
      await store.transaction((transaction) => store.append(transaction, entries, now));
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 4));
    }
  }

  function recordCompletion(delivery: ClaimedDelivery): void {
    const name = `${delivery.event.id}/${delivery.handler}`;
    completed.set(name, (completed.get(name) ?? 0) + 1);
    const event = published.get(delivery.event.id);
    if (event?.key === null || event === undefined) {
      return;
    }
    const group = `${delivery.handler}/${event.key}`;
    const previous = lastPosition.get(group) ?? -1;
    if (event.position !== previous + 1) {
      orderViolations.push(`${group}: completed ${event.position} after ${previous}`);
    }
    lastPosition.set(group, Math.max(previous, event.position));
  }

  const expected = (): number => published.size * HANDLERS.length;
  const done = (): boolean => !publishing && completed.size >= expected();

  async function dispatcher(index: number): Promise<void> {
    let store = stores[index]!;
    let generation = 0;
    let owner = `stress-${index}-${generation}`;
    const deadline = Date.now() + publishMs + drainMs;
    while (!done() && Date.now() < deadline) {
      const now = new Date();
      let batch: ClaimedDelivery[];
      try {
        batch = await store.claim({
          handlers: [...HANDLERS],
          limit: 1 + Math.floor(Math.random() * 12),
          leaseOwner: owner,
          leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
          now,
        });
      } catch (error) {
        if (error === lost) {
          lostAnswers += 1;
          continue;
        }
        throw error;
      }
      for (const delivery of batch) {
        if (abandoned.delete(delivery.deliveryId)) {
          takeovers += 1;
        }
      }
      if (batch.length === 0) {
        await store.relinquish?.(owner, new Date());
        await new Promise((resolve) => setTimeout(resolve, 5));
        continue;
      }
      if (Math.random() < 0.01) {
        // The worker dies with its batch: its lane leases expire and other workers take the deliveries
        // over. It comes back under a new name.
        for (const delivery of batch) {
          abandoned.add(delivery.deliveryId);
        }
        generation += 1;
        owner = `stress-${index}-${generation}`;
        // A new process starts (and sets the store up) next to the running ones.
        store = await backend.createStore();
        retired.push(store);
        continue;
      }
      const toComplete: ClaimedDelivery[] = [];
      for (const delivery of batch) {
        const roll = Math.random();
        if (roll < 0.07) {
          await store.reschedule(
            delivery,
            new Date(Date.now() + Math.random() * 30),
            { name: 'Error', message: 'retry' },
            new Date(),
          );
        } else if (roll < 0.1) {
          await store.release(delivery, new Date());
        } else if (roll < 0.3) {
          if (await store.complete(delivery, new Date())) {
            recordCompletion(delivery);
          }
        } else {
          toComplete.push(delivery);
        }
      }
      if (toComplete.length > 0) {
        const results = await store.completeMany!(toComplete, new Date());
        results.forEach((result, position) => {
          if (result) {
            recordCompletion(toComplete[position]!);
          }
        });
      }
      if (Math.random() < 0.1) {
        await store.relinquish?.(owner, new Date());
      }
    }
  }

  const publishers = Array.from({ length: PUBLISHERS }, (_, index) =>
    publisher(index).catch((error: unknown) => failures.push(error)),
  );
  const dispatchers = stores.map((_, index) =>
    dispatcher(index).catch((error: unknown) => failures.push(error)),
  );
  await new Promise((resolve) => setTimeout(resolve, publishMs));
  publishing = false;
  await Promise.all(publishers);
  await Promise.all(dispatchers);

  const statistics = await stores[0]!.statistics(new Date());
  const duplicates = [...completed].filter(([, count]) => count !== 1);
  const open = await backend.openTransactions();
  await Promise.all([...stores, ...retired].map((store) => store.close?.() ?? Promise.resolve()));

  expect(failures).toEqual([]);
  expect(orderViolations).toEqual([]);
  expect(duplicates).toEqual([]);
  expect(completed.size).toBe(expected());
  expect(statistics).toMatchObject({ pending: 0, processing: 0, dead: 0 });
  expect(open).toBe(0);
  // The run exercised what it is meant to.
  expect(published.size).toBeGreaterThan(500);
  expect(lostAnswers).toBeGreaterThan(0);
  expect(takeovers).toBeGreaterThan(0);
}
