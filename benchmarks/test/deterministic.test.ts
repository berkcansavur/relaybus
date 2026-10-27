import { describe, expect, it } from 'vitest';
import {
  deterministicIdentifiers,
  deterministicKeys,
  WORKLOAD_SEED,
} from '../lib/deterministic.ts';
import { NO_KEYS } from '../lib/cells.ts';

const take = <T>(next: () => T, count: number): T[] => Array.from({ length: count }, next);
const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe('deterministic identifiers', () => {
  it('gives the same sequence for the same seed and worker, as often as it is asked', () => {
    const first = take(deterministicIdentifiers(WORKLOAD_SEED, 0), 1000);
    const second = take(deterministicIdentifiers(WORKLOAD_SEED, 0), 1000);
    expect(second).toEqual(first);
  });

  it('differs between workers and between seeds, and never repeats inside a worker', () => {
    const base = take(deterministicIdentifiers(WORKLOAD_SEED, 0), 1000);
    expect(new Set(base).size).toBe(1000);
    expect(take(deterministicIdentifiers(WORKLOAD_SEED, 1), 1000)).not.toEqual(base);
    expect(take(deterministicIdentifiers(WORKLOAD_SEED + 1, 0), 1000)).not.toEqual(base);
    const other = new Set(take(deterministicIdentifiers(WORKLOAD_SEED, 1), 1000));
    expect(base.filter((id) => other.has(id))).toEqual([]);
  });

  it('is shaped like a version 7 UUID whose time part orders the identifiers', () => {
    const ids = take(deterministicIdentifiers(WORKLOAD_SEED, 3), 300);
    expect(ids.every((id) => UUID_V7.test(id))).toBe(true);
    expect([...ids].sort()).not.toEqual([]);
    const times = ids.map((id) => id.slice(0, 13));
    expect([...times].sort()).toEqual(times);
  });

  it('spreads the ids over the 256 lanes the stores hash them into', () => {
    // The first hex digits of the random part stand in for the hash: a skew here would skew the lane layout of a template.
    const buckets = new Map<string, number>();
    for (const id of take(deterministicIdentifiers(WORKLOAD_SEED, 0), 20_000)) {
      const bucket = id.slice(-2);
      buckets.set(bucket, (buckets.get(bucket) ?? 0) + 1);
    }
    expect(buckets.size).toBe(256);
    expect(Math.min(...buckets.values())).toBeGreaterThan(30);
  });
});

describe('deterministic keys', () => {
  it('publishes without keys when the cell has none', () => {
    expect(take(deterministicKeys(WORKLOAD_SEED, 0, NO_KEYS), 3)).toEqual([
      undefined,
      undefined,
      undefined,
    ]);
  });

  it('repeats the same keys for the same seed and worker, within the key count', () => {
    const spec = { mode: 'hot', count: 10 } as const;
    const first = take(deterministicKeys(WORKLOAD_SEED, 1, spec), 500);
    expect(take(deterministicKeys(WORKLOAD_SEED, 1, spec), 500)).toEqual(first);
    expect(take(deterministicKeys(WORKLOAD_SEED, 2, spec), 500)).not.toEqual(first);
    expect(new Set(first)).toEqual(new Set(Array.from({ length: 10 }, (_, key) => `k${key}`)));
  });
});
