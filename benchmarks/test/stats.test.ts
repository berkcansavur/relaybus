import { describe, expect, it } from 'vitest';
import { hashSeed, median, medianRatio, mulberry32, pairedRatio, quantile } from '../lib/stats.ts';

describe('median and quantile', () => {
  it('handles odd, even and empty lists', () => {
    expect(median([3, 1, 2])).toBe(2);
    expect(median([4, 1, 3, 2])).toBe(2.5);
    expect(median([])).toBeNaN();
  });

  it('interpolates quantiles and clamps the level', () => {
    expect(quantile([1, 2, 3, 4, 5], 0.5)).toBe(3);
    expect(quantile([0, 10], 0.25)).toBe(2.5);
    expect(quantile([1, 2], 2)).toBe(2);
    expect(quantile([], 0.5)).toBeNaN();
  });
});

describe('random numbers', () => {
  it('is deterministic per seed and stays inside [0, 1)', () => {
    const first = mulberry32(7);
    const second = mulberry32(7);
    const values = Array.from({ length: 100 }, () => first());
    expect(values).toEqual(Array.from({ length: 100 }, () => second()));
    expect(values.every((value) => value >= 0 && value < 1)).toBe(true);
    expect(mulberry32(8)()).not.toBe(values[0]);
  });

  it('hashes equal text to equal seeds', () => {
    expect(hashSeed('cell/metric')).toBe(hashSeed('cell/metric'));
    expect(hashSeed('a')).not.toBe(hashSeed('b'));
  });
});

describe('pairedRatio', () => {
  it('gives a degenerate interval when every pair has the same ratio', () => {
    const estimate = pairedRatio([10, 20, 40], [11, 22, 44], { seed: 3 });
    expect(estimate.value).toBeCloseTo(1.1);
    expect(estimate.low).toBeCloseTo(1.1);
    expect(estimate.high).toBeCloseTo(1.1);
    expect(estimate.n).toBe(3);
  });

  it('brackets the median ratio and widens with noise', () => {
    const tight = pairedRatio([100, 100, 100, 100, 100], [98, 99, 100, 101, 102], { seed: 1 });
    const loose = pairedRatio([100, 100, 100, 100, 100], [60, 90, 100, 130, 150], { seed: 1 });
    expect(tight.low).toBeLessThanOrEqual(tight.value);
    expect(tight.high).toBeGreaterThanOrEqual(tight.value);
    expect(loose.high - loose.low).toBeGreaterThan(tight.high - tight.low);
  });

  it('is reproducible and rejects unequal lengths', () => {
    expect(pairedRatio([1, 2, 3], [2, 3, 5], { seed: 9 })).toEqual(
      pairedRatio([1, 2, 3], [2, 3, 5], { seed: 9 }),
    );
    expect(() => pairedRatio([1], [1, 2])).toThrow(RangeError);
    expect(pairedRatio([], []).n).toBe(0);
  });
});

describe('medianRatio', () => {
  it('compares two independent samples', () => {
    const estimate = medianRatio([100, 102, 98, 101], [50, 51, 49, 52], { seed: 5 });
    expect(estimate.value).toBeCloseTo(0.5, 1);
    expect(estimate.high).toBeLessThan(0.6);
    expect(estimate.low).toBeGreaterThan(0.4);
    expect(estimate.n).toBe(4);
  });

  it('returns NaN for an empty side', () => {
    expect(medianRatio([], [1]).value).toBeNaN();
  });
});
