import { describe, expect, it } from 'vitest';
import { parseMemoryMiB } from '../lib/docker-stats.ts';
import { Hist } from '../lib/hist.ts';

describe('Hist', () => {
  it('estimates quantiles within one percent and merges serialized histograms', () => {
    const left = new Hist();
    const right = new Hist();
    for (let value = 1; value <= 500; value += 1) left.add(value);
    for (let value = 501; value <= 1000; value += 1) right.add(value);
    left.merge(right.toJSON());
    const summary = left.summary();
    expect(summary.n).toBe(1000);
    expect(summary.p50).toBeGreaterThan(495);
    expect(summary.p50).toBeLessThan(506);
    expect(summary.p99).toBeGreaterThan(980);
    expect(summary.max).toBe(1000);
    expect(new Hist().quantile(0.5)).toBeNaN();
  });
});

describe('parseMemoryMiB', () => {
  it('converts docker units', () => {
    expect(parseMemoryMiB('512MiB')).toBe(512);
    expect(parseMemoryMiB('1.5GiB')).toBe(1536);
    expect(parseMemoryMiB('2048KiB')).toBe(2);
    expect(parseMemoryMiB('1048576B')).toBe(1);
    expect(parseMemoryMiB('n/a')).toBeNaN();
  });
});
