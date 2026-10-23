import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Sample } from '../lib/sample.ts';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  BASELINE_SCHEMA,
  calibrationDrift,
  findUsableBaseline,
  fingerprintOf,
  loadBaseline,
  mergeSamples,
  saveBaseline,
  stableStringify,
  type BaselineFile,
} from '../lib/baseline.ts';

let root: string;
beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), 'relaybus-baseline-'));
});
afterEach(() => {
  rmSync(root, { recursive: true, force: true });
});

const file = (overrides: Partial<BaselineFile> = {}): BaselineFile => ({
  schema: BASELINE_SCHEMA,
  sha: 'abc123',
  ref: 'main',
  fingerprint: 'fp1',
  createdAt: '2026-01-01T00:00:00Z',
  calibrationMs: 20,
  samples: [],
  ...overrides,
});

describe('fingerprint', () => {
  it('ignores key order and undefined values but reacts to real changes', () => {
    expect(stableStringify({ b: 1, a: [2, { d: 1, c: undefined }] })).toBe(
      '{"a":[2,{"d":1}],"b":1}',
    );
    expect(fingerprintOf({ a: 1, b: 2 })).toBe(fingerprintOf({ b: 2, a: 1 }));
    expect(fingerprintOf({ a: 1 })).not.toBe(fingerprintOf({ a: 2 }));
    expect(fingerprintOf({ a: 1 })).toHaveLength(12);
    expect(stableStringify(undefined)).toBe('null');
  });
});

describe('baseline storage', () => {
  it('returns nothing when there is no baseline for the commit and fingerprint', () => {
    expect(loadBaseline(root, 'fp1', 'abc123')).toBeUndefined();
  });

  it('round trips a baseline and keys it by fingerprint and commit', () => {
    const directory = saveBaseline(root, file(), { machine: 'test' });
    expect(directory).toBe(join(root, 'fp1', 'abc123'));
    expect(loadBaseline(root, 'fp1', 'abc123')).toEqual(file());
    expect(loadBaseline(root, 'fp2', 'abc123')).toBeUndefined();
    expect(loadBaseline(root, 'fp1', 'other')).toBeUndefined();
  });

  it('rejects a file of another schema version', () => {
    saveBaseline(root, file({ schema: BASELINE_SCHEMA + 1 }), {});
    expect(loadBaseline(root, 'fp1', 'abc123')).toBeUndefined();
  });

  it('reports how far the machine state drifted from the baseline', () => {
    expect(calibrationDrift(file({ calibrationMs: 20 }), 25)).toBe(1.25);
  });
});

const stored = (cellId: string, pair: number, side: 'base' | 'candidate' = 'base'): Sample => ({
  cellId,
  side,
  pair,
  startedAt: '2026-01-01T00:00:00Z',
  metrics: { deliveriesPerSec: 1 },
  validation: { valid: true, reasons: [] },
  load: { loadavgStart: 1, loadavgEnd: 1, calibrationMs: 20, hostCpuPct: 10, dbCpuAvgPct: null },
  notes: [],
});

describe('baseline reuse', () => {
  const samples = [stored('a', 0), stored('a', 1), stored('b', 0)];

  it('is used only when it covers every planned cell with enough samples', () => {
    saveBaseline(root, file({ samples }), {});
    expect(
      findUsableBaseline(root, 'fp1', 'abc123', [
        { id: 'a', reps: 2 },
        { id: 'b', reps: 1 },
      ])?.samples,
    ).toHaveLength(3);
    expect(findUsableBaseline(root, 'fp1', 'abc123', [{ id: 'a', reps: 3 }])).toBeUndefined();
    expect(findUsableBaseline(root, 'fp1', 'abc123', [{ id: 'c', reps: 1 }])).toBeUndefined();
    expect(
      findUsableBaseline(root, 'other-machine', 'abc123', [{ id: 'a', reps: 1 }]),
    ).toBeUndefined();
    expect(findUsableBaseline(root, 'fp1', 'other-commit', [{ id: 'a', reps: 1 }])).toBeUndefined();
  });

  it('merges fresh samples over stored ones cell by cell', () => {
    const merged = mergeSamples(samples, [stored('a', 0, 'candidate'), stored('c', 0)]);
    expect(merged.map((sample) => `${sample.cellId}${sample.pair}${sample.side}`)).toEqual([
      'b0base',
      'a0candidate',
      'c0base',
    ]);
  });
});
