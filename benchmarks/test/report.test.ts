import { describe, expect, it } from 'vitest';
import { quickPlan } from '../lib/cells.ts';
import { compareCell, gate } from '../lib/compare.ts';
import { renderReport } from '../lib/report.ts';
import type { Sample, SideName } from '../lib/sample.ts';

const make = (
  side: SideName,
  pair: number,
  metrics: Record<string, number | null>,
  valid = true,
): Sample => ({
  cellId: 'pg-pub-p8-b100',
  side,
  pair,
  startedAt: '2026-01-01T00:00:00Z',
  metrics,
  validation: { valid, reasons: valid ? [] : ['1 deliveries lost'] },
  load: { loadavgStart: 1, loadavgEnd: 1, calibrationMs: 20, hostCpuPct: 40, dbCpuAvgPct: 100 },
  notes: [],
});

describe('renderReport', () => {
  const plan = quickPlan();
  const base = [0, 1, 2].map((pair) =>
    make('base', pair, {
      eventsPerSec: 1000,
      txP99Ms: 10,
      'ctr.statementsPerUnit': 4,
      'ctr.walBytesPerUnit': 900,
      'info.clientCores': 1,
    }),
  );
  const candidate = [0, 1, 2].map((pair) =>
    make('candidate', pair, {
      eventsPerSec: 1500,
      txP99Ms: 10,
      'ctr.statementsPerUnit': 2,
      'ctr.walBytesPerUnit': 900,
      'info.clientCores': 1,
    }),
  );
  const comparison = compareCell('pg-pub-p8-b100', base, candidate, { pairing: 'paired' });

  it('shows the gate, ratios with intervals, moved counters and validation', () => {
    const report = renderReport({
      mode: 'quick',
      base: { ref: 'main', sha: 'a'.repeat(40) },
      candidate: { ref: 'HEAD', sha: 'b'.repeat(40) },
      fingerprint: 'fp',
      baseSource: 'interleaved',
      comparisons: [comparison],
      cells: plan.cells,
      gate: gate([comparison]),
      notes: ['a note'],
      elapsedMinutes: 9.5,
    });
    expect(report).toContain('gate: **PASS**');
    expect(report).toContain('measured interleaved');
    expect(report).toContain('publish P=8 batch=100 (`pg-pub-p8-b100`)');
    expect(report).toContain('1.500 [1.50, 1.50] (improvement)');
    expect(report).toContain('statementsPerUnit 0.50x (improvement)');
    expect(report).toContain('valid 6/6');
    expect(report).toContain('- a note');
  });

  it('lists gate failures, a stored baseline source and failed validation', () => {
    const broken = compareCell('pg-pub-p8-b100', base, [
      make('candidate', 0, { 'ctr.statementsPerUnit': 9 }, false),
    ]);
    const report = renderReport({
      mode: 'full',
      base: { ref: 'main', sha: 'a'.repeat(40) },
      candidate: { ref: 'HEAD', sha: 'b'.repeat(40) },
      fingerprint: 'fp',
      baseSource: 'baseline',
      comparisons: [broken],
      cells: plan.cells,
      gate: gate([broken]),
      notes: [],
      elapsedMinutes: 1,
    });
    expect(report).toContain('gate: **FAIL**');
    expect(report).toContain('loaded from the stored baseline');
    expect(report).toContain('FAILED: candidate pair 0: 1 deliveries lost');
    expect(report).toContain('statementsPerUnit 2.25x (regression)');
  });

  it('falls back to the id for cells it does not know and prints dashes for missing ratios', () => {
    const lonely = compareCell(
      'unknown-cell',
      [make('base', 0, { 'ctr.x': 0 })],
      [make('candidate', 0, { 'ctr.x': 1 })],
    );
    const report = renderReport({
      mode: 'quick',
      base: { ref: 'a', sha: 'a'.repeat(40) },
      candidate: { ref: 'b', sha: 'b'.repeat(40) },
      fingerprint: 'fp',
      baseSource: 'interleaved',
      comparisons: [lonely],
      cells: [],
      gate: gate([lonely]),
      notes: [],
      elapsedMinutes: 0,
    });
    expect(report).toContain('### unknown-cell');
    expect(report).toContain('x from 0 (regression)');
  });
});
