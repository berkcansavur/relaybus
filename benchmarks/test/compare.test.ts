import { describe, expect, it } from 'vitest';
import { busyFlags, compareCell, gate } from '../lib/compare.ts';
import type { Sample, SideName } from '../lib/sample.ts';

function sample(
  side: SideName,
  pair: number,
  metrics: Record<string, number | null>,
  extra: { calibrationMs?: number; valid?: boolean } = {},
): Sample {
  return {
    cellId: 'cell',
    side,
    pair,
    startedAt: '2026-01-01T00:00:00Z',
    metrics,
    validation: { valid: extra.valid ?? true, reasons: extra.valid === false ? ['lost 1'] : [] },
    load: {
      loadavgStart: 1,
      loadavgEnd: 1,
      calibrationMs: extra.calibrationMs ?? 20,
      hostCpuPct: 50,
      dbCpuAvgPct: 100,
    },
    notes: [],
  };
}

const series = (
  side: SideName,
  values: number[],
  metric = 'deliveriesPerSec',
  calibration: number[] = [],
): Sample[] =>
  values.map((value, pair) =>
    sample(side, pair, { [metric]: value }, { calibrationMs: calibration[pair] ?? 20 }),
  );

const find = (comparison: ReturnType<typeof compareCell>, metric: string) =>
  comparison.metrics.find((entry) => entry.metric === metric)!;

describe('counters', () => {
  it('treat changes inside 3 % as unchanged and outside as regression or improvement', () => {
    const base = [
      sample('base', 0, {
        'ctr.statementsPerUnit': 10,
        'ctr.walBytesPerUnit': 1000,
        'ctr.deliveries.updPerUnit': 3,
      }),
    ];
    const candidate = [
      sample('candidate', 0, {
        'ctr.statementsPerUnit': 10.2,
        'ctr.walBytesPerUnit': 1100,
        'ctr.deliveries.updPerUnit': 2,
      }),
    ];
    const result = compareCell('cell', base, candidate);
    expect(find(result, 'ctr.statementsPerUnit').verdict).toBe('unchanged');
    expect(find(result, 'ctr.walBytesPerUnit').verdict).toBe('regression');
    expect(find(result, 'ctr.deliveries.updPerUnit').verdict).toBe('improvement');
  });

  it('judges paired runs by the median of the per-pair ratios, so drift over the run cancels', () => {
    // The counter drifts from 0.045 to 0.057 over the run on both sides; every pair is within 5 %, the medians alone differ by 3.3 %.
    const base = series('base', [0.0455, 0.0449, 0.0446, 0.0563, 0.0563], 'ctr.statementsPerUnit');
    const candidate = series(
      'candidate',
      [0.045, 0.047, 0.0454, 0.0536, 0.057],
      'ctr.statementsPerUnit',
    );
    const paired = find(
      compareCell('cell', base, candidate, { pairing: 'paired' }),
      'ctr.statementsPerUnit',
    );
    expect(paired.verdict).toBe('unchanged');
    expect(paired.ratio).toBeCloseTo(1.0124, 3);
    expect(find(compareCell('cell', base, candidate), 'ctr.statementsPerUnit').verdict).toBe(
      'regression',
    );
  });

  it('still flags a counter that rises in every pair, and keeps zero baselines on the medians', () => {
    const base = series('base', [0.045, 0.05, 0.056], 'ctr.deliveries.updPerUnit');
    const candidate = base.map((entry) => ({
      ...entry,
      side: 'candidate' as const,
      metrics: {
        'ctr.deliveries.updPerUnit': (entry.metrics['ctr.deliveries.updPerUnit'] ?? 0) + 1,
      },
    }));
    const result = compareCell('cell', base, candidate, { pairing: 'paired' });
    expect(find(result, 'ctr.deliveries.updPerUnit').verdict).toBe('regression');
    const zero = compareCell(
      'cell',
      series('base', [0, 0, 0], 'ctr.x'),
      series('candidate', [1, 1, 1], 'ctr.x'),
      {
        pairing: 'paired',
      },
    );
    expect(find(zero, 'ctr.x').verdict).toBe('regression');
  });

  it('gives the timing dependent lane counter a wide tolerance and every other counter 3 %', () => {
    const base = [
      sample('base', 0, { 'ctr.lanes.updPerUnit': 0.04, 'ctr.deliveries.updPerUnit': 1 }),
    ];
    const candidate = [
      sample('candidate', 0, { 'ctr.lanes.updPerUnit': 0.044, 'ctr.deliveries.updPerUnit': 1.1 }),
    ];
    const result = compareCell('cell', base, candidate);
    expect(find(result, 'ctr.lanes.updPerUnit').verdict).toBe('unchanged');
    expect(find(result, 'ctr.deliveries.updPerUnit').verdict).toBe('regression');
    const doubled = compareCell('cell', base, [
      sample('candidate', 0, { 'ctr.lanes.updPerUnit': 0.08, 'ctr.deliveries.updPerUnit': 1 }),
    ]);
    expect(find(doubled, 'ctr.lanes.updPerUnit').verdict).toBe('regression');
  });

  it('treats a higher HOT update share as an improvement', () => {
    const result = compareCell(
      'cell',
      [sample('base', 0, { 'ctr.hotUpdPct': 50 })],
      [sample('candidate', 0, { 'ctr.hotUpdPct': 90 })],
    );
    expect(find(result, 'ctr.hotUpdPct').verdict).toBe('improvement');
  });

  it('handles zero baselines and values that do not exist for one side', () => {
    const result = compareCell(
      'cell',
      [sample('base', 0, { 'ctr.lanes.updPerUnit': null, 'ctr.a': 0, 'ctr.b': 0, 'ctr.c': 0 })],
      [sample('candidate', 0, { 'ctr.lanes.updPerUnit': 1, 'ctr.a': 0, 'ctr.b': 2, 'ctr.c': 0 })],
    );
    expect(find(result, 'ctr.lanes.updPerUnit').verdict).toBe('n/a');
    expect(find(result, 'ctr.a').verdict).toBe('unchanged');
    expect(find(result, 'ctr.b').verdict).toBe('regression');
  });

  it('only report informational counters', () => {
    const result = compareCell(
      'cell',
      [sample('base', 0, { 'ctr.walFpiPerUnit': 1, 'info.clientCores': 1 })],
      [sample('candidate', 0, { 'ctr.walFpiPerUnit': 5, 'info.clientCores': 3 })],
    );
    expect(find(result, 'ctr.walFpiPerUnit').verdict).toBe('n/a');
    expect(find(result, 'info.clientCores').ratio).toBe(3);
  });
});

describe('throughput and latency', () => {
  it('flags a clear throughput regression from paired samples', () => {
    const result = compareCell(
      'cell',
      series('base', [1000, 1010, 990, 1005]),
      series('candidate', [800, 805, 795, 810]),
      { pairing: 'paired' },
    );
    const throughput = find(result, 'deliveriesPerSec');
    expect(result.pairing).toBe('paired');
    expect(throughput.verdict).toBe('regression');
    expect(throughput.ratio).toBeCloseTo(0.8, 1);
    expect(throughput.high).toBeLessThan(0.93);
  });

  it('flags an improvement and a no-change result', () => {
    expect(
      find(
        compareCell('cell', series('base', [100, 100, 100]), series('candidate', [150, 152, 148]), {
          pairing: 'paired',
        }),
        'deliveriesPerSec',
      ).verdict,
    ).toBe('improvement');
    expect(
      find(
        compareCell('cell', series('base', [100, 101, 99]), series('candidate', [101, 100, 100]), {
          pairing: 'paired',
        }),
        'deliveriesPerSec',
      ).verdict,
    ).toBe('unchanged');
  });

  it('calls a wide interval inconclusive and needs three samples for a verdict', () => {
    expect(
      find(
        compareCell('cell', series('base', [100, 100, 100]), series('candidate', [60, 100, 140]), {
          pairing: 'paired',
        }),
        'deliveriesPerSec',
      ).verdict,
    ).toBe('inconclusive');
    expect(
      find(
        compareCell('cell', series('base', [100]), series('candidate', [50]), {
          pairing: 'paired',
        }),
        'deliveriesPerSec',
      ).verdict,
    ).toBe('inconclusive');
  });

  it('treats a latency increase as the regression', () => {
    const base = series('base', [10, 10, 10], 'txP99Ms');
    expect(
      find(
        compareCell('cell', base, series('candidate', [20, 21, 19], 'txP99Ms'), {
          pairing: 'paired',
        }),
        'txP99Ms',
      ).verdict,
    ).toBe('regression');
    expect(
      find(
        compareCell('cell', base, series('candidate', [5, 5.1, 4.9], 'txP99Ms'), {
          pairing: 'paired',
        }),
        'txP99Ms',
      ).verdict,
    ).toBe('improvement');
  });

  it('compares unpaired data (stored baseline) by the ratio of medians', () => {
    const result = compareCell(
      'cell',
      series('base', [100, 105, 95]),
      series('candidate', [70, 72, 69]),
    );
    expect(result.pairing).toBe('unpaired');
    expect(find(result, 'deliveriesPerSec').verdict).toBe('regression');
  });

  it('does not pair when the pair numbers differ', () => {
    const base = series('base', [100, 100, 100]);
    const candidate = series('candidate', [100, 100, 100]).map((entry, index) => ({
      ...entry,
      pair: index + 10,
    }));
    expect(compareCell('cell', base, candidate, { pairing: 'paired' }).pairing).toBe('unpaired');
  });
});

describe('busy machine', () => {
  it('flags samples whose calibration is much slower than the quietest one', () => {
    expect(busyFlags(series('base', [1, 1, 1], 'x', [20, 21, 40]))).toEqual([false, false, true]);
  });

  it('drops a pair when either side was busy', () => {
    const base = series('base', [100, 100, 100, 100], 'deliveriesPerSec', [20, 20, 20, 20]);
    const candidate = series(
      'candidate',
      [100, 100, 100, 10],
      'deliveriesPerSec',
      [20, 20, 20, 60],
    );
    const result = compareCell('cell', base, candidate, { pairing: 'paired' });
    expect(result.discardedBusy).toBe(2);
    expect(result.noisy).toBe(false);
    expect(find(result, 'deliveriesPerSec').verdict).toBe('unchanged');
  });

  it('keeps busy samples and says so when too few clean ones remain', () => {
    const base = series('base', [100, 100], 'deliveriesPerSec', [20, 20]);
    const candidate = series('candidate', [100, 50], 'deliveriesPerSec', [20, 60]);
    const result = compareCell('cell', base, candidate, { pairing: 'paired' });
    expect(result.noisy).toBe(true);
    expect(result.discardedBusy).toBe(0);
  });

  it('filters busy samples of an unpaired comparison per side', () => {
    const base = series('base', [100, 100, 100], 'deliveriesPerSec', [20, 20, 90]);
    const candidate = series('candidate', [100, 100, 100], 'deliveriesPerSec', [20, 20, 20]);
    const result = compareCell('cell', base, candidate);
    expect(result.discardedBusy).toBe(1);
    expect(result.noisy).toBe(false);
  });
});

describe('gate', () => {
  const clean = compareCell(
    'cell',
    [sample('base', 0, { 'ctr.statementsPerUnit': 10 })],
    [sample('candidate', 0, { 'ctr.statementsPerUnit': 10 })],
  );

  it('passes when nothing regressed', () => {
    expect(gate([clean]).passed).toBe(true);
  });

  it('fails on a counter regression and on a validation failure', () => {
    const regression = compareCell(
      'cell',
      [sample('base', 0, { 'ctr.statementsPerUnit': 10 })],
      [sample('candidate', 0, { 'ctr.statementsPerUnit': 20 })],
    );
    expect(gate([regression]).reasons[0]).toContain('statementsPerUnit regressed (100.0 %)');
    const fromZero = compareCell(
      'cell',
      [sample('base', 0, { 'ctr.x': 0 })],
      [sample('candidate', 0, { 'ctr.x': 1 })],
    );
    expect(gate([fromZero]).reasons[0]).toContain('from zero');
    const invalid = compareCell(
      'cell',
      [sample('base', 0, {})],
      [sample('candidate', 0, {}, { valid: false })],
    );
    expect(gate([invalid]).reasons[0]).toContain('validation failed');
  });

  it('waives a counter regression that was accepted by name but still reports it', () => {
    const shifted = compareCell(
      'cell',
      [sample('base', 0, { 'ctr.deliveries.delPerUnit': 0 })],
      [sample('candidate', 0, { 'ctr.deliveries.delPerUnit': 1 })],
    );
    const result = gate([shifted], { accept: ['deliveries.delPerUnit'] });
    expect(result.passed).toBe(true);
    expect(result.accepted).toEqual(['cell: counter deliveries.delPerUnit regressed (from zero)']);
    expect(gate([shifted], { accept: ['something.else'] }).passed).toBe(false);
  });

  it('fails on throughput regressions only in strict mode', () => {
    const slower = compareCell(
      'cell',
      series('base', [100, 100, 100]),
      series('candidate', [50, 51, 49]),
      { pairing: 'paired' },
    );
    expect(gate([slower]).passed).toBe(true);
    expect(gate([slower], { strict: true }).reasons[0]).toContain('regressed (ratio');
  });
});
