import { describe, expect, it } from 'vitest';
import { counterMetrics, type DbWindow } from '../lib/db-metrics.ts';
import { definitionFor } from '../lib/metrics.ts';

const window = (overrides: Partial<DbWindow> = {}): DbWindow => ({
  eventsInserted: 100,
  deliveriesInserted: 100,
  deliveriesCompleted: 100,
  counters: { statements: 450, walBytes: 120_000, 'deliveries.upd': 200 },
  deliveriesHotUpdates: 150,
  dockerCpuAvgPct: 80,
  waits: '',
  ...overrides,
});

describe('counterMetrics', () => {
  it('divides raw counters by the units and derives the HOT update share', () => {
    expect(counterMetrics(window(), 100)).toEqual({
      'ctr.statementsPerUnit': 4.5,
      'ctr.walBytesPerUnit': 1200,
      'ctr.deliveries.updPerUnit': 2,
      'ctr.hotUpdPct': 75,
    });
  });

  it('reports null for every counter when the window saw no unit and skips the HOT share without updates', () => {
    expect(counterMetrics(window(), 0)['ctr.statementsPerUnit']).toBeNull();
    expect(
      counterMetrics(window({ counters: { statements: 3 }, deliveriesHotUpdates: null }), 3),
    ).toEqual({ 'ctr.statementsPerUnit': 1 });
  });
});

describe('definitionFor', () => {
  it('classifies metric keys', () => {
    expect(definitionFor('deliveriesPerSec')).toMatchObject({
      class: 'throughput',
      better: 'higher',
    });
    expect(definitionFor('txP99Ms')).toMatchObject({ class: 'latency', better: 'lower' });
    expect(definitionFor('ctr.statementsPerUnit')).toMatchObject({
      class: 'counter',
      better: 'lower',
      label: 'statementsPerUnit',
    });
    expect(definitionFor('ctr.hotUpdPct')).toMatchObject({ class: 'counter', better: 'higher' });
    expect(definitionFor('ctr.walFpiPerUnit').class).toBe('info');
    expect(definitionFor('info.clientCores')).toMatchObject({
      class: 'info',
      label: 'clientCores',
    });
  });
});
