// What each measured quantity means and how it is judged. Keys prefixed `ctr.` are database-side counters per
// delivery (or per event for publish cells), the near-deterministic primary gate; `info.` keys are only reported.

export type MetricClass = 'counter' | 'throughput' | 'latency' | 'info';

export interface MetricDefinition {
  readonly class: MetricClass;
  /** Direction in which the value is an improvement. */
  readonly better: 'lower' | 'higher';
  /** Relative change inside which a difference is treated as noise. */
  readonly tolerance: number;
  readonly label: string;
}

export const COUNTER_TOLERANCE = 0.03;
export const THROUGHPUT_TOLERANCE = 0.07;
export const LATENCY_TOLERANCE = 0.15;

const THROUGHPUT_METRICS = new Set(['eventsPerSec', 'deliveriesPerSec', 'achievedPerSec']);
const LATENCY_METRICS = new Set(['txP99Ms', 'publishP99Ms', 'e2eP99Ms']);
/** Counters that are reported but depend on background flushing, so they never gate. */
const INFORMATIONAL_COUNTERS = new Set([
  'ctr.walFpiPerUnit',
  'ctr.journalBytesPerUnit',
  'ctr.commandsPerUnit',
]);

/** Classifies a metric key. Unknown keys are informational. */
export function definitionFor(metric: string): MetricDefinition {
  if (THROUGHPUT_METRICS.has(metric))
    return {
      class: 'throughput',
      better: 'higher',
      tolerance: THROUGHPUT_TOLERANCE,
      label: metric,
    };
  if (LATENCY_METRICS.has(metric))
    return { class: 'latency', better: 'lower', tolerance: LATENCY_TOLERANCE, label: metric };
  if (metric.startsWith('ctr.') && !INFORMATIONAL_COUNTERS.has(metric)) {
    return {
      class: 'counter',
      better: metric === 'ctr.hotUpdPct' ? 'higher' : 'lower',
      tolerance: COUNTER_TOLERANCE,
      label: metric.slice(4),
    };
  }
  return {
    class: 'info',
    better: 'lower',
    tolerance: Number.POSITIVE_INFINITY,
    label: metric.replace(/^(ctr|info)\./, ''),
  };
}
