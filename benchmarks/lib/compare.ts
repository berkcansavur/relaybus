// Comparison of the samples of one cell: which quantities moved, by how much, and whether the move is trustworthy.
// Pure functions only; nothing here touches a database or the file system.
import { definitionFor, type MetricDefinition } from './metrics.ts';
import type { Sample } from './sample.ts';
import { hashSeed, median, medianRatio, pairedRatio, type BootstrapOptions } from './stats.ts';

export type Verdict = 'regression' | 'improvement' | 'unchanged' | 'inconclusive' | 'n/a';

export interface MetricComparison {
  readonly metric: string;
  readonly definition: MetricDefinition;
  readonly baseMedian: number;
  readonly candidateMedian: number;
  /** candidate / base, median based. */
  readonly ratio: number;
  /** 95 % bootstrap interval of the ratio; absent for counters, which are compared on the median alone. */
  readonly low: number | null;
  readonly high: number | null;
  readonly verdict: Verdict;
}

export interface CellComparison {
  readonly cellId: string;
  readonly pairing: 'paired' | 'unpaired';
  readonly baseSamples: number;
  readonly candidateSamples: number;
  /** Samples left out because the machine was busy while they ran. */
  readonly discardedBusy: number;
  /** Fewer than two clean samples were left, so busy ones were kept; read the throughput ratios with care. */
  readonly noisy: boolean;
  readonly validationFailures: readonly string[];
  readonly metrics: readonly MetricComparison[];
}

export interface CompareOptions extends BootstrapOptions {
  /** `paired` when both sides were measured as interleaved pairs in this run; `unpaired` against a stored baseline. Default `unpaired`. */
  readonly pairing?: 'paired' | 'unpaired';
  /** A sample is busy when its calibration is this much slower than the fastest sample of the same data set. Default 1.25. */
  readonly busyFactor?: number;
  /** Clean samples needed before busy ones are discarded. Default 2. */
  readonly minimumClean?: number;
}

const DEFAULT_BUSY_FACTOR = 1.25;
const DEFAULT_MINIMUM_CLEAN = 2;

/** Marks the samples whose CPU calibration was much slower than the quietest sample of the same data set. */
export function busyFlags(samples: readonly Sample[], factor = DEFAULT_BUSY_FACTOR): boolean[] {
  const fastest = Math.min(...samples.map((sample) => sample.load.calibrationMs));
  return samples.map((sample) => sample.load.calibrationMs > fastest * factor);
}

function metricNames(samples: readonly Sample[]): string[] {
  const names = new Set<string>();
  for (const sample of samples) for (const name of Object.keys(sample.metrics)) names.add(name);
  return [...names].sort();
}

function valuesOf(samples: readonly Sample[], metric: string): number[] {
  const values: number[] = [];
  for (const sample of samples) {
    const value = sample.metrics[metric];
    if (value !== null && value !== undefined && Number.isFinite(value)) values.push(value);
  }
  return values;
}

/**
 * Counters are compared on the median alone; a counter only moves when the code changes, not when the machine is loaded.
 * Some of them still drift with the state of the database over the run (lane updates per delivery follow how the claims
 * batch up), and that drift hits both sides of one pair alike, so paired runs compare the median of the per-pair ratios.
 */
function counterVerdict(
  definition: MetricDefinition,
  base: number,
  candidate: number,
  pairedRatio?: number,
): Verdict {
  if (base === 0 && candidate === 0) return 'unchanged';
  if (base === 0) return definition.better === 'lower' ? 'regression' : 'improvement';
  const change = (pairedRatio ?? candidate / base) - 1;
  if (Math.abs(change) <= definition.tolerance) return 'unchanged';
  const worse = definition.better === 'lower' ? change > 0 : change < 0;
  return worse ? 'regression' : 'improvement';
}

/** Throughput and latency need an interval: a difference counts only when the whole interval is beyond the tolerance. */
function intervalVerdict(
  definition: MetricDefinition,
  low: number,
  high: number,
  samples: number,
): Verdict {
  if (samples < 3 || !Number.isFinite(low) || !Number.isFinite(high)) return 'inconclusive';
  const upper = 1 + definition.tolerance;
  const lower = 1 - definition.tolerance;
  const worseAbove = definition.better === 'lower';
  if (worseAbove ? low > upper : high < lower) return 'regression';
  if (worseAbove ? high < lower : low > upper) return 'improvement';
  return low >= lower && high <= upper ? 'unchanged' : 'inconclusive';
}

/**
 * Compares the candidate samples of a cell with the base samples. When both sides were measured as interleaved pairs
 * the ratios are paired per pair (drift cancels); otherwise (candidate against a stored baseline) the medians are compared.
 */
export function compareCell(
  cellId: string,
  base: readonly Sample[],
  candidate: readonly Sample[],
  options: CompareOptions = {},
): CellComparison {
  const factor = options.busyFactor ?? DEFAULT_BUSY_FACTOR;
  const minimumClean = options.minimumClean ?? DEFAULT_MINIMUM_CLEAN;
  const baseBusy = busyFlags(base, factor);
  const candidateBusy = busyFlags(candidate, factor);
  const pairNumbers = new Set(base.map((sample) => sample.pair));
  const paired =
    options.pairing === 'paired' &&
    base.length > 0 &&
    base.length === candidate.length &&
    candidate.every((sample) => pairNumbers.has(sample.pair));

  let usedBase: Sample[];
  let usedCandidate: Sample[];
  let discarded = 0;
  let noisy = false;
  if (paired) {
    const byPair = new Map(
      candidate.map((sample, index) => [
        sample.pair,
        { sample, busy: candidateBusy[index] ?? false },
      ]),
    );
    const rows = base.flatMap((sample, index) => {
      const other = byPair.get(sample.pair);
      return other === undefined
        ? []
        : [
            {
              base: sample,
              candidate: other.sample,
              busy: (baseBusy[index] ?? false) || other.busy,
            },
          ];
    });
    const clean = rows.filter((row) => !row.busy);
    noisy = clean.length < minimumClean;
    const kept = noisy ? rows : clean;
    discarded = (rows.length - kept.length) * 2;
    usedBase = kept.map((row) => row.base);
    usedCandidate = kept.map((row) => row.candidate);
  } else {
    const cleanBase = base.filter((_, index) => !baseBusy[index]);
    const cleanCandidate = candidate.filter((_, index) => !candidateBusy[index]);
    noisy = cleanBase.length < minimumClean || cleanCandidate.length < minimumClean;
    usedBase = noisy ? [...base] : cleanBase;
    usedCandidate = noisy ? [...candidate] : cleanCandidate;
    discarded = base.length - usedBase.length + (candidate.length - usedCandidate.length);
  }

  const failures = [...base, ...candidate].flatMap((sample) =>
    sample.validation.valid
      ? []
      : [`${sample.side} pair ${sample.pair}: ${sample.validation.reasons.join('; ')}`],
  );
  const metrics: MetricComparison[] = [];
  for (const metric of metricNames([...base, ...candidate])) {
    const definition = definitionFor(metric);
    const baseValues = valuesOf(usedBase, metric);
    const candidateValues = valuesOf(usedCandidate, metric);
    if (baseValues.length === 0 || candidateValues.length === 0) {
      metrics.push({
        metric,
        definition,
        baseMedian: median(baseValues),
        candidateMedian: median(candidateValues),
        ratio: Number.NaN,
        low: null,
        high: null,
        verdict: 'n/a',
      });
      continue;
    }
    const baseMedian = median(baseValues);
    const candidateMedian = median(candidateValues);
    if (definition.class === 'counter' || definition.class === 'info') {
      const pairedCounter =
        paired &&
        definition.class === 'counter' &&
        baseValues.length === candidateValues.length &&
        baseValues.length === usedBase.length &&
        baseValues.every((value) => value > 0);
      const ratio = pairedCounter
        ? median(candidateValues.map((value, index) => value / (baseValues[index] ?? Number.NaN)))
        : baseMedian === 0
          ? Number.NaN
          : candidateMedian / baseMedian;
      metrics.push({
        metric,
        definition,
        baseMedian,
        candidateMedian,
        ratio,
        low: null,
        high: null,
        verdict:
          definition.class === 'info'
            ? 'n/a'
            : counterVerdict(
                definition,
                baseMedian,
                candidateMedian,
                pairedCounter ? ratio : undefined,
              ),
      });
      continue;
    }
    const bootstrap: BootstrapOptions = {
      ...options,
      seed: (options.seed ?? 1) ^ hashSeed(`${cellId}/${metric}`),
    };
    const usePairs =
      paired &&
      baseValues.length === candidateValues.length &&
      baseValues.length === usedBase.length;
    const estimate = usePairs
      ? pairedRatio(baseValues, candidateValues, bootstrap)
      : medianRatio(baseValues, candidateValues, bootstrap);
    metrics.push({
      metric,
      definition,
      baseMedian,
      candidateMedian,
      ratio: estimate.value,
      low: estimate.low,
      high: estimate.high,
      verdict: intervalVerdict(
        definition,
        estimate.low,
        estimate.high,
        Math.min(baseValues.length, candidateValues.length),
      ),
    });
  }
  return {
    cellId,
    pairing: paired ? 'paired' : 'unpaired',
    baseSamples: base.length,
    candidateSamples: candidate.length,
    discardedBusy: discarded,
    noisy,
    validationFailures: failures,
    metrics,
  };
}

export interface GateResult {
  readonly passed: boolean;
  readonly reasons: readonly string[];
  /** Counter regressions that were waived with `--accept`; still reported. */
  readonly accepted: readonly string[];
}

export interface GateOptions {
  /** Also fail on throughput and latency regressions. */
  readonly strict?: boolean;
  /** Counter names (as shown in the report) whose regression is expected and therefore waived. */
  readonly accept?: readonly string[];
}

/**
 * The gate: exactness validation must hold everywhere and no gating counter may regress. Throughput and latency
 * regressions are reported but only fail the gate in strict mode, because a laptop cannot resolve small differences.
 * A counter that moves on purpose (work shifting from an UPDATE to a DELETE, say) is waived by naming it in `accept`.
 */
export function gate(
  comparisons: readonly CellComparison[],
  options: GateOptions = {},
): GateResult {
  const reasons: string[] = [];
  const accepted: string[] = [];
  for (const cell of comparisons) {
    for (const failure of cell.validationFailures)
      reasons.push(`${cell.cellId}: validation failed (${failure})`);
    for (const metric of cell.metrics) {
      if (metric.verdict !== 'regression') continue;
      if (metric.definition.class === 'counter') {
        const change = Number.isFinite(metric.ratio)
          ? `${((metric.ratio - 1) * 100).toFixed(1)} %`
          : 'from zero';
        const text = `${cell.cellId}: counter ${metric.definition.label} regressed (${change})`;
        if (options.accept?.includes(metric.definition.label) === true) accepted.push(text);
        else reasons.push(text);
      } else if (options.strict === true) {
        reasons.push(
          `${cell.cellId}: ${metric.definition.label} regressed (ratio ${metric.ratio.toFixed(3)})`,
        );
      }
    }
  }
  return { passed: reasons.length === 0, reasons, accepted };
}
