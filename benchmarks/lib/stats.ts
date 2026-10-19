// Small statistics toolbox for the benchmark comparison: medians and seeded bootstrap confidence intervals.
// Everything here is pure and deterministic so the comparison logic can be unit tested.

/** Confidence interval around a point estimate. */
export interface Estimate {
  /** Point estimate (median based). */
  readonly value: number;
  readonly low: number;
  readonly high: number;
  /** Number of observations (pairs for a paired estimate, the smaller side for an unpaired one). */
  readonly n: number;
}

export interface BootstrapOptions {
  /** Number of resamples. Default 5000. */
  readonly iterations?: number;
  /** Two-sided confidence level. Default 0.95. */
  readonly confidence?: number;
  /** Seed for the pseudo random generator, so the same data always yields the same interval. Default 1. */
  readonly seed?: number;
}

const DEFAULT_ITERATIONS = 5000;
const DEFAULT_CONFIDENCE = 0.95;

/** Median of a non-empty list; NaN for an empty list. */
export function median(values: readonly number[]): number {
  if (values.length === 0) return Number.NaN;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  const upper = sorted[middle] ?? Number.NaN;
  if (sorted.length % 2 === 1) return upper;
  return ((sorted[middle - 1] ?? Number.NaN) + upper) / 2;
}

/** Linear-interpolated quantile (0..1) of a list; NaN for an empty list. */
export function quantile(values: readonly number[], q: number): number {
  if (values.length === 0) return Number.NaN;
  const sorted = [...values].sort((a, b) => a - b);
  const position = Math.min(1, Math.max(0, q)) * (sorted.length - 1);
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const low = sorted[lower] ?? Number.NaN;
  const high = sorted[upper] ?? Number.NaN;
  return low + (high - low) * (position - lower);
}

/** Deterministic 32 bit pseudo random generator returning numbers in [0, 1). */
export function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** FNV-1a hash of a string, used to derive a stable seed from a cell id. */
export function hashSeed(text: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

function resample(values: readonly number[], random: () => number): number[] {
  return Array.from(
    { length: values.length },
    () => values[Math.floor(random() * values.length)] ?? Number.NaN,
  );
}

function interval(statistics: number[], confidence: number): [number, number] {
  const tail = (1 - confidence) / 2;
  return [quantile(statistics, tail), quantile(statistics, 1 - tail)];
}

/**
 * Paired comparison: `candidate[i] / base[i]` is one observation (the two samples of pair i were measured back to back).
 * The estimate is the median of those ratios; the interval is a percentile bootstrap of that median.
 */
export function pairedRatio(
  base: readonly number[],
  candidate: readonly number[],
  options: BootstrapOptions = {},
): Estimate {
  if (base.length !== candidate.length)
    throw new RangeError('A paired ratio needs the same number of base and candidate samples');
  const ratios = base.map((value, index) => (candidate[index] ?? Number.NaN) / value);
  if (ratios.length === 0) return { value: Number.NaN, low: Number.NaN, high: Number.NaN, n: 0 };
  const random = mulberry32(options.seed ?? 1);
  const medians = Array.from({ length: options.iterations ?? DEFAULT_ITERATIONS }, () =>
    median(resample(ratios, random)),
  );
  const [low, high] = interval(medians, options.confidence ?? DEFAULT_CONFIDENCE);
  return { value: median(ratios), low, high, n: ratios.length };
}

/**
 * Unpaired comparison (candidate measured now, base loaded from a stored baseline): ratio of the two medians,
 * with a percentile bootstrap that resamples each side independently.
 */
export function medianRatio(
  base: readonly number[],
  candidate: readonly number[],
  options: BootstrapOptions = {},
): Estimate {
  const n = Math.min(base.length, candidate.length);
  if (n === 0) return { value: Number.NaN, low: Number.NaN, high: Number.NaN, n: 0 };
  const random = mulberry32(options.seed ?? 1);
  const ratios = Array.from(
    { length: options.iterations ?? DEFAULT_ITERATIONS },
    () => median(resample(candidate, random)) / median(resample(base, random)),
  );
  const [low, high] = interval(ratios, options.confidence ?? DEFAULT_CONFIDENCE);
  return { value: median(candidate) / median(base), low, high, n };
}
