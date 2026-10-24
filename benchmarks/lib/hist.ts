// Mergeable log-bucket histogram (1% relative error), values in milliseconds.
const LOG = Math.log(1.01);

export type SerializedHist = {
  counts: Record<string, number>;
  n: number;
  sum: number;
  max: number;
};

export class Hist {
  counts = new Map<number, number>();
  n = 0;
  sum = 0;
  max = 0;

  add(ms: number): void {
    const us = Math.max(1, Math.round(ms * 1000));
    const bucket = Math.floor(Math.log(us) / LOG);
    this.counts.set(bucket, (this.counts.get(bucket) ?? 0) + 1);
    this.n += 1;
    this.sum += ms;
    if (ms > this.max) this.max = ms;
  }

  merge(other: SerializedHist): void {
    for (const [bucket, count] of Object.entries(other.counts)) {
      this.counts.set(Number(bucket), (this.counts.get(Number(bucket)) ?? 0) + count);
    }
    this.n += other.n;
    this.sum += other.sum;
    if (other.max > this.max) this.max = other.max;
  }

  quantile(q: number): number {
    if (this.n === 0) return NaN;
    const target = Math.ceil(q * this.n);
    let seen = 0;
    for (const bucket of [...this.counts.keys()].sort((a, b) => a - b)) {
      seen += this.counts.get(bucket)!;
      if (seen >= target) return Math.exp((bucket + 0.5) * LOG) / 1000;
    }
    return this.max;
  }

  summary(): { n: number; mean: number; p50: number; p95: number; p99: number; max: number } {
    return {
      n: this.n,
      mean: this.n === 0 ? NaN : this.sum / this.n,
      p50: this.quantile(0.5),
      p95: this.quantile(0.95),
      p99: this.quantile(0.99),
      max: this.max,
    };
  }

  toJSON(): SerializedHist {
    return { counts: Object.fromEntries(this.counts), n: this.n, sum: this.sum, max: this.max };
  }
}
