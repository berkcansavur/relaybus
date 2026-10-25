/**
 * Delays that double with every attempt, from `initialMs` up to `maxMs`: the first retry waits
 * `initialMs`, the second twice that, and so on. Without jitter, for in-process retries.
 */
export class ExponentialBackoff {
  readonly initialMs: number;
  readonly maxMs: number;

  constructor({ initialMs, maxMs }: { readonly initialMs: number; readonly maxMs: number }) {
    this.initialMs = initialMs;
    this.maxMs = maxMs;
  }

  /** The delay after failed attempt `attempt` (1-based). */
  delayFor(attempt: number): number {
    return Math.min(this.initialMs * 2 ** (attempt - 1), this.maxMs);
  }
}
