import { toMilliseconds, type Duration } from './duration.js';
import { ConfigurationError, NonRetryableError } from './errors.js';

/**
 * How the delay between attempts is randomized.
 * - `full`: uniform in [0, delay]. Spreads retries the most (AWS recommendation).
 * - `equal`: uniform in [delay / 2, delay].
 * - `none`: exactly `delay`. Synchronized retries can stampede a recovering dependency.
 */
export type JitterStrategy = 'full' | 'equal' | 'none';

export interface RetryPolicyOptions {
  /**
   * Total attempts including the first one. Default 10 for handlers, which
   * gives up after roughly 17 minutes of failures with the default delays.
   * `Infinity` retries until the delivery succeeds.
   */
  readonly attempts?: number;
  /** Delay before the second attempt. Default `'1s'`. */
  readonly initialDelay?: Duration;
  /** Growth factor of the delay per attempt. Default 2. */
  readonly factor?: number;
  /** Upper bound of a single delay. Default `'10m'`. */
  readonly maxDelay?: Duration;
  /** Default `'full'`. */
  readonly jitter?: JitterStrategy;
  /** Return false to dead-letter an error without retrying it. */
  readonly retryIf?: (error: unknown) => boolean;
}

export interface RetryPolicy {
  readonly attempts: number;
  readonly initialDelayMs: number;
  readonly factor: number;
  readonly maxDelayMs: number;
  readonly jitter: JitterStrategy;
  readonly retryIf: ((error: unknown) => boolean) | undefined;
}

/** Default policy of handlers. */
export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  attempts: 10,
  initialDelayMs: 1_000,
  factor: 2,
  maxDelayMs: 600_000,
  jitter: 'full',
  retryIf: undefined,
};

/**
 * Default policy of routes to transports: a broker outage must only delay
 * events, so routes retry until the broker accepts them, at most every 5 minutes.
 */
export const DEFAULT_ROUTE_RETRY_POLICY: RetryPolicy = {
  attempts: Number.POSITIVE_INFINITY,
  initialDelayMs: 1_000,
  factor: 2,
  maxDelayMs: 300_000,
  jitter: 'full',
  retryIf: undefined,
};

const JITTER_STRATEGIES: readonly JitterStrategy[] = ['full', 'equal', 'none'];

export function createRetryPolicy(
  options: RetryPolicyOptions | undefined,
  base: RetryPolicy = DEFAULT_RETRY_POLICY,
): RetryPolicy {
  const policy = mergeRetryPolicy(options, base);
  assertValidRetryPolicy(policy);
  return policy;
}

/** `options` over `base`, durations converted, without validation. */
function mergeRetryPolicy(options: RetryPolicyOptions | undefined, base: RetryPolicy): RetryPolicy {
  const overrides: RetryPolicyOptions = options ?? {};
  const initialDelayMs =
    overrides.initialDelay === undefined
      ? base.initialDelayMs
      : toMilliseconds(overrides.initialDelay, 'retry.initialDelay');
  return {
    attempts: overrides.attempts ?? base.attempts,
    initialDelayMs,
    factor: overrides.factor ?? base.factor,
    maxDelayMs:
      overrides.maxDelay === undefined
        ? // Raising only the initial delay must not trip the inherited cap.
          Math.max(base.maxDelayMs, initialDelayMs)
        : toMilliseconds(overrides.maxDelay, 'retry.maxDelay'),
    jitter: overrides.jitter ?? base.jitter,
    retryIf: overrides.retryIf ?? base.retryIf,
  };
}

function assertValidRetryPolicy(policy: RetryPolicy): void {
  if (
    policy.attempts !== Number.POSITIVE_INFINITY &&
    (!Number.isInteger(policy.attempts) || policy.attempts < 1)
  ) {
    throw new ConfigurationError(
      `retry.attempts must be a positive integer or Infinity, received ${policy.attempts}`,
    );
  }
  if (!Number.isFinite(policy.factor) || policy.factor < 1) {
    throw new ConfigurationError(`retry.factor must be at least 1, received ${policy.factor}`);
  }
  if (policy.maxDelayMs < policy.initialDelayMs) {
    throw new ConfigurationError('retry.maxDelay must not be smaller than retry.initialDelay');
  }
  if (!JITTER_STRATEGIES.includes(policy.jitter)) {
    throw new ConfigurationError(
      `retry.jitter must be one of ${JITTER_STRATEGIES.join(', ')}, received '${String(policy.jitter)}'`,
    );
  }
}

/**
 * Delay before the next attempt after `failedAttempts` failures (1-based):
 * capped exponential backoff with the policy's jitter.
 */
export function computeRetryDelay(
  policy: RetryPolicy,
  failedAttempts: number,
  random: () => number = Math.random,
): number {
  const exponential = policy.initialDelayMs * policy.factor ** (failedAttempts - 1);
  const capped = Math.min(policy.maxDelayMs, exponential);
  switch (policy.jitter) {
    case 'full':
      return Math.round(random() * capped);
    case 'equal':
      return Math.round(capped / 2 + (random() * capped) / 2);
    case 'none':
      return Math.round(capped);
  }
}

export function isRetryableError(policy: RetryPolicy, error: unknown): boolean {
  if (error instanceof NonRetryableError) {
    return false;
  }
  if (policy.retryIf === undefined) {
    return true;
  }
  try {
    return policy.retryIf(error);
  } catch {
    // A broken predicate must not lose the event: keep retrying until the budget runs out.
    return true;
  }
}
