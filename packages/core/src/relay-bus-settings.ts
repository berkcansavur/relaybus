import { hostname } from 'node:os';
import type { DispatcherSettings } from './dispatcher.js';
import { toMilliseconds, toTimerMilliseconds } from './duration.js';
import { ConfigurationError } from './errors.js';
import type { RegistrationDefaults } from './handler-registry.js';
import type { IdentifierGenerator } from './identifier.js';
import { assertPositiveInteger } from './internal/validation.js';
import type { DispatcherOptions, RelayBusOptions, RetentionOptions } from './relay-bus.js';
import { createRetryPolicy } from './retry-policy.js';

// Option resolution of RelayBus: defaults and validation, in the order the options are read.

export const DEFAULTS = {
  concurrency: 10,
  pollInterval: '1s',
  leaseDuration: '30s',
  shutdownTimeout: '30s',
  completionBatchWindow: '2ms',
  completionBatchSize: 200,
  completedRetention: '7d',
  retentionInterval: '1h',
  routeTimeoutMs: 30_000,
  transactionTimeout: '5s',
} as const;

export function oneOf<T extends string>(
  value: T,
  { allowed, name }: { readonly allowed: readonly T[]; readonly name: string },
): T {
  if (!allowed.includes(value)) {
    throw new ConfigurationError(
      `${name} must be one of ${allowed.map((option) => `'${option}'`).join(', ')}, received '${String(value)}'`,
    );
  }
  return value;
}

export function isDispatcherEnabled<TTransaction>(options: RelayBusOptions<TTransaction>): boolean {
  return options.dispatcher?.enabled ?? true;
}

/** Milliseconds every `bus.transaction` may take. */
export function resolveTransactionTimeout<TTransaction>(
  options: RelayBusOptions<TTransaction>,
): number {
  return toTimerMilliseconds(
    options.transactionTimeout ?? DEFAULTS.transactionTimeout,
    'transactionTimeout',
  );
}

export function createHandlerDefaults<TTransaction>(
  options: RelayBusOptions<TTransaction>,
): RegistrationDefaults {
  return {
    retry: createRetryPolicy(options.retry),
    timeoutMs:
      options.handlerTimeout === undefined || options.handlerTimeout === false
        ? null
        : toTimerMilliseconds(options.handlerTimeout, 'handlerTimeout'),
  };
}

export function createDispatcherSettings(
  options: DispatcherOptions | undefined,
  generateIdentifier: IdentifierGenerator,
): DispatcherSettings {
  const dispatcher: DispatcherOptions = options ?? {};
  const concurrency = assertPositiveInteger(
    dispatcher.concurrency ?? DEFAULTS.concurrency,
    'dispatcher.concurrency',
  );
  return {
    owner: `${hostname()}:${process.pid}:${generateIdentifier()}`,
    concurrency,
    batchSize: assertPositiveInteger(dispatcher.batchSize ?? concurrency, 'dispatcher.batchSize'),
    ...createDispatcherTimings(dispatcher),
  };
}

/** The intervals and completion batching of the dispatcher, validated in declaration order. */
export function createDispatcherTimings(
  dispatcher: DispatcherOptions,
): Omit<DispatcherSettings, 'owner' | 'concurrency' | 'batchSize'> {
  return {
    pollIntervalMs: toTimerMilliseconds(
      dispatcher.pollInterval ?? DEFAULTS.pollInterval,
      'dispatcher.pollInterval',
    ),
    leaseDurationMs: toTimerMilliseconds(
      dispatcher.leaseDuration ?? DEFAULTS.leaseDuration,
      'dispatcher.leaseDuration',
    ),
    shutdownTimeoutMs: toTimerMilliseconds(
      dispatcher.shutdownTimeout ?? DEFAULTS.shutdownTimeout,
      'dispatcher.shutdownTimeout',
    ),
    completionBatchWindowMs: toMilliseconds(
      dispatcher.completionBatchWindow ?? DEFAULTS.completionBatchWindow,
      'dispatcher.completionBatchWindow',
    ),
    completionBatchSize: assertPositiveInteger(
      dispatcher.completionBatchSize ?? DEFAULTS.completionBatchSize,
      'dispatcher.completionBatchSize',
    ),
  };
}

export function createRetentionSettings(options: RetentionOptions | undefined): {
  completedMs: number | null;
  deadMs: number | null;
  intervalMs: number;
} {
  const completed = options?.completed ?? DEFAULTS.completedRetention;
  const dead = options?.dead ?? false;
  return {
    completedMs: completed === false ? null : toMilliseconds(completed, 'retention.completed'),
    deadMs: dead === false ? null : toMilliseconds(dead, 'retention.dead'),
    intervalMs: toTimerMilliseconds(
      options?.interval ?? DEFAULTS.retentionInterval,
      'retention.interval',
    ),
  };
}
