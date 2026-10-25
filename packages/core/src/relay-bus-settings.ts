import { hostname } from 'node:os';
import type { DispatcherSettings } from './dispatcher.js';
import { Milliseconds } from './values/milliseconds.js';
import { ConfigurationError } from './errors.js';
import type { RegistrationDefaults } from './handler-registry.js';
import type { IdentifierGenerator } from './identifier.js';
import { PositiveInteger } from './values/positive-integer.js';
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
/** How often the backlog is read for `onBacklog`, in milliseconds; undefined when off. */
export function resolveBacklogInterval<TTransaction>(
  options: RelayBusOptions<TTransaction>,
): number | undefined {
  return options.backlogInterval === undefined
    ? undefined
    : Milliseconds.forTimer(options.backlogInterval, 'backlogInterval');
}

export function resolveTransactionTimeout<TTransaction>(
  options: RelayBusOptions<TTransaction>,
): number {
  return Milliseconds.forTimer(
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
        : Milliseconds.forTimer(options.handlerTimeout, 'handlerTimeout'),
  };
}

export function createDispatcherSettings(
  options: DispatcherOptions | undefined,
  generateIdentifier: IdentifierGenerator,
): DispatcherSettings {
  const dispatcher: DispatcherOptions = options ?? {};
  const concurrency = PositiveInteger.of(
    dispatcher.concurrency ?? DEFAULTS.concurrency,
    'dispatcher.concurrency',
  );
  return {
    owner: `${hostname()}:${process.pid}:${generateIdentifier()}`,
    concurrency,
    batchSize: PositiveInteger.of(dispatcher.batchSize ?? concurrency, 'dispatcher.batchSize'),
    ...createDispatcherTimings(dispatcher),
  };
}

/** The intervals and completion batching of the dispatcher, validated in declaration order. */
export function createDispatcherTimings(
  dispatcher: DispatcherOptions,
): Omit<DispatcherSettings, 'owner' | 'concurrency' | 'batchSize'> {
  return {
    pollIntervalMs: Milliseconds.forTimer(
      dispatcher.pollInterval ?? DEFAULTS.pollInterval,
      'dispatcher.pollInterval',
    ),
    leaseDurationMs: Milliseconds.forTimer(
      dispatcher.leaseDuration ?? DEFAULTS.leaseDuration,
      'dispatcher.leaseDuration',
    ),
    shutdownTimeoutMs: Milliseconds.forTimer(
      dispatcher.shutdownTimeout ?? DEFAULTS.shutdownTimeout,
      'dispatcher.shutdownTimeout',
    ),
    completionBatchWindowMs: Milliseconds.of(
      dispatcher.completionBatchWindow ?? DEFAULTS.completionBatchWindow,
      'dispatcher.completionBatchWindow',
    ),
    completionBatchSize: PositiveInteger.of(
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
    completedMs: completed === false ? null : Milliseconds.of(completed, 'retention.completed'),
    deadMs: dead === false ? null : Milliseconds.of(dead, 'retention.dead'),
    intervalMs: Milliseconds.forTimer(
      options?.interval ?? DEFAULTS.retentionInterval,
      'retention.interval',
    ),
  };
}
