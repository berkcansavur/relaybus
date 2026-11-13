import type { ErrorInfo } from './errors.js';
import type { EventEnvelope } from './event.js';
import type { ClaimedDelivery } from './event-store.js';
import type { RelayBusLogger, RelayBusObserver } from './observer.js';

export type LoggedEvent = 'committed' | 'succeeded' | 'retry' | 'dead';

type LogLevel = keyof RelayBusLogger;

export interface LoggingObserverFormat {
  readonly committed?: (eventEnvelope: EventEnvelope) => string | undefined;
  readonly succeeded?: (event: {
    readonly delivery: ClaimedDelivery;
    readonly durationMs: number;
  }) => string | undefined;
  readonly retry?: (event: {
    readonly delivery: ClaimedDelivery;
    readonly error: ErrorInfo;
    readonly availableAt: Date;
  }) => string | undefined;
  readonly dead?: (event: {
    readonly delivery: ClaimedDelivery;
    readonly error: ErrorInfo;
  }) => string | undefined;
}

export interface LoggingObserverOptions {
  readonly logger: RelayBusLogger;
  readonly events?: readonly LoggedEvent[];
  readonly format?: LoggingObserverFormat;
}

const deliveryFields = (claimedDelivery: ClaimedDelivery): string =>
  `handler=${claimedDelivery.handler} | eventId=${claimedDelivery.event.id} | eventType=${claimedDelivery.event.type} | attempt=${claimedDelivery.attempt}`;

export const defaultLoggingFormat: Required<LoggingObserverFormat> = {
  committed: (eventEnvelope) =>
    `[relaybus](onPublishCommitted) Event committed | eventId=${eventEnvelope.id} | eventType=${eventEnvelope.type} | correlationId=${String(eventEnvelope.correlationId)}`,
  succeeded: ({ delivery, durationMs }) =>
    `[relaybus](onDeliverySucceeded) Delivery succeeded | ${deliveryFields(delivery)} | durationMs=${durationMs}`,
  retry: ({ delivery, error, availableAt }) =>
    `[relaybus](onDeliveryRetryScheduled) Delivery failed, retry scheduled | ${deliveryFields(delivery)} | availableAt=${availableAt.toISOString()} | error=${error.message}`,
  dead: ({ delivery, error }) =>
    `[relaybus](onDeliveryDead) Delivery dead-lettered | ${deliveryFields(delivery)} | error=${error.message}`,
};

export function createLoggingObserver({
  logger,
  events = ['retry', 'dead'],
  format = {},
}: LoggingObserverOptions): RelayBusObserver {
  const enabled = new Set(events);
  const lines: Required<LoggingObserverFormat> = { ...defaultLoggingFormat, ...format };
  const write = (logLevel: LogLevel, line: string | undefined): void => {
    if (line !== undefined) {
      logger[logLevel](line);
    }
  };
  return {
    ...(enabled.has('committed')
      ? {
          onPublishCommitted: ({ envelopes }) =>
            writeEach(envelopes, (eventEnvelope) => write('log', lines.committed(eventEnvelope))),
        }
      : {}),
    ...(enabled.has('succeeded')
      ? { onDeliverySucceeded: (event) => write('log', lines.succeeded(event)) }
      : {}),
    ...(enabled.has('retry')
      ? { onDeliveryRetryScheduled: (event) => write('warn', lines.retry(event)) }
      : {}),
    ...(enabled.has('dead')
      ? { onDeliveryDead: (event) => write('error', lines.dead(event)) }
      : {}),
  };
}

function writeEach<T>(items: readonly T[], writeOne: (item: T) => void): void {
  const failures: unknown[] = [];
  for (const item of items) {
    try {
      writeOne(item);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length > 0) {
    throw failures[0];
  }
}
