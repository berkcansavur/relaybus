import { vi } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  defineEvent,
  type RelayBusLogger,
  type RelayBusObserver,
  type RelayBusOptions,
  type InMemoryTransaction,
} from '@relaybus/core';
import { FakeClock } from '@relaybus/testing';

export class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
export class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

type LoggerMock = ReturnType<typeof vi.fn<(message: string) => void>>;

export function createLogger(): RelayBusLogger & {
  log: LoggerMock;
  warn: LoggerMock;
  error: LoggerMock;
} {
  return {
    log: vi.fn<(message: string) => void>(),
    warn: vi.fn<(message: string) => void>(),
    error: vi.fn<(message: string) => void>(),
  };
}

export function createObserver(): Required<RelayBusObserver> {
  return {
    onPublished: vi.fn(),
    onReceived: vi.fn(),
    onDeliveryStarted: vi.fn(),
    onDeliverySucceeded: vi.fn(),
    onDeliveryRetryScheduled: vi.fn(),
    onDeliveryDead: vi.fn(),
    onDeliveryReleased: vi.fn(),
    onLeaseLost: vi.fn(),
    onPurged: vi.fn(),
    onFailure: vi.fn(),
    onPublishFinished: vi.fn(),
    onClaimFinished: vi.fn(),
    onHandlerStarted: vi.fn(),
    onHandlerFinished: vi.fn(),
    onTransportSent: vi.fn(),
    onReceiveFinished: vi.fn(),
    onInvalidMessage: vi.fn(),
    onBacklog: vi.fn(),
  };
}

export interface TestBus {
  readonly bus: RelayBus<InMemoryTransaction>;
  readonly store: InMemoryEventStore;
  readonly clock: FakeClock;
  readonly logger: ReturnType<typeof createLogger>;
  readonly observer: Required<RelayBusObserver>;
}

export function createTestBus(
  options: Partial<RelayBusOptions<InMemoryTransaction>> = {},
): TestBus {
  const store =
    (options.store as InMemoryEventStore | undefined) ??
    new InMemoryEventStore({ retainCompleted: true });
  const clock = new FakeClock();
  const logger = createLogger();
  const observer = createObserver();
  const bus = new RelayBus<InMemoryTransaction>({
    store,
    clock,
    logger,
    random: () => 0.5,
    observers: [observer],
    ...options,
  });
  return { bus, store, clock, logger, observer };
}

export function deferred<T = void>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  return { promise, resolve, reject };
}

export async function waitFor(condition: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!condition()) {
    if (Date.now() - started > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}
