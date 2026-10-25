import { vi, type Mock } from 'vitest';
import { createRetryPolicy, type EventEnvelope } from '@relaybus/core';
import type { AmqpMessage } from '../../src/amqp-types.js';
import { QueueConsumer, type ConfirmedRepublish } from '../../src/queue-consumer.js';
import { decodeEnvelope } from '../../src/wire-format.js';
import { FakeChannel, createLogger } from '../fakes.js';

export const RETRY = createRetryPolicy({
  attempts: 3,
  initialDelay: 5,
  maxDelay: 5,
  jitter: 'none',
});
export const REDELIVERY = createRetryPolicy(
  { attempts: Infinity, initialDelay: 100, maxDelay: 400, factor: 2, jitter: 'none' },
  { ...RETRY, attempts: Infinity },
);

interface SetupOverrides {
  handlerRetry?: typeof RETRY;
  redelivery?: typeof REDELIVERY;
  retryLadder?: readonly number[];
  redeliveryMaxAgeMs?: number;
  ordered?: boolean;
  shutdownTimeoutMs?: number;
  republish?: ConfirmedRepublish;
  decode?: (message: AmqpMessage) => EventEnvelope;
  onInvalidMessage?: (message: { destination: string; error: unknown }) => void;
}

export interface ConsumerSetup {
  readonly consumer: QueueConsumer;
  readonly handler: Mock<(envelope: EventEnvelope) => Promise<void>>;
  readonly logger: ReturnType<typeof createLogger>;
  readonly onConsumerLost: Mock;
  readonly republish: Mock<ConfirmedRepublish>;
  readonly channel: FakeChannel;
}

export function setup(
  onMessage: (envelope: EventEnvelope) => Promise<void> = async () => undefined,
  overrides: SetupOverrides = {},
): ConsumerSetup {
  const logger = createLogger();
  const onConsumerLost = vi.fn();
  const handler = vi.fn(onMessage);
  const republish = vi.fn<ConfirmedRepublish>(overrides.republish ?? (async () => undefined));
  const consumer = new QueueConsumer({
    queue: 'relaybus.billing',
    prefetch: 7,
    handlerRetry: overrides.handlerRetry ?? RETRY,
    redelivery: overrides.redelivery ?? REDELIVERY,
    retryLadder: overrides.retryLadder ?? [100, 200, 400],
    redeliveryMaxAgeMs: overrides.redeliveryMaxAgeMs ?? 10_000,
    ordered: overrides.ordered ?? false,
    deadLetterExchange: 'relaybus.dead',
    shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 1_000,
    onMessage: handler,
    decode: overrides.decode ?? decodeEnvelope,
    ...(overrides.onInvalidMessage === undefined
      ? {}
      : { onInvalidMessage: overrides.onInvalidMessage }),
    onConsumerLost,
    republish,
    logger,
    random: () => 0.5,
  });
  return { consumer, handler, logger, onConsumerLost, republish, channel: new FakeChannel() };
}

export const failing = async (): Promise<void> => {
  throw new Error('database down');
};

export function withHeaders(message: AmqpMessage, headers: Record<string, unknown>): AmqpMessage {
  return {
    ...message,
    properties: { ...message.properties, headers: { ...message.properties.headers, ...headers } },
  };
}
