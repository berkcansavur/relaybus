import { vi } from 'vitest';
import {
  type EventEnvelope,
  type TransportMessage,
  type TransportSubscriptionHooks,
} from '@relaybus/core';
import { KafkaTransport, type KafkaTransportOptions } from '../../src/index.js';
import { FakeKafka, createLogger } from '../fakes.js';

export const FAST = { maxAttempts: 3, initialBackoff: 1, maxBackoff: 2 } as const;
export const FAST_RESTART = { initialBackoff: 1, maxBackoff: 2 } as const;

export function setup(options: Partial<KafkaTransportOptions> = {}) {
  const kafka = new FakeKafka();
  const logger = createLogger();
  const transport = new KafkaTransport({
    kafka,
    groupId: 'billing',
    logger,
    processing: FAST,
    restart: FAST_RESTART,
    ...options,
  });
  return { kafka, logger, transport, producer: kafka.producer_, consumer: kafka.consumer_ };
}

export async function subscribed(
  options: Partial<KafkaTransportOptions> = {},
  hooks?: TransportSubscriptionHooks,
) {
  const context = setup(options);
  const received: EventEnvelope[] = [];
  const onMessage = vi.fn(async (envelope: EventEnvelope, _message?: TransportMessage) => {
    received.push(envelope);
  });
  await context.transport.subscribe(['orders'], onMessage, hooks);
  return { ...context, received, onMessage };
}
