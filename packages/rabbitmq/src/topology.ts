import type { AmqpChannel } from './amqp-types.js';

/** Names of the broker objects a subscribing transport owns. */
export interface ConsumerTopology {
  readonly exchange: string;
  readonly queue: string;
  readonly deadLetterExchange: string;
  readonly deadLetterQueue: string;
  readonly deliveryLimit: number;
  /** Single active consumer, so the queue is consumed strictly in order. */
  readonly ordered: boolean;
  /** TTLs of the retry queues, ascending; empty when ordered. */
  readonly retryDelaysMs: readonly number[];
  /** Routing-key patterns the queue is bound with. */
  readonly destinations: readonly string[];
}

/** Name of the retry queue that holds messages for `delayMs` before returning them to `queue`. */
export function retryQueueName(queue: string, delayMs: number): string {
  return `${queue}.retry.${delayMs}`;
}

/** Declares the durable topic exchange events are published to. Idempotent. */
export async function assertExchange(channel: AmqpChannel, exchange: string): Promise<void> {
  await channel.assertExchange(exchange, 'topic', { durable: true });
}

/**
 * Declares the consumer side, all durable quorum queues and idempotent:
 *
 * - the main queue with a dead-letter exchange, `x-dead-letter-strategy`
 *   `at-least-once` (which requires `x-overflow` `reject-publish`),
 *   `x-delivery-limit` as a backstop for crash loops, and optionally
 *   `x-single-active-consumer`; bound to the topic exchange once per destination;
 * - one retry queue per delay tier: a queue-level `x-message-ttl`, expired
 *   messages dead-lettered through the default exchange back to the main queue;
 * - a direct dead-letter exchange and the dead-letter queue, bound with the main
 *   queue's name as routing key.
 *
 * Declaring an existing queue with different arguments makes the broker close
 * the channel (`PRECONDITION_FAILED`); see the README for migrating a queue
 * created by an older version or with other options.
 */
export async function assertConsumerTopology(
  channel: AmqpChannel,
  topology: ConsumerTopology,
): Promise<void> {
  await assertExchange(channel, topology.exchange);
  await channel.assertExchange(topology.deadLetterExchange, 'direct', { durable: true });
  await channel.assertQueue(topology.deadLetterQueue, {
    durable: true,
    arguments: { 'x-queue-type': 'quorum' },
  });
  await channel.bindQueue(topology.deadLetterQueue, topology.deadLetterExchange, topology.queue);
  for (const delayMs of topology.retryDelaysMs) {
    await channel.assertQueue(retryQueueName(topology.queue, delayMs), {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-message-ttl': delayMs,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': topology.queue,
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
      },
    });
  }
  await channel.assertQueue(topology.queue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-delivery-limit': topology.deliveryLimit,
      'x-dead-letter-exchange': topology.deadLetterExchange,
      'x-dead-letter-routing-key': topology.queue,
      'x-dead-letter-strategy': 'at-least-once',
      'x-overflow': 'reject-publish',
      ...(topology.ordered ? { 'x-single-active-consumer': true } : {}),
    },
  });
  for (const destination of topology.destinations) {
    await channel.bindQueue(topology.queue, topology.exchange, destination);
  }
}
