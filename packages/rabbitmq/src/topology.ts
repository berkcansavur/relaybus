import type { AmqpChannel } from './amqp-types.js';

/** Names of the broker objects a subscribing transport owns. */
export interface ConsumerTopology {
  readonly exchange: string;
  readonly queue: string;
  readonly deadLetterExchange: string;
  readonly deadLetterQueue: string;
  readonly deliveryLimit: number;
  /** Routing-key patterns the queue is bound with. */
  readonly destinations: readonly string[];
}

/** Declares the durable topic exchange events are published to. Idempotent. */
export async function assertExchange(channel: AmqpChannel, exchange: string): Promise<void> {
  await channel.assertExchange(exchange, 'topic', { durable: true });
}

/**
 * Declares the consumer side, all durable and idempotent:
 *
 * - the main quorum queue with `x-delivery-limit` and a dead-letter exchange,
 *   bound to the topic exchange once per destination;
 * - a direct dead-letter exchange and a quorum dead-letter queue, bound with the
 *   main queue's name as routing key.
 *
 * Declaring an existing queue with different arguments makes the broker close
 * the channel (`PRECONDITION_FAILED`); change `deliveryLimit` or the dead-letter
 * names by declaring a new queue or by deleting the old one.
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
  await channel.assertQueue(topology.queue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-delivery-limit': topology.deliveryLimit,
      'x-dead-letter-exchange': topology.deadLetterExchange,
      'x-dead-letter-routing-key': topology.queue,
    },
  });
  for (const destination of topology.destinations) {
    await channel.bindQueue(topology.queue, topology.exchange, destination);
  }
}
