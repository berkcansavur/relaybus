import type {
  EventEnvelope,
  RelayBusLogger,
  RetryPolicy,
  TransportMessageHandler,
  TransportSubscriptionHooks,
} from '@relaybus/core';
import type { AmqpChannel, AmqpMessage, AmqpPublishOptions } from './amqp-types.js';

/** Publishes a message and resolves only after the broker confirmed it was routed to a queue. */
export type ConfirmedRepublish = (
  destination: { readonly exchange: string; readonly routingKey: string },
  copy: { readonly content: Buffer; readonly options: AmqpPublishOptions },
) => Promise<void>;

/** What a {@link QueueConsumer} consumes, how it retries and dead-letters, and whom it tells. */
export interface QueueConsumerSettings {
  readonly queue: string;
  readonly prefetch: number;
  readonly handlerRetry: RetryPolicy;
  /** Backoff between delayed redeliveries. */
  readonly redelivery: RetryPolicy;
  /** Delays of the retry queues, ascending. Unused when ordered. */
  readonly retryLadder: readonly number[];
  /** A message failing for longer than this is dead-lettered. */
  readonly redeliveryMaxAgeMs: number;
  /** Retry in place instead of through the retry queues, so order is kept. */
  readonly ordered: boolean;
  readonly deadLetterExchange: string;
  readonly shutdownTimeoutMs: number;
  readonly onMessage: TransportMessageHandler;
  /** Told of each message `decode` rejected; undefined when nobody observes them. */
  readonly onInvalidMessage?: TransportSubscriptionHooks['onInvalidMessage'];
  /** Turns a message into an envelope; anything it throws dead-letters the message as malformed. */
  readonly decode: (message: AmqpMessage) => EventEnvelope;
  /** The consumer's channel closed or the broker cancelled the consumer. */
  readonly onConsumerLost: (reason: string) => void;
  /** Publishes to another queue on a confirm channel; rejects when the copy is not confirmed. */
  readonly republish: ConfirmedRepublish;
  readonly logger: RelayBusLogger;
  readonly random: () => number;
}

/** A message being processed, with the redelivery state its retries carry. */
export type ProcessedMessage = {
  readonly channel: AmqpChannel;
  readonly message: AmqpMessage;
  readonly envelope: EventEnvelope;
  redeliveries: number;
  firstFailureAt: number | undefined;
};

/** A message to park in a retry queue, with the retry state its copy carries. */
export type RedeliverLaterInput = {
  readonly message: AmqpMessage;
  readonly delayMs: number;
  readonly redeliveries: number;
  readonly firstFailureAt: number;
};

/** A message to move to the dead-letter queue and the error that sent it there. */
export type DeadLetterInput = {
  readonly message: AmqpMessage;
  readonly reason: string;
  readonly error: unknown;
};
