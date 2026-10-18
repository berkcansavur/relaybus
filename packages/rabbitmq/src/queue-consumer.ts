import {
  computeRetryDelay,
  isRetryableError,
  toErrorInfo,
  type EventEnvelope,
  type RelayBusLogger,
  type RetryPolicy,
  type TransportMessageHandler,
} from '@relaybus/core';
import { abortableSleep } from './abortable-sleep.js';
import type { AmqpChannel, AmqpMessage } from './amqp-types.js';
import { decodeEnvelope } from './wire-format.js';

export interface QueueConsumerSettings {
  readonly queue: string;
  readonly prefetch: number;
  /** Deliveries after which a failing message is dead-lettered instead of requeued. */
  readonly deliveryLimit: number;
  readonly handlerRetry: RetryPolicy;
  readonly shutdownTimeoutMs: number;
  readonly onMessage: TransportMessageHandler;
  /** The consumer's channel closed or the broker cancelled the consumer. */
  readonly onConsumerLost: (reason: string) => void;
  readonly logger: RelayBusLogger;
  readonly random: () => number;
}

type Settlement = 'ack' | 'requeue' | 'dead-letter';

/**
 * Consumes one queue with manual acknowledgements.
 *
 * A message is acknowledged only after the handler resolved. When the handler
 * fails it is retried on the same delivery with backoff, so a failing message
 * cannot hot-loop through the broker; once those attempts are spent the message
 * is requeued, and the quorum queue's delivery limit eventually dead-letters it.
 * Malformed messages and non-retryable errors are dead-lettered immediately.
 * Nothing is ever acknowledged without having been handled or dead-lettered.
 */
export class QueueConsumer {
  readonly #settings: QueueConsumerSettings;
  readonly #inFlight = new Set<Promise<void>>();
  readonly #stopping = new AbortController();
  #channel: AmqpChannel | undefined;
  #consumerTag: string | undefined;

  constructor(settings: QueueConsumerSettings) {
    this.#settings = settings;
  }

  /** Starts consuming on a channel whose topology is already declared. */
  async attach(channel: AmqpChannel): Promise<void> {
    channel.on('error', (error: Error) => {
      this.#settings.logger.warn(
        `[QueueConsumer](attach) Channel error | queue=${this.#settings.queue} error=${error.message}`,
      );
    });
    channel.on('close', () => {
      if (this.#channel === channel) {
        this.#channel = undefined;
        this.#consumerTag = undefined;
        this.#settings.onConsumerLost('consumer channel closed');
      }
    });
    await channel.prefetch(this.#settings.prefetch);
    const { consumerTag } = await channel.consume(
      this.#settings.queue,
      (message) => this.#receive(channel, message),
      { noAck: false },
    );
    this.#channel = channel;
    this.#consumerTag = consumerTag;
  }

  /** Forgets the channel without closing it, because its connection is gone. */
  detach(): void {
    this.#channel = undefined;
    this.#consumerTag = undefined;
  }

  /**
   * Stops taking messages, aborts pending handler backoff (those messages are
   * requeued), waits up to the shutdown timeout for running handlers, then
   * closes the channel. Unacknowledged messages return to the queue.
   */
  async stop(): Promise<void> {
    this.#stopping.abort();
    const channel = this.#channel;
    const consumerTag = this.#consumerTag;
    this.#channel = undefined;
    this.#consumerTag = undefined;
    if (channel !== undefined && consumerTag !== undefined) {
      await channel.cancel(consumerTag).catch((error: unknown) => {
        this.#settings.logger.warn(
          `[QueueConsumer](stop) Could not cancel consumer | queue=${this.#settings.queue} error=${toErrorInfo(error).message}`,
        );
      });
    }
    await this.#awaitInFlight();
    await channel?.close().catch(() => undefined);
  }

  async #awaitInFlight(): Promise<void> {
    if (this.#inFlight.size === 0) {
      return;
    }
    const timeout = new AbortController();
    const timedOut = abortableSleep(this.#settings.shutdownTimeoutMs, timeout.signal);
    await Promise.race([Promise.allSettled([...this.#inFlight]), timedOut]);
    timeout.abort();
  }

  #receive(channel: AmqpChannel, message: AmqpMessage | null): void {
    if (message === null) {
      this.#settings.onConsumerLost('consumer cancelled by the broker');
      return;
    }
    const task: Promise<void> = this.#process(channel, message).finally(() => {
      this.#inFlight.delete(task);
    });
    this.#inFlight.add(task);
  }

  async #process(channel: AmqpChannel, message: AmqpMessage): Promise<void> {
    const { queue, handlerRetry } = this.#settings;
    let envelope: EventEnvelope;
    try {
      envelope = decodeEnvelope(message);
    } catch (error) {
      this.#settings.logger.error(
        `[QueueConsumer](process) Dead-lettering malformed message | queue=${queue} routingKey=${message.fields.routingKey} error=${toErrorInfo(error).message}`,
      );
      this.#settle(channel, message, 'dead-letter');
      return;
    }
    for (let attempt = 1; ; attempt += 1) {
      try {
        await this.#settings.onMessage(envelope);
        this.#settle(channel, message, 'ack');
        return;
      } catch (error) {
        const failure = `queue=${queue} eventId=${envelope.id} attempt=${attempt} error=${toErrorInfo(error).message}`;
        if (!isRetryableError(handlerRetry, error)) {
          this.#settings.logger.error(
            `[QueueConsumer](process) Dead-lettering message the handler cannot process | ${failure}`,
          );
          this.#settle(channel, message, 'dead-letter');
          return;
        }
        if (attempt >= handlerRetry.attempts) {
          this.#settings.logger.warn(
            `[QueueConsumer](process) Handler attempts spent | ${failure} deliveries=${previousDeliveries(message) + 1} deliveryLimit=${this.#settings.deliveryLimit}`,
          );
          this.#settle(channel, message, this.#exhausted(message) ? 'dead-letter' : 'requeue');
          return;
        }
        const delayMs = computeRetryDelay(handlerRetry, attempt, this.#settings.random);
        this.#settings.logger.warn(
          `[QueueConsumer](process) Handler failed, retrying | ${failure} delayMs=${delayMs}`,
        );
        await abortableSleep(delayMs, this.#stopping.signal);
        if (this.#stopping.signal.aborted) {
          this.#settle(channel, message, 'requeue');
          return;
        }
      }
    }
  }

  /**
   * The broker's own `x-delivery-limit` does not count `nack(requeue)` from
   * AMQP 0-9-1 consumers on RabbitMQ 4, so the consumer enforces the limit from
   * the delivery counters the broker stamps on redelivered messages.
   */
  #exhausted(message: AmqpMessage): boolean {
    return previousDeliveries(message) + 1 >= this.#settings.deliveryLimit;
  }

  #settle(channel: AmqpChannel, message: AmqpMessage, settlement: Settlement): void {
    try {
      if (settlement === 'ack') {
        channel.ack(message);
      } else {
        channel.nack(message, false, settlement === 'requeue');
      }
    } catch (error) {
      // The channel is gone; the broker redelivers the message and the inbox drops the duplicate.
      this.#settings.logger.warn(
        `[QueueConsumer](settle) Could not ${settlement} message, the broker will redeliver it | queue=${this.#settings.queue} error=${toErrorInfo(error).message}`,
      );
    }
  }
}

/** Earlier deliveries of a message: `x-delivery-count` (RabbitMQ 3.x) or `x-acquired-count` (4.x). */
function previousDeliveries(message: AmqpMessage): number {
  // A message that decoded has headers: `cloudEvents:specversion` is required.
  const headers = message.properties.headers!;
  const counts = [headers['x-delivery-count'], headers['x-acquired-count']];
  return Math.max(0, ...counts.filter((count): count is number => typeof count === 'number'));
}
