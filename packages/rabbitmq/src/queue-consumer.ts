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
import type { AmqpChannel, AmqpMessage, AmqpPublishOptions } from './amqp-types.js';
import { RabbitMqUnavailableError } from './errors.js';
import {
  DEAD_LETTER_ERROR_CODE_HEADER,
  DEAD_LETTER_ERROR_NAME_HEADER,
  DEAD_LETTER_REASON_HEADER,
  FIRST_FAILURE_HEADER,
  ORIGINAL_ROUTING_KEY_HEADER,
  REDELIVERIES_HEADER,
  chooseRetryTier,
  copyForRepublish,
  originalRoutingKey,
  readCounterHeader,
  withOriginalRoutingKey,
} from './redelivery.js';
import { retryQueueName } from './topology.js';

/** Publishes a message and resolves only after the broker confirmed it was routed to a queue. */
export type ConfirmedRepublish = (
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: AmqpPublishOptions,
) => Promise<void>;

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
  /** Turns a message into an envelope; anything it throws dead-letters the message as malformed. */
  readonly decode: (message: AmqpMessage) => EventEnvelope;
  /** The consumer's channel closed or the broker cancelled the consumer. */
  readonly onConsumerLost: (reason: string) => void;
  /** Publishes to another queue on a confirm channel; rejects when the copy is not confirmed. */
  readonly republish: ConfirmedRepublish;
  readonly logger: RelayBusLogger;
  readonly random: () => number;
}

type Settlement = 'ack' | 'requeue' | 'dead-letter';

/** A message being processed, with the redelivery state its retries carry. */
type ProcessedMessage = {
  readonly channel: AmqpChannel;
  readonly message: AmqpMessage;
  readonly envelope: EventEnvelope;
  redeliveries: number;
  firstFailureAt: number | undefined;
};

/** A message to park in a retry queue, with the retry state its copy carries. */
type RedeliverLaterInput = {
  readonly message: AmqpMessage;
  readonly delayMs: number;
  readonly redeliveries: number;
  readonly firstFailureAt: number;
};

/** A message to move to the dead-letter queue and the error that sent it there. */
type DeadLetterInput = {
  readonly message: AmqpMessage;
  readonly reason: string;
  readonly error: unknown;
};

/**
 * Consumes one queue with manual acknowledgements.
 *
 * A message is acknowledged only after the handler resolved. When the handler
 * fails it is first retried on the same delivery with short backoff
 * (`handlerRetry`). If that does not help, a copy goes to the retry queue whose
 * TTL matches the next backoff step, and the original is acknowledged only
 * after the broker confirmed the copy; the copy returns to the main queue when
 * its TTL expires. This waits out a long outage of the handler's dependency.
 *
 * Malformed messages, non-retryable errors and messages that kept failing for
 * `redeliveryMaxAgeMs` go to the dead-letter exchange through a confirmed copy
 * (carrying the reason, without retry state, so a replay starts fresh) and the
 * original is acknowledged afterwards. If a copy cannot be confirmed the
 * original is requeued or rejected without requeue, which makes the broker
 * dead-letter it; it is never acknowledged unprocessed, so nothing is lost.
 *
 * In ordered mode nothing is moved: the failing message is retried in place with
 * the same backoff, holding the queue.
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

  /** True while a consumer is registered on a live channel. */
  isActive(): boolean {
    return this.#consumerTag !== undefined;
  }

  /**
   * Starts consuming on a channel whose topology is already declared.
   *
   * @throws {RabbitMqUnavailableError} the channel closed while it was being set up
   */
  async attach(channel: AmqpChannel): Promise<void> {
    let closedDuringSetup = false;
    channel.on('error', (error: Error) => {
      this.#settings.logger.warn(
        `[QueueConsumer](attach) Channel error | queue=${this.#settings.queue} error=${error.message}`,
      );
    });
    channel.on('close', () => {
      closedDuringSetup = true;
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
    if (closedDuringSetup) {
      throw new RabbitMqUnavailableError(
        `[QueueConsumer](attach) Consumer channel closed while the consumer was being set up | queue=${this.#settings.queue}`,
      );
    }
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
      this.#consumerTag = undefined;
      this.#settings.onConsumerLost('consumer cancelled by the broker');
      return;
    }
    const task: Promise<void> = this.#process(channel, message).finally(() => {
      this.#inFlight.delete(task);
    });
    this.#inFlight.add(task);
  }

  async #process(channel: AmqpChannel, message: AmqpMessage): Promise<void> {
    const decoded = await this.#decodeOrDeadLetter(channel, message);
    if (decoded === undefined) {
      return;
    }
    const { envelope } = decoded;
    const delivery: ProcessedMessage = {
      channel,
      message,
      envelope,
      redeliveries: readCounterHeader(message, REDELIVERIES_HEADER) ?? 0,
      firstFailureAt: readCounterHeader(message, FIRST_FAILURE_HEADER),
    };
    for (let attempt = 1; ; attempt += 1) {
      try {
        await this.#settings.onMessage(envelope);
        this.#settle(channel, message, 'ack');
        return;
      } catch (error) {
        if ((await this.#afterFailure(delivery, attempt, error)) === 'settled') {
          return;
        }
      }
    }
  }

  /** The event of `message`, or undefined after dead-lettering a message that is not one. */
  async #decodeOrDeadLetter(
    channel: AmqpChannel,
    message: AmqpMessage,
  ): Promise<{ readonly envelope: EventEnvelope } | undefined> {
    try {
      return { envelope: this.#settings.decode(withOriginalRoutingKey(message)) };
    } catch (error) {
      this.#settings.logger.error(
        `[QueueConsumer](process) Dead-lettering malformed message | queue=${this.#settings.queue} routingKey=${message.fields.routingKey} ${describeError(error)}`,
      );
      await this.#deadLetter(channel, { message, reason: 'malformed message', error });
      return undefined;
    }
  }

  /**
   * Decides what follows a failed attempt: dead-letter a non-retryable error, retry on the same
   * delivery while local attempts remain, else redeliver later. `retry` runs the handler again.
   */
  async #afterFailure(
    delivery: ProcessedMessage,
    attempt: number,
    error: unknown,
  ): Promise<'retry' | 'settled'> {
    const { channel, message, envelope } = delivery;
    const { queue, handlerRetry } = this.#settings;
    const failure = `queue=${queue} eventId=${envelope.id} attempt=${attempt} ${describeError(error)}`;
    if (!isRetryableError(handlerRetry, error)) {
      this.#settings.logger.error(
        `[QueueConsumer](process) Dead-lettering message the handler cannot process | ${failure}`,
      );
      await this.#deadLetter(channel, {
        message,
        reason: 'non-retryable handler error',
        error,
      });
      return 'settled';
    }
    if (attempt < handlerRetry.attempts) {
      const delayMs = computeRetryDelay(handlerRetry, attempt, this.#settings.random);
      this.#settings.logger.warn(
        `[QueueConsumer](process) Handler failed, retrying | ${failure} delayMs=${delayMs}`,
      );
      return this.#waitOrRequeue(delivery, delayMs);
    }
    return this.#afterSpentAttempts(delivery, failure, error);
  }

  /**
   * The local attempts are spent: dead-letter a message that kept failing past `redeliveryMaxAgeMs`,
   * else park it in a retry queue, or, when ordered, wait and retry it in place.
   */
  async #afterSpentAttempts(
    delivery: ProcessedMessage,
    failure: string,
    error: unknown,
  ): Promise<'retry' | 'settled'> {
    const { channel, message } = delivery;
    delivery.redeliveries += 1;
    const now = Date.now();
    delivery.firstFailureAt ??= now;
    const { redeliveries, firstFailureAt } = delivery;
    const delayMs = this.#redeliveryDelay(redeliveries);
    if (now + delayMs - firstFailureAt > this.#settings.redeliveryMaxAgeMs) {
      this.#settings.logger.error(
        `[QueueConsumer](process) Dead-lettering message that kept failing | ${failure} redeliveries=${redeliveries - 1} failingForMs=${now - firstFailureAt} maxAgeMs=${this.#settings.redeliveryMaxAgeMs}`,
      );
      await this.#deadLetter(channel, {
        message,
        reason: 'redelivery window exhausted',
        error,
      });
      return 'settled';
    }
    this.#settings.logger.warn(
      `[QueueConsumer](process) Handler attempts spent, retrying later | ${failure} redeliveries=${redeliveries} delayMs=${delayMs}`,
    );
    if (!this.#settings.ordered) {
      await this.#redeliverLater(channel, { message, delayMs, redeliveries, firstFailureAt });
      return 'settled';
    }
    return this.#waitOrRequeue(delivery, delayMs);
  }

  /** Waits `delayMs` before the next attempt; requeues instead when the consumer stops meanwhile. */
  async #waitOrRequeue(delivery: ProcessedMessage, delayMs: number): Promise<'retry' | 'settled'> {
    if (await this.#interrupted(delayMs)) {
      this.#settle(delivery.channel, delivery.message, 'requeue');
      return 'settled';
    }
    return 'retry';
  }

  /** The backoff before the next delayed redelivery: a retry queue's TTL, or an exact wait when ordered. */
  #redeliveryDelay(redeliveries: number): number {
    const { redelivery, retryLadder, ordered, random } = this.#settings;
    const delayMs = computeRetryDelay(redelivery, redeliveries, random);
    return ordered ? delayMs : chooseRetryTier(retryLadder, delayMs, random);
  }

  /** Sleeps; true when the consumer is stopping, so the caller must give the message back. */
  async #interrupted(delayMs: number): Promise<boolean> {
    await abortableSleep(delayMs, this.#stopping.signal);
    return this.#stopping.signal.aborted;
  }

  /**
   * Parks a copy in the retry queue of `delayMs` and acknowledges the original
   * only once the broker confirmed the copy. When the copy cannot be confirmed
   * the original is requeued after the same delay, so it is neither lost nor
   * hot-looped.
   */
  async #redeliverLater(channel: AmqpChannel, input: RedeliverLaterInput): Promise<void> {
    const { message, delayMs, redeliveries, firstFailureAt } = input;
    const { content, options } = copyForRepublish(message, {
      [REDELIVERIES_HEADER]: String(redeliveries),
      [FIRST_FAILURE_HEADER]: String(firstFailureAt),
      [ORIGINAL_ROUTING_KEY_HEADER]: originalRoutingKey(message),
    });
    try {
      await this.#settings.republish(
        '',
        retryQueueName(this.#settings.queue, delayMs),
        content,
        options,
      );
    } catch (error) {
      this.#settings.logger.warn(
        `[QueueConsumer](redeliverLater) Could not park the message in the retry queue, requeueing it | queue=${this.#settings.queue} delayMs=${delayMs} error=${toErrorInfo(error).message}`,
      );
      await this.#interrupted(delayMs);
      this.#settle(channel, message, 'requeue');
      return;
    }
    this.#settle(channel, message, 'ack');
  }

  /**
   * Moves the message to the dead-letter queue through a confirmed copy that
   * carries the reason and no retry state. If the copy cannot be confirmed the
   * message is rejected without requeue, which makes the broker dead-letter it
   * (at-least-once), so it still ends up in the dead-letter queue.
   */
  async #deadLetter(channel: AmqpChannel, input: DeadLetterInput): Promise<void> {
    const { message, reason, error } = input;
    const { name, code } = toErrorInfo(error);
    const { content, options } = copyForRepublish(message, {
      [DEAD_LETTER_REASON_HEADER]: reason,
      [DEAD_LETTER_ERROR_NAME_HEADER]: name,
      ...(code === undefined ? {} : { [DEAD_LETTER_ERROR_CODE_HEADER]: code }),
    });
    try {
      await this.#settings.republish(
        this.#settings.deadLetterExchange,
        this.#settings.queue,
        content,
        options,
      );
    } catch (error) {
      this.#settings.logger.warn(
        `[QueueConsumer](deadLetter) Could not publish to the dead-letter exchange, leaving it to the broker | queue=${this.#settings.queue} error=${toErrorInfo(error).message}`,
      );
      this.#settle(channel, message, 'dead-letter');
      return;
    }
    this.#settle(channel, message, 'ack');
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

/** Log fields of an error: its message, class name and, for relaybus errors, its code. */
function describeError(error: unknown): string {
  const { message, name, code } = toErrorInfo(error);
  return `error=${message} errorName=${name}${code === undefined ? '' : ` code=${code}`}`;
}
