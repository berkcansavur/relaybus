import { systemClock, toErrorInfo } from '@relaybus/core';
import { schedule, type CancelTimer } from '@relaybus/core/internal';
import type { AmqpConfirmChannel, AmqpPublishOptions } from './amqp-types.js';
import {
  RabbitMqPublishRejectedError,
  RabbitMqUnavailableError,
  UnroutableMessageError,
} from './errors.js';

/** Publisher channel plus the broker returns not yet matched with their confirm. */
export interface Publisher {
  readonly channel: AmqpConfirmChannel;
  readonly unroutable: Map<string, number>;
  /** Set when the channel closed, also while the transport was still being set up. */
  closed: boolean;
}

/** One message to publish and wait for the broker's confirm. */
export type PublishConfirmedInput = {
  readonly exchange: string;
  readonly routingKey: string;
  readonly content: Buffer;
  readonly options: AmqpPublishOptions;
  /** Ask the broker to return the message when no queue is bound to its routing key. */
  readonly mandatory: boolean;
};

/** amqplib reports a negative acknowledgement with this exact message. */
const NACK_MESSAGE = 'message nacked';

/** The event and destination a publish error names. */
type PublishTarget = { readonly eventId: string; readonly destination: string };

/** How one pending publish settles. */
type PendingConfirm = PublishTarget & {
  readonly publisher: Publisher;
  readonly returnKey: string;
  readonly cancelTimeout: CancelTimer;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
};

/**
 * Publishes on a confirm channel and resolves only after the broker confirmed the message was
 * persisted and routed to a queue; rejects when not connected, nacked, unroutable or unconfirmed
 * within the publish timeout. It never drops a message silently.
 */
export class ConfirmedPublishing {
  readonly #transportName: string;
  readonly #publishTimeoutMs: number;

  constructor(transportName: string, publishTimeoutMs: number) {
    this.#transportName = transportName;
    this.#publishTimeoutMs = publishTimeoutMs;
  }

  async publish(publisher: Publisher | undefined, input: PublishConfirmedInput): Promise<void> {
    const { exchange, routingKey, content, options, mandatory } = input;
    const eventId = options.messageId ?? 'unknown';
    if (publisher === undefined) {
      throw new RabbitMqUnavailableError(
        `[RabbitMqTransport](publish) Not connected to the broker | transport=${this.#transportName} destination=${routingKey}`,
      );
    }
    const returnKey = unroutableKey(options.messageId, routingKey);
    const target = { eventId, destination: routingKey };
    await new Promise<void>((resolve, reject) => {
      const cancelTimeout = this.#startTimeout(target, reject);
      const pending = { ...target, publisher, returnKey, cancelTimeout, resolve, reject };
      try {
        publisher.channel.publish(
          exchange,
          routingKey,
          content,
          { ...options, mandatory },
          (error) => this.#confirmed(pending, error),
        );
      } catch (error) {
        cancelTimeout();
        reject(this.#toPublishError(error, target));
      }
    });
  }

  #startTimeout(
    { eventId, destination }: PublishTarget,
    reject: (error: Error) => void,
  ): CancelTimer {
    return schedule(systemClock, {
      run: () => {
        reject(
          new RabbitMqUnavailableError(
            `[RabbitMqTransport](publish) No broker confirm within ${this.#publishTimeoutMs}ms | transport=${this.#transportName} eventId=${eventId} destination=${destination}`,
          ),
        );
      },
      delayMs: this.#publishTimeoutMs,
    });
  }

  /** The broker's confirm (or nack) of one message arrived. */
  #confirmed(pending: PendingConfirm, error: Error | null | undefined): void {
    pending.cancelTimeout();
    const returned = takeUnroutable(pending.publisher.unroutable, pending.returnKey);
    if (error) {
      pending.reject(this.#toPublishError(error, pending));
    } else if (returned) {
      pending.reject(
        new UnroutableMessageError(
          `[RabbitMqTransport](publish) No queue is bound to the routing key | transport=${this.#transportName} eventId=${pending.eventId} destination=${pending.destination}`,
        ),
      );
    } else {
      pending.resolve();
    }
  }

  #toPublishError(error: unknown, { eventId, destination }: PublishTarget): Error {
    const { message } = toErrorInfo(error);
    const detail = `transport=${this.#transportName} eventId=${eventId} destination=${destination} error=${message}`;
    return message === NACK_MESSAGE
      ? new RabbitMqPublishRejectedError(
          `[RabbitMqTransport](publish) Broker nacked the message | ${detail}`,
          { cause: error },
        )
      : new RabbitMqUnavailableError(`[RabbitMqTransport](publish) Publish failed | ${detail}`, {
          cause: error,
        });
  }
}

/** Matches a broker return with the publish it belongs to: message id and routing key. */
export function unroutableKey(messageId: unknown, routingKey: string): string {
  return `${String(messageId)}\n${routingKey}`;
}

export function recordUnroutable(unroutable: Map<string, number>, key: string): void {
  unroutable.set(key, (unroutable.get(key) ?? 0) + 1);
}

function takeUnroutable(unroutable: Map<string, number>, key: string): boolean {
  const count = unroutable.get(key);
  if (count === undefined) {
    return false;
  }
  if (count === 1) {
    unroutable.delete(key);
  } else {
    unroutable.set(key, count - 1);
  }
  return true;
}
