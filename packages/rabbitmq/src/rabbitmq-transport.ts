import {
  ConfigurationError,
  systemClock,
  toErrorInfo,
  type EventEnvelope,
  type Transport,
  type TransportMessageHandler,
} from '@relaybus/core';
import { schedule } from '@relaybus/core/internal';
import type {
  AmqpConfirmChannel,
  AmqpConnection,
  AmqpMessage,
  AmqpPublishOptions,
} from './amqp-types.js';
import { ConnectionSupervisor } from './connection-supervisor.js';
import {
  RabbitMqPublishRejectedError,
  RabbitMqUnavailableError,
  UnroutableMessageError,
} from './errors.js';
import {
  resolveQueueName,
  resolveSettings,
  type RabbitMqTransportOptions,
  type ResolvedTransportSettings,
} from './options.js';
import { QueueConsumer } from './queue-consumer.js';
import { SerialQueue } from './serial-queue.js';
import { assertConsumerTopology, assertExchange, type ConsumerTopology } from './topology.js';
import { assertShortString } from './wire-format.js';

/** Publisher channel plus the broker returns not yet matched with their confirm. */
interface Publisher {
  readonly channel: AmqpConfirmChannel;
  readonly unroutable: Map<string, number>;
  /** Set when the channel closed, also while the transport was still being set up. */
  closed: boolean;
}

interface LiveConnection {
  readonly connection: AmqpConnection;
  readonly publisher: Publisher;
}

interface Subscription {
  readonly topology: ConsumerTopology;
  readonly consumer: QueueConsumer;
}

/** amqplib reports a negative acknowledgement with this exact message. */
const NACK_MESSAGE = 'message nacked';

/** One message to publish and wait for the broker's confirm. */
type PublishConfirmedInput = {
  readonly exchange: string;
  readonly routingKey: string;
  readonly content: Buffer;
  readonly options: AmqpPublishOptions;
  /** Ask the broker to return the message when no queue is bound to its routing key. */
  readonly mandatory: boolean;
};

/**
 * RabbitMQ {@link Transport} built on amqplib.
 *
 * Publishing: each event goes to a durable topic exchange as a persistent,
 * mandatory message with the destination as routing key. `publish` resolves
 * only after the broker confirmed it was persisted and routed to a queue, and
 * rejects when the transport is disconnected, the broker nacks, the message is
 * unroutable or no confirm arrives in time. It never drops a message silently,
 * so the outbox retries the delivery.
 *
 * Subscribing: one durable quorum queue per consumer group, bound to the
 * destinations (routing-key patterns such as `orders.*`). A message is
 * acknowledged only after the handler resolved. A failing handler is retried
 * on the same delivery, then through retry queues with growing delays (see
 * `redelivery`), and finally dead-lettered after the retry window; poison
 * messages are dead-lettered at once. Nothing is lost on the way.
 *
 * Reconnection: after a connection or channel loss the transport reconnects
 * with capped, jittered backoff and restores channels, topology and the
 * consumer. Events published meanwhile are rejected, and therefore retried by
 * the outbox, rather than buffered in memory.
 */
export class RabbitMqTransport implements Transport {
  readonly name: string;
  readonly #settings: ResolvedTransportSettings;
  readonly #supervisor: ConnectionSupervisor;
  /** Serializes topology work so a subscription never races a reconnect. */
  readonly #topologyWork = new SerialQueue();
  #live: LiveConnection | undefined;
  #subscription: Subscription | undefined;
  #closed = false;

  constructor(options: RabbitMqTransportOptions) {
    this.#settings = resolveSettings(options);
    this.name = this.#settings.name;
    this.#supervisor = new ConnectionSupervisor({
      connect: this.#settings.connect,
      reconnect: this.#settings.reconnect,
      onConnected: (connection) => this.#establishChannels(connection),
      onDisconnected: () => this.#forgetConnection(),
      logger: this.#settings.logger,
      random: this.#settings.random,
    });
  }

  /**
   * True while the connection is open and publishing and consuming are set up.
   * Use it for readiness probes.
   */
  isConnected(): boolean {
    return this.#supervisor.isConnected();
  }

  /**
   * True when the transport can publish and, once subscribed, is consuming: the
   * connection is up, its channels are open and the consumer is registered. False while reconnecting and after a cancelled consumer.
   */
  isReady(): boolean {
    return (
      this.#supervisor.isConnected() &&
      this.#live !== undefined &&
      (this.#subscription === undefined || this.#subscription.consumer.isActive())
    );
  }

  /** Connects to the broker. Rejects when it is unreachable unless `failOnConnectError` is false. */
  async connect(): Promise<void> {
    await this.#supervisor.start(this.#settings.failOnConnectError);
  }

  /**
   * Publishes to the exchange with `destination` as routing key and resolves
   * after the broker confirmed the message.
   *
   * @throws {UnserializablePayloadError} the payload is not representable as JSON (never retried)
   * @throws {AmqpLimitExceededError} the event id, a header name or `destination` exceeds 255 bytes (never retried)
   * @throws {RabbitMqUnavailableError} not connected, channel lost, or confirm timed out
   * @throws {RabbitMqPublishRejectedError} the broker nacked the message
   * @throws {UnroutableMessageError} no queue is bound to `destination` (unless `allowUnroutable`)
   * @throws whatever the `codec`'s `encode` throws, such as {@link RabbitMqUnencodableEventError}
   */
  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    assertShortString('Destination (routing key)', destination);
    const { content, options } = this.#settings.encode(envelope);
    await this.#publishConfirmed({
      exchange: this.#settings.exchange,
      routingKey: destination,
      content,
      options,
      mandatory: !this.#settings.allowUnroutable,
    });
  }

  async #publishConfirmed(input: PublishConfirmedInput): Promise<void> {
    const { exchange, routingKey, content, options, mandatory } = input;
    const publisher = this.#live?.publisher;
    const eventId = options.messageId ?? 'unknown';
    if (publisher === undefined) {
      throw new RabbitMqUnavailableError(
        `[RabbitMqTransport](publish) Not connected to the broker | transport=${this.name} destination=${routingKey}`,
      );
    }
    const returnKey = unroutableKey(options.messageId, routingKey);
    await new Promise<void>((resolve, reject) => {
      const cancelTimeout = schedule(systemClock, {
        run: () => {
          reject(
            new RabbitMqUnavailableError(
              `[RabbitMqTransport](publish) No broker confirm within ${this.#settings.publishTimeoutMs}ms | transport=${this.name} eventId=${eventId} destination=${routingKey}`,
            ),
          );
        },
        delayMs: this.#settings.publishTimeoutMs,
      });
      const onConfirm = (error: Error | null | undefined): void => {
        cancelTimeout();
        const returned = takeUnroutable(publisher.unroutable, returnKey);
        if (error) {
          reject(this.#toPublishError(error, { eventId, destination: routingKey }));
        } else if (returned) {
          reject(
            new UnroutableMessageError(
              `[RabbitMqTransport](publish) No queue is bound to the routing key | transport=${this.name} eventId=${eventId} destination=${routingKey}`,
            ),
          );
        } else {
          resolve();
        }
      };
      try {
        publisher.channel.publish(
          exchange,
          routingKey,
          content,
          { ...options, mandatory },
          onConfirm,
        );
      } catch (error) {
        cancelTimeout();
        reject(this.#toPublishError(error, { eventId, destination: routingKey }));
      }
    });
  }

  /**
   * Binds one durable quorum queue to `destinations` and delivers its messages
   * to `onMessage`. Call it once per transport; give each subscription its own
   * transport instance, with its own `name` and `consumerGroup`.
   */
  async subscribe(
    destinations: readonly string[],
    onMessage: TransportMessageHandler,
  ): Promise<void> {
    if (this.#subscription !== undefined) {
      throw new ConfigurationError(
        `Transport '${this.name}' already has a subscription; create another transport for another consumer group`,
      );
    }
    if (destinations.length === 0) {
      throw new ConfigurationError(`Transport '${this.name}' needs at least one destination`);
    }
    const queue = resolveQueueName(this.#settings);
    const subscription: Subscription = {
      topology: {
        exchange: this.#settings.exchange,
        queue,
        deadLetterExchange: this.#settings.deadLetterExchange,
        deadLetterQueue: this.#settings.deadLetterQueue ?? `${queue}.dead`,
        deliveryLimit: this.#settings.deliveryLimit,
        ordered: this.#settings.ordered,
        retryDelaysMs: this.#settings.retryLadder,
        destinations,
      },
      consumer: new QueueConsumer({
        queue,
        prefetch: this.#settings.prefetch,
        handlerRetry: this.#settings.handlerRetry,
        redelivery: this.#settings.redelivery,
        retryLadder: this.#settings.retryLadder,
        redeliveryMaxAgeMs: this.#settings.redeliveryMaxAgeMs,
        ordered: this.#settings.ordered,
        deadLetterExchange: this.#settings.deadLetterExchange,
        republish: ({ exchange, routingKey }, { content, options }) =>
          this.#publishConfirmed({ exchange, routingKey, content, options, mandatory: true }),
        shutdownTimeoutMs: this.#settings.shutdownTimeoutMs,
        onMessage,
        decode: this.#settings.decode,
        onConsumerLost: (reason) => this.#supervisor.recover(reason),
        logger: this.#settings.logger,
        random: this.#settings.random,
      }),
    };
    await this.#topologyWork.run(async () => {
      this.#subscription = subscription;
      if (this.#live !== undefined) {
        await this.#startConsuming(this.#live.connection, subscription);
      }
    });
  }

  /**
   * Stops consuming, waits up to `shutdownTimeout` for running handlers
   * (messages still being retried go back to the queue), then closes the
   * channels and the connection.
   */
  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    await this.#supervisor.close(() =>
      this.#topologyWork.run(async () => {
        const live = this.#live;
        this.#live = undefined;
        await this.#subscription?.consumer.stop();
        await live?.publisher.channel.close().catch(() => undefined);
      }),
    );
  }

  async #establishChannels(connection: AmqpConnection): Promise<void> {
    await this.#topologyWork.run(async () => {
      const channel = await connection.createConfirmChannel();
      const publisher: Publisher = { channel, unroutable: new Map(), closed: false };
      channel.on('error', (error: Error) => {
        this.#settings.logger.warn(
          `[RabbitMqTransport](establishChannels) Publisher channel error | transport=${this.name} error=${error.message}`,
        );
      });
      channel.on('close', () => {
        publisher.closed = true;
        if (this.#live?.publisher === publisher) {
          this.#supervisor.recover('publisher channel closed');
        }
      });
      channel.on('return', (message: AmqpMessage) => {
        recordUnroutable(
          publisher.unroutable,
          unroutableKey(message.properties.messageId, message.fields.routingKey),
        );
      });
      await assertExchange(channel, this.#settings.exchange);
      if (this.#subscription !== undefined) {
        await this.#startConsuming(connection, this.#subscription);
      }
      if (publisher.closed) {
        // The close came before `live` was set, so nothing reacted to it.
        throw new RabbitMqUnavailableError(
          `[RabbitMqTransport](establishChannels) Publisher channel closed while the transport was being set up | transport=${this.name}`,
        );
      }
      this.#live = { connection, publisher };
    });
  }

  async #startConsuming(connection: AmqpConnection, subscription: Subscription): Promise<void> {
    const channel = await connection.createChannel();
    await assertConsumerTopology(channel, subscription.topology);
    await subscription.consumer.attach(channel);
  }

  #forgetConnection(): void {
    this.#live = undefined;
    this.#subscription?.consumer.detach();
  }

  #toPublishError(
    error: unknown,
    { eventId, destination }: { readonly eventId: string; readonly destination: string },
  ): Error {
    const { message } = toErrorInfo(error);
    const detail = `transport=${this.name} eventId=${eventId} destination=${destination} error=${message}`;
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

function unroutableKey(messageId: unknown, routingKey: string): string {
  return `${String(messageId)}\n${routingKey}`;
}

function recordUnroutable(unroutable: Map<string, number>, key: string): void {
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
