import {
  ConfigurationError,
  toErrorInfo,
  type EventEnvelope,
  type Transport,
  type TransportMessageHandler,
} from '@relaybus/core';
import type { AmqpConfirmChannel, AmqpConnection, AmqpMessage } from './amqp-types.js';
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
import { encodeEnvelope } from './wire-format.js';

/** Publisher channel plus the broker returns not yet matched with their confirm. */
interface Publisher {
  readonly channel: AmqpConfirmChannel;
  readonly unroutable: Map<string, number>;
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
 * acknowledged only after the handler resolved; failures are retried with
 * backoff and finally dead-lettered, never lost.
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

  /** Connects to the broker. Rejects when it is unreachable unless `failOnConnectError` is false. */
  async connect(): Promise<void> {
    await this.#supervisor.start(this.#settings.failOnConnectError);
  }

  /**
   * Publishes to the exchange with `destination` as routing key and resolves
   * after the broker confirmed the message.
   *
   * @throws {RabbitMqUnavailableError} not connected, channel lost, or confirm timed out
   * @throws {RabbitMqPublishRejectedError} the broker nacked the message
   * @throws {UnroutableMessageError} no queue is bound to `destination` (unless `allowUnroutable`)
   */
  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    const publisher = this.#live?.publisher;
    if (publisher === undefined) {
      throw new RabbitMqUnavailableError(
        `[RabbitMqTransport](publish) Not connected to the broker | transport=${this.name} destination=${destination}`,
      );
    }
    const { content, options } = encodeEnvelope(envelope);
    const returnKey = unroutableKey(envelope.id, destination);
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new RabbitMqUnavailableError(
            `[RabbitMqTransport](publish) No broker confirm within ${this.#settings.publishTimeoutMs}ms | transport=${this.name} eventId=${envelope.id} destination=${destination}`,
          ),
        );
      }, this.#settings.publishTimeoutMs);
      const onConfirm = (error: Error | null | undefined): void => {
        clearTimeout(timer);
        const returned = takeUnroutable(publisher.unroutable, returnKey);
        if (error) {
          reject(this.#toPublishError(error, envelope.id, destination));
        } else if (returned) {
          reject(
            new UnroutableMessageError(
              `[RabbitMqTransport](publish) No queue is bound to the routing key | transport=${this.name} eventId=${envelope.id} destination=${destination}`,
            ),
          );
        } else {
          resolve();
        }
      };
      try {
        publisher.channel.publish(
          this.#settings.exchange,
          destination,
          content,
          { ...options, mandatory: !this.#settings.allowUnroutable },
          onConfirm,
        );
      } catch (error) {
        clearTimeout(timer);
        reject(this.#toPublishError(error, envelope.id, destination));
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
        destinations,
      },
      consumer: new QueueConsumer({
        queue,
        prefetch: this.#settings.prefetch,
        deliveryLimit: this.#settings.deliveryLimit,
        handlerRetry: this.#settings.handlerRetry,
        shutdownTimeoutMs: this.#settings.shutdownTimeoutMs,
        onMessage,
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
      const publisher: Publisher = { channel, unroutable: new Map() };
      channel.on('error', (error: Error) => {
        this.#settings.logger.warn(
          `[RabbitMqTransport](establishChannels) Publisher channel error | transport=${this.name} error=${error.message}`,
        );
      });
      channel.on('close', () => {
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

  #toPublishError(error: unknown, eventId: string, destination: string): Error {
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
