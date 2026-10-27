import {
  ConfigurationError,
  WireTypesUnsupportedError,
  type CloseOptions,
  type EventEnvelope,
} from '@relaybus/core';
import {
  SubscribingTransport,
  withDeclaredWireTypes,
  type TransportSubscription,
} from '@relaybus/core/internal';
import type { AmqpConnection, AmqpMessage } from './amqp-types.js';
import {
  ConfirmedPublishing,
  recordUnroutable,
  unroutableKey,
  type Publisher,
} from './confirmed-publishing.js';
import { ConnectionSupervisor } from './connection-supervisor.js';
import { RabbitMqUnavailableError } from './errors.js';
import {
  codecSettings,
  resolveQueueName,
  resolveSettings,
  type RabbitMqTransportOptions,
  type ResolvedTransportSettings,
} from './options.js';
import { QueueConsumer } from './queue-consumer.js';
import { SerialQueue } from './serial-queue.js';
import { assertConsumerTopology, assertExchange, type ConsumerTopology } from './topology.js';
import { assertShortString } from './wire-format.js';

interface LiveConnection {
  readonly connection: AmqpConnection;
  readonly publisher: Publisher;
}

interface Subscription {
  readonly topology: ConsumerTopology;
  readonly consumer: QueueConsumer;
}

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
export class RabbitMqTransport extends SubscribingTransport {
  readonly name: string;
  readonly system = 'rabbitmq';
  #settings: ResolvedTransportSettings;
  readonly #supervisor: ConnectionSupervisor;
  readonly #publishing: ConfirmedPublishing;
  /** Serializes topology work so a subscription never races a reconnect. */
  readonly #topologyWork = new SerialQueue();
  #live: LiveConnection | undefined;
  #subscription: Subscription | undefined;
  #closing: Promise<void> | undefined;
  /** Set when the transport first connects, publishes or subscribes: its wire format is fixed from then on. */
  #used = false;

  constructor(options: RabbitMqTransportOptions) {
    super();
    this.#settings = resolveSettings(options);
    this.name = this.#settings.name;
    this.#publishing = new ConfirmedPublishing(this.name, this.#settings.publishTimeoutMs);
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
    this.#used = true;
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
  /**
   * Checks the wire names a route or catalog declares and returns the function that makes the codec
   * write and read events under them. The bus calls it when it is created, before the transport
   * connects.
   *
   * @throws WireTypesUnsupportedError when the codec does not map type names (use `jsonEnvelope`),
   * WireTypeCollisionError when a name clashes with one of the codec's own `wireTypes`.
   */
  declareWireTypes(wireTypes: Readonly<Record<string, string>>): () => void {
    if (this.#used) {
      throw new WireTypesUnsupportedError(
        `Transport '${this.name}' already started; declare wire names before it connects or subscribes`,
      );
    }
    const codec = withDeclaredWireTypes(this.#settings.codec, { wireTypes, transport: this.name });
    return () => {
      this.#settings = { ...this.#settings, ...codecSettings(codec) };
    };
  }

  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    this.#used = true;
    assertShortString('Destination (routing key)', destination);
    const { content, options } = this.#settings.encode(envelope);
    await this.#publishing.publish(this.#live?.publisher, {
      exchange: this.#settings.exchange,
      routingKey: destination,
      content,
      options,
      mandatory: !this.#settings.allowUnroutable,
    });
  }

  /**
   * Binds one durable quorum queue to `destinations` and delivers its messages
   * to `onMessage`. Call it once per transport; give each subscription its own
   * transport instance, with its own `name` and `consumerGroup`.
   */
  protected async openSubscription({
    destinations,
    onMessage,
    hooks,
  }: TransportSubscription): Promise<void> {
    this.#used = true;
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
      topology: this.#topologyOf(queue, destinations),
      consumer: this.#createConsumer(queue, { onMessage, hooks }),
    };
    await this.#topologyWork.run(async () => {
      this.#subscription = subscription;
      if (this.#live !== undefined) {
        await this.#startConsuming(this.#live.connection, subscription);
      }
    });
  }

  #topologyOf(queue: string, destinations: readonly string[]): ConsumerTopology {
    return {
      exchange: this.#settings.exchange,
      queue,
      deadLetterExchange: this.#settings.deadLetterExchange,
      deadLetterQueue: this.#settings.deadLetterQueue ?? `${queue}.dead`,
      deliveryLimit: this.#settings.deliveryLimit,
      ordered: this.#settings.ordered,
      retryDelaysMs: this.#settings.retryLadder,
      destinations,
    };
  }

  #createConsumer(
    queue: string,
    { onMessage, hooks }: Pick<TransportSubscription, 'onMessage' | 'hooks'>,
  ): QueueConsumer {
    return new QueueConsumer({
      queue,
      prefetch: this.#settings.prefetch,
      handlerRetry: this.#settings.handlerRetry,
      redelivery: this.#settings.redelivery,
      retryLadder: this.#settings.retryLadder,
      redeliveryMaxAgeMs: this.#settings.redeliveryMaxAgeMs,
      ordered: this.#settings.ordered,
      deadLetterExchange: this.#settings.deadLetterExchange,
      republish: ({ exchange, routingKey }, { content, options }) =>
        this.#publishing.publish(this.#live?.publisher, {
          exchange,
          routingKey,
          content,
          options,
          mandatory: true,
        }),
      shutdownTimeoutMs: this.#settings.shutdownTimeoutMs,
      onMessage,
      ...(hooks === undefined ? {} : { onInvalidMessage: hooks.onInvalidMessage }),
      decode: this.#settings.decode,
      onConsumerLost: (reason) => this.#supervisor.recover(reason),
      logger: this.#settings.logger,
      random: this.#settings.random,
    });
  }

  /**
   * Stops consuming, waits up to `shutdownTimeout` for running handlers
   * (messages still being retried go back to the queue), then closes the
   * channels and the connection. Once `options.signal` aborts (the bus shutdown deadline passed),
   * the connection's socket is destroyed instead of waiting for the broker.
   */
  async close(options: CloseOptions = {}): Promise<void> {
    this.#used = true;
    const { signal } = options;
    const force = (): void => this.#supervisor.forceClose();
    if (signal?.aborted === true) {
      force();
    }
    signal?.addEventListener('abort', force, { once: true });
    try {
      // A second call shares the work of the first, and its signal can still cut the connection.
      this.#closing ??= this.#supervisor.close(() =>
        this.#topologyWork.run(async () => {
          const live = this.#live;
          this.#live = undefined;
          await this.#subscription?.consumer.stop();
          await live?.publisher.channel.close().catch(() => undefined);
        }),
      );
      await this.#closing;
    } finally {
      signal?.removeEventListener('abort', force);
    }
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
}
