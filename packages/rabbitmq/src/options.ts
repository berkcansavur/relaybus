import {
  ConfigurationError,
  createRetryPolicy,
  toMilliseconds,
  type Duration,
  type EventEnvelope,
  type RelayBusLogger,
  type RetryPolicy,
  type RetryPolicyOptions,
} from '@relaybus/core';
import type { AmqpConnection, AmqpMessage } from './amqp-types.js';
import { completeDecodedEvent, type RabbitMqMessageDecoder } from './inbound-decoders.js';
import { buildRetryLadder } from './redelivery.js';
import { decodeEnvelope } from './wire-format.js';

/** Connection settings accepted by `amqplib.connect`, given as an object instead of a URL. */
export interface BrokerConnectionSettings {
  readonly protocol?: string;
  readonly hostname?: string;
  readonly port?: number;
  readonly username?: string;
  readonly password?: string;
  readonly vhost?: string;
  /** Seconds. See "Operational notes" in the README for the heartbeat defaults of each amqplib version. */
  readonly heartbeat?: number;
}

/** Backoff between reconnect attempts. Attempts are unlimited. */
export type ReconnectOptions = Pick<
  RetryPolicyOptions,
  'initialDelay' | 'factor' | 'maxDelay' | 'jitter'
>;

/**
 * Delayed redelivery of messages whose handler keeps failing. Each failed
 * delivery goes to a retry queue and returns to the main queue after a growing
 * delay, so an outage of the handler's dependency is waited out instead of
 * burning through delivery attempts.
 */
export type RedeliveryOptions = Pick<
  RetryPolicyOptions,
  'initialDelay' | 'factor' | 'maxDelay' | 'jitter'
> & {
  /**
   * How long a message may keep failing, counted from its first delayed
   * redelivery, before it is dead-lettered. Default `'1h'`.
   */
  readonly maxAge?: Duration;
};

export interface RabbitMqTransportOptions {
  /** Name referenced by routes and subscriptions. Default `'rabbitmq'`. */
  readonly name?: string;
  /**
   * Broker location, for example `amqp://user:pass@host:5672/vhost?heartbeat=30`.
   * Give exactly one of `url` and `connect`.
   */
  readonly url?: string | BrokerConnectionSettings;
  /**
   * Opens one connection. Called again after every connection loss. Use it for
   * TLS options, custom credentials or an alternative amqplib instance.
   */
  readonly connect?: () => Promise<AmqpConnection>;
  /** Topic exchange events are published to. Durable, created when missing. Default `'relaybus'`. */
  readonly exchange?: string;
  /**
   * Unacknowledged messages the consumer holds, which is also its concurrency.
   * Default 50, or 1 when `ordered`.
   */
  readonly prefetch?: number;
  /** Prefix of derived queue names. Default `'relaybus'`. */
  readonly queuePrefix?: string;
  /**
   * Consumer group: services sharing it share one queue and split the messages,
   * services with different groups each receive every message. The queue is
   * `<queuePrefix>.<consumerGroup>`. Required when subscribing, unless `queue` is set.
   */
  readonly consumerGroup?: string;
  /** Explicit queue name. Overrides `queuePrefix` and `consumerGroup`. */
  readonly queue?: string;
  /** Exchange poison messages are dead-lettered to. Default `'<exchange>.dead'`. */
  readonly deadLetterExchange?: string;
  /** Queue that keeps dead-lettered messages for inspection. Default `'<queue>.dead'`. */
  readonly deadLetterQueue?: string;
  /**
   * Quorum queue `x-delivery-limit`: deliveries after which the broker itself
   * dead-letters a message. Failing handlers do not consume it (they use
   * `redelivery`); it only stops a message that crashes the consumer or its
   * channel again and again. Default 10.
   */
  readonly deliveryLimit?: number;
  /**
   * Handler attempts on the same delivery, with backoff, before the message is
   * scheduled for delayed redelivery. Absorbs short blips without a broker round
   * trip. Default: 3 attempts, 200 ms initial delay, factor 2, 5 s cap, equal jitter.
   */
  readonly handlerRetry?: RetryPolicyOptions;
  /**
   * Delayed redelivery after `handlerRetry` is spent. Default: 2 s initial
   * delay, factor 2, 5 min cap, equal jitter, dead-lettered after 1 h of
   * failing (about 18 redeliveries). Delays are quantized to the retry queues,
   * one per step of the exponential ladder (9 queues with the defaults).
   * Ignored when `ordered`, where the same delays are waited in place.
   */
  readonly redelivery?: RedeliveryOptions;
  /**
   * Consume the queue strictly in order: a single active consumer across all
   * replicas, one message at a time, a failing message is retried in place
   * (blocking the queue) instead of being delayed. Needed when
   * `EventEnvelope.key` order matters; costs throughput, because one handler
   * runs at a time. Default false: handlers run concurrently and a delayed
   * redelivery overtakes later messages, so order is not preserved.
   */
  readonly ordered?: boolean;
  /** Reconnect backoff, or `false` to never reconnect. Default: 500 ms initial, factor 2, 30 s cap, equal jitter. */
  readonly reconnect?: ReconnectOptions | false;
  /**
   * When true (default) `connect()` rejects if the broker is unreachable, which
   * fails startup. When false it resolves and keeps trying in the background.
   */
  readonly failOnConnectError?: boolean;
  /**
   * When false (default) a message that no queue is bound to makes `publish`
   * reject. When true it resolves and the broker silently discards the message.
   */
  readonly allowUnroutable?: boolean;
  /** How long `publish` waits for the broker confirm. Default `'30s'`. */
  readonly publishTimeout?: Duration;
  /** How long `close` waits for running handlers. Default `'30s'`. */
  readonly shutdownTimeout?: Duration;
  /**
   * How consumed messages become events. Default: CloudEvents binary mode as
   * relaybus producers write it (`cloudEvents:*` headers, AMQP properties as
   * fallback); any other message is malformed and dead-lettered. Set it to
   * consume messages from services that do not use relaybus:
   * {@link decodeCloudEvent} also accepts structured mode, {@link plainJsonDecoder}
   * maps plain JSON messages, and a function of your own can do anything else
   * (switch on `message.fields.routingKey` to decode routing keys differently).
   * Whatever a decoder throws marks the message as malformed: it is
   * dead-lettered, the consumer keeps running.
   */
  readonly decode?: RabbitMqMessageDecoder;
  /** Default: `console`. */
  readonly logger?: RelayBusLogger;
  /** Random source for jitter. Default `Math.random`. Intended for tests. */
  readonly random?: () => number;
}

export interface ResolvedTransportSettings {
  readonly name: string;
  readonly connect: () => Promise<AmqpConnection>;
  readonly exchange: string;
  readonly prefetch: number;
  readonly queuePrefix: string;
  readonly consumerGroup: string | undefined;
  readonly queue: string | undefined;
  readonly deadLetterExchange: string;
  readonly deadLetterQueue: string | undefined;
  readonly deliveryLimit: number;
  readonly handlerRetry: RetryPolicy;
  readonly redelivery: RetryPolicy;
  /** Fixed delays of the retry queues; empty when `ordered`. */
  readonly retryLadder: readonly number[];
  readonly redeliveryMaxAgeMs: number;
  readonly ordered: boolean;
  readonly reconnect: RetryPolicy | undefined;
  readonly failOnConnectError: boolean;
  readonly allowUnroutable: boolean;
  readonly publishTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  /** Turns a consumed message into a complete envelope; throws for a malformed one. */
  readonly decode: (message: AmqpMessage) => EventEnvelope;
  readonly logger: RelayBusLogger;
  readonly random: () => number;
}

const DEFAULT_HANDLER_RETRY: RetryPolicy = {
  attempts: 3,
  initialDelayMs: 200,
  factor: 2,
  maxDelayMs: 5_000,
  jitter: 'equal',
  retryIf: undefined,
};

const DEFAULT_REDELIVERY: RetryPolicy = {
  attempts: Number.POSITIVE_INFINITY,
  initialDelayMs: 2_000,
  factor: 2,
  maxDelayMs: 300_000,
  jitter: 'equal',
  retryIf: undefined,
};

const DEFAULT_REDELIVERY_MAX_AGE = '1h';

const DEFAULT_RECONNECT: RetryPolicy = {
  attempts: 1,
  initialDelayMs: 500,
  factor: 2,
  maxDelayMs: 30_000,
  jitter: 'equal',
  retryIf: undefined,
};

/** Validates the options and applies defaults. Throws `ConfigurationError`. */
export function resolveSettings(options: RabbitMqTransportOptions): ResolvedTransportSettings {
  const exchange = nonEmpty(options.exchange ?? 'relaybus', 'exchange');
  const failOnConnectError = options.failOnConnectError ?? true;
  if (!failOnConnectError && options.reconnect === false) {
    throw new ConfigurationError(
      'failOnConnectError=false needs reconnect enabled, otherwise the transport would never connect',
    );
  }
  const ordered = options.ordered ?? false;
  const prefetch = positiveInteger(options.prefetch ?? (ordered ? 1 : 50), 'prefetch');
  if (ordered && prefetch !== 1) {
    throw new ConfigurationError(`ordered consumption needs prefetch 1, received ${prefetch}`);
  }
  const redelivery = createRetryPolicy(options.redelivery, DEFAULT_REDELIVERY);
  return {
    name: nonEmpty(options.name ?? 'rabbitmq', 'name'),
    connect: resolveConnect(options),
    exchange,
    prefetch,
    queuePrefix: nonEmpty(options.queuePrefix ?? 'relaybus', 'queuePrefix'),
    consumerGroup: optionalNonEmpty(options.consumerGroup, 'consumerGroup'),
    queue: optionalNonEmpty(options.queue, 'queue'),
    deadLetterExchange: nonEmpty(
      options.deadLetterExchange ?? `${exchange}.dead`,
      'deadLetterExchange',
    ),
    deadLetterQueue: optionalNonEmpty(options.deadLetterQueue, 'deadLetterQueue'),
    deliveryLimit: positiveInteger(options.deliveryLimit ?? 10, 'deliveryLimit'),
    handlerRetry: createRetryPolicy(options.handlerRetry, DEFAULT_HANDLER_RETRY),
    redelivery,
    retryLadder: ordered ? [] : buildRetryLadder(redelivery),
    redeliveryMaxAgeMs: positiveDuration(
      options.redelivery?.maxAge ?? DEFAULT_REDELIVERY_MAX_AGE,
      'redelivery.maxAge',
    ),
    ordered,
    reconnect: resolveReconnect(options.reconnect),
    failOnConnectError,
    allowUnroutable: options.allowUnroutable ?? false,
    publishTimeoutMs: positiveDuration(options.publishTimeout ?? '30s', 'publishTimeout'),
    shutdownTimeoutMs: toMilliseconds(options.shutdownTimeout ?? '30s', 'shutdownTimeout'),
    decode: resolveDecode(options.decode),
    logger: options.logger ?? console,
    random: options.random ?? Math.random,
  };
}

/** `<queuePrefix>.<consumerGroup>`, or the explicit queue name. */
export function resolveQueueName(settings: ResolvedTransportSettings): string {
  if (settings.queue !== undefined) {
    return settings.queue;
  }
  if (settings.consumerGroup === undefined) {
    throw new ConfigurationError(
      `Transport '${settings.name}' needs 'consumerGroup' or 'queue' to subscribe`,
    );
  }
  return `${settings.queuePrefix}.${settings.consumerGroup}`;
}

function resolveDecode(
  decode: RabbitMqMessageDecoder | undefined,
): (message: AmqpMessage) => EventEnvelope {
  if (decode === undefined) {
    return decodeEnvelope;
  }
  return (message) => completeDecodedEvent(decode(message), message);
}

function resolveConnect(options: RabbitMqTransportOptions): () => Promise<AmqpConnection> {
  const { url, connect } = options;
  if ((url === undefined) === (connect === undefined)) {
    throw new ConfigurationError("Give exactly one of 'url' and 'connect'");
  }
  if (connect !== undefined) {
    return connect;
  }
  return async () => {
    const amqplib = await import('amqplib');
    return amqplib.connect(url!);
  };
}

function resolveReconnect(
  reconnect: ReconnectOptions | false | undefined,
): RetryPolicy | undefined {
  if (reconnect === false) {
    return undefined;
  }
  return createRetryPolicy({ ...reconnect, attempts: 1 }, DEFAULT_RECONNECT);
}

function nonEmpty(value: string, name: string): string {
  if (value.length === 0) {
    throw new ConfigurationError(`${name} must not be empty`);
  }
  return value;
}

function optionalNonEmpty(value: string | undefined, name: string): string | undefined {
  return value === undefined ? undefined : nonEmpty(value, name);
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new ConfigurationError(`${name} must be a positive integer, received ${value}`);
  }
  return value;
}

function positiveDuration(value: Duration, name: string): number {
  const milliseconds = toMilliseconds(value, name);
  if (milliseconds === 0) {
    throw new ConfigurationError(`${name} must be greater than zero`);
  }
  return milliseconds;
}
