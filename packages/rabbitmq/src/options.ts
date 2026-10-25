import {
  ConfigurationError,
  createRetryPolicy,
  type Duration,
  type EventEnvelope,
  type RelayBusLogger,
  type RetryPolicy,
  type RetryPolicyOptions,
} from '@relaybus/core';
import { Milliseconds, NonEmptyText, PositiveInteger } from '@relaybus/core/internal';
import type { AmqpConnection, AmqpMessage } from './amqp-types.js';
import { cloudEventsBinary, type RabbitMqCodec } from './codecs.js';
import { completeDecodedEvent, type RabbitMqMessageDecoder } from './inbound-decoders.js';
import { buildRetryLadder } from './redelivery.js';
import { decodeEnvelope, encodeEnvelope, type EncodedMessage } from './wire-format.js';

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
   * TLS options, custom credentials or an alternative amqplib instance. `signal` aborts when a
   * shutdown runs past its deadline: pass it on (`amqplib.connect(url, { signal })`) so a broker
   * that never answers the handshake cannot hold the socket open.
   */
  readonly connect?: (options: ConnectOptions) => Promise<AmqpConnection>;
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
   * The wire format, both ways: how events become messages and consumed
   * messages become events. Default {@link cloudEventsBinary}: CloudEvents
   * binary mode (`cloudEvents:*` headers, AMQP properties as fallback), any
   * other message is malformed and dead-lettered. {@link cloudEventsStructured}
   * writes structured mode and reads both, {@link jsonEnvelope} maps a JSON
   * format of your own (an envelope with the payload inside, or the bare
   * payload), and an object with `encode` and `decode` can do anything else.
   * Whatever `decode` throws marks the message as malformed: it is
   * dead-lettered, the consumer keeps running.
   */
  readonly codec?: RabbitMqCodec;
  /**
   * How consumed messages become events; events are still written in
   * CloudEvents binary mode.
   *
   * @deprecated Use `codec`: `decode: fn` is `codec: { encode: encodeEnvelope, decode: fn }`.
   * Giving both `decode` and `codec` is a configuration error.
   */
  readonly decode?: RabbitMqMessageDecoder;
  /** Default: `console`. */
  readonly logger?: RelayBusLogger;
  /** Random source for jitter. Default `Math.random`. Intended for tests. */
  readonly random?: () => number;
}

/** What `connect` gets for each connection it opens. */
export interface ConnectOptions {
  /** Aborted when a shutdown runs past its deadline; destroys a socket still connecting. */
  readonly signal: AbortSignal;
}

export interface ResolvedTransportSettings {
  readonly name: string;
  readonly connect: (options: ConnectOptions) => Promise<AmqpConnection>;
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
  /** Turns an event into the message to publish. */
  readonly encode: (envelope: EventEnvelope) => EncodedMessage;
  /** Turns a consumed message into a complete envelope; throws for a malformed one. */
  readonly decode: (message: AmqpMessage) => EventEnvelope;
  readonly logger: RelayBusLogger;
  readonly random: () => number;
}

/** Default `prefetch` when not ordered. */
const DEFAULT_PREFETCH = 50;
/** Default `deliveryLimit`. */
const DEFAULT_DELIVERY_LIMIT = 10;

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
  const basics = resolveBasics(options);
  return {
    ...resolveTopology(options, basics),
    ...resolveRetries(options, basics),
    ...resolveRuntime(options, basics),
  };
}

/** The settings the others depend on, checked first: exchange, connect behaviour, ordering, prefetch, redelivery. */
type BasicSettings = Pick<
  ResolvedTransportSettings,
  'exchange' | 'failOnConnectError' | 'ordered' | 'prefetch' | 'redelivery'
>;

function resolveBasics(options: RabbitMqTransportOptions): BasicSettings {
  const exchange = NonEmptyText.of(options.exchange ?? 'relaybus', 'exchange');
  const failOnConnectError = options.failOnConnectError ?? true;
  if (!failOnConnectError && options.reconnect === false) {
    throw new ConfigurationError(
      'failOnConnectError=false needs reconnect enabled, otherwise the transport would never connect',
    );
  }
  const ordered = options.ordered ?? false;
  const prefetch = PositiveInteger.of(
    options.prefetch ?? (ordered ? 1 : DEFAULT_PREFETCH),
    'prefetch',
  );
  if (ordered && prefetch !== 1) {
    throw new ConfigurationError(`ordered consumption needs prefetch 1, received ${prefetch}`);
  }
  const redelivery = createRetryPolicy(options.redelivery, DEFAULT_REDELIVERY);
  return { exchange, failOnConnectError, ordered, prefetch, redelivery };
}

/** Names, connection, exchanges and queues. */
function resolveTopology(
  options: RabbitMqTransportOptions,
  { exchange, prefetch }: BasicSettings,
): Pick<
  ResolvedTransportSettings,
  | 'name'
  | 'connect'
  | 'exchange'
  | 'prefetch'
  | 'queuePrefix'
  | 'consumerGroup'
  | 'queue'
  | 'deadLetterExchange'
  | 'deadLetterQueue'
  | 'deliveryLimit'
> {
  return {
    name: NonEmptyText.of(options.name ?? 'rabbitmq', 'name'),
    connect: resolveConnect(options),
    exchange,
    prefetch,
    queuePrefix: NonEmptyText.of(options.queuePrefix ?? 'relaybus', 'queuePrefix'),
    consumerGroup: NonEmptyText.optional(options.consumerGroup, 'consumerGroup'),
    queue: NonEmptyText.optional(options.queue, 'queue'),
    deadLetterExchange: NonEmptyText.of(
      options.deadLetterExchange ?? `${exchange}.dead`,
      'deadLetterExchange',
    ),
    deadLetterQueue: NonEmptyText.optional(options.deadLetterQueue, 'deadLetterQueue'),
    deliveryLimit: PositiveInteger.of(
      options.deliveryLimit ?? DEFAULT_DELIVERY_LIMIT,
      'deliveryLimit',
    ),
  };
}

/** Handler retries, delayed redelivery and its retry queues. */
function resolveRetries(
  options: RabbitMqTransportOptions,
  { ordered, redelivery }: BasicSettings,
): Pick<
  ResolvedTransportSettings,
  'handlerRetry' | 'redelivery' | 'retryLadder' | 'redeliveryMaxAgeMs' | 'ordered'
> {
  return {
    handlerRetry: createRetryPolicy(options.handlerRetry, DEFAULT_HANDLER_RETRY),
    redelivery,
    retryLadder: ordered ? [] : buildRetryLadder(redelivery),
    redeliveryMaxAgeMs: Milliseconds.positive(
      options.redelivery?.maxAge ?? DEFAULT_REDELIVERY_MAX_AGE,
      'redelivery.maxAge',
    ),
    ordered,
  };
}

/** Reconnection, timeouts, the codec and the injected logger and randomness. */
function resolveRuntime(
  options: RabbitMqTransportOptions,
  { failOnConnectError }: BasicSettings,
): Pick<
  ResolvedTransportSettings,
  | 'reconnect'
  | 'failOnConnectError'
  | 'allowUnroutable'
  | 'publishTimeoutMs'
  | 'shutdownTimeoutMs'
  | 'encode'
  | 'decode'
  | 'logger'
  | 'random'
> {
  return {
    reconnect: resolveReconnect(options.reconnect),
    failOnConnectError,
    allowUnroutable: options.allowUnroutable ?? false,
    publishTimeoutMs: Milliseconds.positive(options.publishTimeout ?? '30s', 'publishTimeout'),
    shutdownTimeoutMs: Milliseconds.of(options.shutdownTimeout ?? '30s', 'shutdownTimeout'),
    ...resolveCodec(options),
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

/** The codec of the options; the deprecated `decode` pairs with the default encoding. */
function resolveCodec({
  codec,
  decode,
}: RabbitMqTransportOptions): Pick<ResolvedTransportSettings, 'encode' | 'decode'> {
  if (decode !== undefined && codec !== undefined) {
    throw new ConfigurationError("Give 'codec' or the deprecated 'decode', not both");
  }
  const chosen =
    decode === undefined ? (codec ?? cloudEventsBinary) : { encode: encodeEnvelope, decode };
  if (chosen === cloudEventsBinary) {
    return { encode: encodeEnvelope, decode: decodeEnvelope };
  }
  return {
    encode: chosen.encode,
    decode: (message) => completeDecodedEvent(chosen.decode(message), message),
  };
}

function resolveConnect(
  options: RabbitMqTransportOptions,
): (options: ConnectOptions) => Promise<AmqpConnection> {
  const { url, connect } = options;
  if ((url === undefined) === (connect === undefined)) {
    throw new ConfigurationError("Give exactly one of 'url' and 'connect'");
  }
  if (connect !== undefined) {
    return connect;
  }
  return async ({ signal }) => {
    const amqplib = await import('amqplib');
    // amqplib hands its socket options to net.connect / tls.connect, which take a signal; its types omit it.
    const socketOptions = { signal } as unknown as Parameters<typeof amqplib.connect>[1];
    return amqplib.connect(url!, socketOptions);
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
