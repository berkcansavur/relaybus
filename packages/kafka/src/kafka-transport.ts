import {
  ConfigurationError,
  InvalidEnvelopeError,
  RelayBusError,
  toErrorInfo,
  toMilliseconds,
  type Duration,
  type EventEnvelope,
  type RelayBusLogger,
  type Transport,
  type TransportMessageHandler,
} from '@relaybus/core';
import {
  InvalidKafkaMessageError,
  decodeEnvelope,
  encodeEnvelope,
  toOutgoingHeaders,
} from './cloudevents-codec.js';
import type {
  KafkaClientLike,
  KafkaConsumerEvent,
  KafkaConsumerLike,
  KafkaEachMessagePayload,
  KafkaProducerLike,
} from './kafka-client.js';

/** A record the transport refused to hand to the bus because it is malformed. */
export interface InvalidKafkaMessage {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  /** Why the record was rejected. */
  readonly reason: string;
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
}

/**
 * What to do with a record that cannot be decoded:
 * - `'skip'` (default): log an error and move on.
 * - a function: called with the record; if it rejects, the record is retried
 *   like any other failure, so nothing is dropped unless the function returns.
 * - `{ deadLetterTopic }`: the original record is produced to that topic with
 *   `relaybus-invalid-reason`, `relaybus-source-topic`, `relaybus-source-partition`
 *   and `relaybus-source-offset` headers; the offset advances only after the ack.
 */
export type InvalidMessagePolicy =
  | 'skip'
  | ((message: InvalidKafkaMessage) => void | Promise<void>)
  | { readonly deadLetterTopic: string };

/** Bounded in-process retries around the inbound handler (the bus's `receive`). */
export interface KafkaProcessingOptions {
  /** Attempts per delivery of a record before the error is handed to the client. Default 5. */
  readonly maxAttempts?: number;
  /** Delay before the second attempt; doubles each attempt. Default `'200ms'`. */
  readonly initialBackoff?: Duration;
  /** Upper bound of the delay between attempts. Default `'5s'`. */
  readonly maxBackoff?: Duration;
}

export interface KafkaTransportOptions {
  /** A `kafkajs` `Kafka` or `@confluentinc/kafka-javascript` `KafkaJS.Kafka` instance. */
  readonly kafka: KafkaClientLike;
  /** Name referenced by routes and subscriptions. Default `'kafka'`. */
  readonly name?: string;
  /** Consumer group id. Required as soon as `subscribe` is used. */
  readonly groupId?: string;
  /**
   * Where a group without committed offsets starts. Default `true` (earliest),
   * so events published before the first start are not skipped.
   */
  readonly fromBeginning?: boolean;
  /** Producer `maxInFlightRequests`, 1 to 5. Default 1, which keeps retries from reordering a key. */
  readonly maxInFlightRequests?: number;
  /** Extra producer settings; `idempotent` and `maxInFlightRequests` always win. */
  readonly producerConfig?: object;
  /** Extra consumer settings; `groupId` always wins. */
  readonly consumerConfig?: object;
  readonly processing?: KafkaProcessingOptions;
  /** How long `subscribe` waits for the consumer group join before failing. Default `'30s'`. */
  readonly startupTimeout?: Duration;
  /** Handling of undecodable records. Default `'skip'`. */
  readonly onInvalidMessage?: InvalidMessagePolicy;
  /** Default `console`. */
  readonly logger?: RelayBusLogger;
}

const DEFAULTS = {
  name: 'kafka',
  maxInFlightRequests: 1,
  maxAttempts: 5,
  initialBackoff: '200ms',
  maxBackoff: '5s',
  startupTimeout: '30s',
} as const;

type TransportState = 'idle' | 'subscribed' | 'closed';

interface Settings {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly startupTimeoutMs: number;
}

/**
 * Kafka {@link Transport}. Events travel as CloudEvents binary-mode records
 * (`ce_*` headers, JSON value), so any CloudEvents-aware consumer can read them.
 *
 * Publishing: idempotent producer, `acks=-1`, resolves after the broker ack.
 * The record key is the envelope key, so events sharing a key land on one
 * partition and keep their order; events without a key have no ordering.
 *
 * Subscribing: one consumer, one group, one `subscribe` call. `eachMessage`
 * awaits the handler; the client commits the offset only after it resolved. A
 * failing handler is retried in-process (bounded, with backoff) and then the
 * error is rethrown to the client, which does not commit the offset and
 * redelivers the record. A record is therefore never skipped because of a
 * handler failure; the partition simply waits. That is acceptable because the
 * handler is `RelayBus.receive`, a single idempotent insert.
 */
export class KafkaTransport implements Transport {
  readonly name: string;
  readonly #kafka: KafkaClientLike;
  readonly #groupId: string | undefined;
  readonly #fromBeginning: boolean;
  readonly #maxInFlightRequests: number;
  readonly #producerConfig: object;
  readonly #consumerConfig: object;
  readonly #invalidPolicy: InvalidMessagePolicy;
  readonly #logger: RelayBusLogger;
  readonly #settings: Settings;
  readonly #sleepers = new Set<() => void>();
  #state: TransportState = 'idle';
  #producer: Promise<KafkaProducerLike> | undefined;
  #consumer: KafkaConsumerLike | undefined;
  #groupJoined = false;
  #stalled = false;
  #joinWaiter: { resolve(): void; reject(error: unknown): void } | undefined;

  constructor(options: KafkaTransportOptions) {
    this.name = options.name ?? DEFAULTS.name;
    this.#kafka = options.kafka;
    this.#groupId = options.groupId;
    this.#fromBeginning = options.fromBeginning ?? true;
    this.#maxInFlightRequests = options.maxInFlightRequests ?? DEFAULTS.maxInFlightRequests;
    if (
      !Number.isInteger(this.#maxInFlightRequests) ||
      this.#maxInFlightRequests < 1 ||
      this.#maxInFlightRequests > 5
    ) {
      throw new ConfigurationError(
        `maxInFlightRequests must be an integer from 1 to 5, received ${this.#maxInFlightRequests}`,
      );
    }
    this.#producerConfig = options.producerConfig ?? {};
    this.#consumerConfig = options.consumerConfig ?? {};
    this.#invalidPolicy = options.onInvalidMessage ?? 'skip';
    this.#logger = options.logger ?? console;
    this.#settings = resolveSettings(options);
  }

  /**
   * True while the consumer is joined to its group, not between a crash or
   * rebalance and the next join, and not stalled on a record whose handler keeps
   * failing. Use it for readiness so a consumer that silently stopped is noticed.
   */
  isConsuming(): boolean {
    return this.#state === 'subscribed' && this.#groupJoined && !this.#stalled;
  }

  /** Connects the producer. Called by the bus on start; `publish` also connects on demand. */
  async connect(): Promise<void> {
    await this.#connectedProducer();
  }

  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    const producer = await this.#connectedProducer();
    const { key, value, headers } = encodeEnvelope(envelope);
    await producer.send({ topic: destination, acks: -1, messages: [{ key, value, headers }] });
  }

  /**
   * Starts the consumer and resolves once it joined its group, so a start-up
   * that cannot consume fails loudly instead of looking healthy.
   *
   * @throws ConfigurationError when `groupId` is missing, `destinations` is empty
   * or the transport already subscribed (create one transport per subscription).
   */
  async subscribe(
    destinations: readonly string[],
    onMessage: TransportMessageHandler,
  ): Promise<void> {
    this.#assertCanSubscribe(destinations);
    this.#state = 'subscribed';
    const consumer = this.#kafka.consumer({ ...this.#consumerConfig, groupId: this.#groupId! });
    this.#consumer = consumer;
    this.#observeConsumer(consumer);
    await consumer.connect();
    await consumer.subscribe({ topics: [...destinations], fromBeginning: this.#fromBeginning });
    const joined = this.#waitForGroupJoin(consumer);
    try {
      await consumer.run({ eachMessage: (payload) => this.#handleMessage(payload, onMessage) });
      await joined.promise;
    } finally {
      joined.dispose();
    }
    this.#logger.log(
      `[KafkaTransport](subscribe) Consuming | transport=${this.name} group=${this.#groupId} topics=${destinations.join(',')}`,
    );
  }

  /** Stops consuming and disconnects the consumer and the producer. Safe to call twice. */
  async close(): Promise<void> {
    if (this.#state === 'closed') {
      return;
    }
    this.#state = 'closed';
    this.#groupJoined = false;
    for (const wake of this.#sleepers) {
      wake();
    }
    const producer = await this.#producer?.catch(() => undefined);
    const results = await Promise.allSettled([
      this.#consumer?.disconnect(),
      producer?.disconnect(),
    ]);
    const failure = results.find((result) => result.status === 'rejected');
    if (failure !== undefined) {
      throw failure.reason;
    }
  }

  #assertCanSubscribe(destinations: readonly string[]): void {
    if (this.#state === 'closed') {
      throw new RelayBusError(`Transport '${this.name}' is closed`);
    }
    if (this.#state === 'subscribed') {
      throw new ConfigurationError(
        `Transport '${this.name}' already subscribed; list every topic in one subscription or use one KafkaTransport per consumer group`,
      );
    }
    if (this.#groupId === undefined || this.#groupId.length === 0) {
      throw new ConfigurationError(`Transport '${this.name}' needs a groupId to subscribe`);
    }
    if (destinations.length === 0) {
      throw new ConfigurationError(`Transport '${this.name}' subscribed to no topics`);
    }
    const policy = this.#invalidPolicy;
    if (typeof policy === 'object' && destinations.includes(policy.deadLetterTopic)) {
      throw new ConfigurationError(
        `Dead letter topic '${policy.deadLetterTopic}' is also consumed, which would loop`,
      );
    }
  }

  #connectedProducer(): Promise<KafkaProducerLike> {
    if (this.#state === 'closed') {
      return Promise.reject(new RelayBusError(`Transport '${this.name}' is closed`));
    }
    this.#producer ??= this.#openProducer();
    return this.#producer;
  }

  async #openProducer(): Promise<KafkaProducerLike> {
    const producer = this.#kafka.producer({
      ...this.#producerConfig,
      idempotent: true,
      maxInFlightRequests: this.#maxInFlightRequests,
    });
    try {
      await producer.connect();
    } catch (error) {
      this.#producer = undefined;
      throw error;
    }
    return producer;
  }

  async #handleMessage(
    payload: KafkaEachMessagePayload,
    onMessage: TransportMessageHandler,
  ): Promise<void> {
    let envelope: EventEnvelope;
    try {
      envelope = decodeEnvelope(payload.message);
    } catch (error) {
      if (!(error instanceof InvalidKafkaMessageError)) {
        throw error;
      }
      await this.#withRetries(payload, 'handleInvalid', () =>
        this.#handleInvalid(payload, error.message),
      );
      return;
    }
    await this.#withRetries(payload, 'deliver', async () => {
      try {
        await onMessage(envelope);
      } catch (error) {
        // The bus rejected the event for good; redelivering it would block the partition forever.
        if (!(error instanceof InvalidEnvelopeError)) {
          throw error;
        }
        await this.#handleInvalid(payload, error.message);
      }
    });
  }

  async #handleInvalid(payload: KafkaEachMessagePayload, reason: string): Promise<void> {
    const { topic, partition, message } = payload;
    const location = `topic=${topic} partition=${partition} offset=${message.offset}`;
    const policy = this.#invalidPolicy;
    if (policy === 'skip') {
      this.#logger.error(
        `[KafkaTransport](handleInvalid) Skipping malformed message | ${location} reason=${reason}`,
      );
    } else if (typeof policy === 'function') {
      await policy({
        topic,
        partition,
        offset: message.offset,
        reason,
        key: message.key,
        value: message.value,
      });
    } else {
      const producer = await this.#connectedProducer();
      await producer.send({
        topic: policy.deadLetterTopic,
        acks: -1,
        messages: [
          {
            key: message.key === null ? null : Buffer.from(message.key),
            value: message.value === null ? null : Buffer.from(message.value),
            headers: {
              ...toOutgoingHeaders(message.headers),
              'relaybus-invalid-reason': reason,
              'relaybus-source-topic': topic,
              'relaybus-source-partition': String(partition),
              'relaybus-source-offset': message.offset,
            },
          },
        ],
      });
      this.#logger.error(
        `[KafkaTransport](handleInvalid) Dead-lettered malformed message | ${location} deadLetterTopic=${policy.deadLetterTopic} reason=${reason}`,
      );
    }
  }

  /**
   * Runs `operation` up to `maxAttempts` times with exponential backoff. The
   * last error is rethrown so the client leaves the offset uncommitted and
   * redelivers the record; heartbeats between attempts keep the session alive.
   */
  async #withRetries(
    payload: KafkaEachMessagePayload,
    method: string,
    operation: () => Promise<void>,
  ): Promise<void> {
    const { maxAttempts, initialBackoffMs, maxBackoffMs } = this.#settings;
    const location = `topic=${payload.topic} partition=${payload.partition} offset=${payload.message.offset}`;
    for (let attempt = 1; ; attempt += 1) {
      try {
        await operation();
        this.#stalled = false;
        return;
      } catch (error) {
        const reason = toErrorInfo(error).message;
        if (attempt >= maxAttempts || this.#state === 'closed') {
          this.#stalled = true;
          this.#logger.error(
            `[KafkaTransport](${method}) Giving up for now, the record stays uncommitted and will be redelivered | ${location} attempts=${attempt} error=${reason}`,
          );
          throw error;
        }
        this.#logger.warn(
          `[KafkaTransport](${method}) Attempt failed, retrying | ${location} attempt=${attempt} error=${reason}`,
        );
        await this.#sleep(Math.min(initialBackoffMs * 2 ** (attempt - 1), maxBackoffMs));
        await payload.heartbeat();
      }
    }
  }

  #sleep(milliseconds: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = (): void => {
        clearTimeout(timer);
        this.#sleepers.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, milliseconds);
      this.#sleepers.add(wake);
    });
  }

  /** Whether the client exposes the instrumentation needed to follow the group membership. */
  #tracksMembership(consumer: KafkaConsumerLike): boolean {
    return consumer.on !== undefined && consumer.events?.GROUP_JOIN !== undefined;
  }

  #observeConsumer(consumer: KafkaConsumerLike): void {
    if (!this.#tracksMembership(consumer)) {
      this.#groupJoined = true;
      return;
    }
    const names = consumer.events!;
    const listen = (
      name: string | undefined,
      listener: (event: KafkaConsumerEvent) => void,
    ): void => {
      if (name !== undefined) {
        consumer.on!(name, listener);
      }
    };
    listen(names.GROUP_JOIN, () => {
      this.#groupJoined = true;
      this.#joinWaiter?.resolve();
      this.#logger.log(`[KafkaTransport](observeConsumer) Joined group | group=${this.#groupId}`);
    });
    listen(names.REBALANCING, () => {
      this.#groupJoined = false;
      this.#logger.warn(`[KafkaTransport](observeConsumer) Rebalancing | group=${this.#groupId}`);
    });
    listen(names.CRASH, (event) => this.#handleCrash(event));
    listen(names.STOP, () => {
      this.#groupJoined = false;
    });
    listen(names.DISCONNECT, () => {
      this.#groupJoined = false;
    });
  }

  #handleCrash(event: KafkaConsumerEvent): void {
    const { error, restart } = (event.payload ?? {}) as { error?: unknown; restart?: unknown };
    this.#groupJoined = false;
    this.#logger.error(
      `[KafkaTransport](handleCrash) Consumer crashed | group=${this.#groupId} restart=${String(restart)} error=${toErrorInfo(error).message}`,
    );
    this.#joinWaiter?.reject(error);
  }

  #waitForGroupJoin(consumer: KafkaConsumerLike): { promise: Promise<void>; dispose(): void } {
    if (!this.#tracksMembership(consumer)) {
      return { promise: Promise.resolve(), dispose: () => undefined };
    }
    let timer: NodeJS.Timeout | undefined;
    const promise = new Promise<void>((resolve, reject) => {
      this.#joinWaiter = { resolve, reject };
      timer = setTimeout(() => {
        reject(
          new RelayBusError(
            `Consumer of transport '${this.name}' did not join group '${this.#groupId}' within ${this.#settings.startupTimeoutMs}ms`,
          ),
        );
      }, this.#settings.startupTimeoutMs);
    });
    // The rejection may land while `run` is still pending; the caller awaits it afterwards.
    promise.catch(() => undefined);
    return {
      promise,
      dispose: () => {
        clearTimeout(timer);
        this.#joinWaiter = undefined;
      },
    };
  }
}

function resolveSettings(options: KafkaTransportOptions): Settings {
  const maxAttempts = options.processing?.maxAttempts ?? DEFAULTS.maxAttempts;
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new ConfigurationError(
      `processing.maxAttempts must be a positive integer, received ${maxAttempts}`,
    );
  }
  return {
    maxAttempts,
    initialBackoffMs: toMilliseconds(
      options.processing?.initialBackoff ?? DEFAULTS.initialBackoff,
      'processing.initialBackoff',
    ),
    maxBackoffMs: toMilliseconds(
      options.processing?.maxBackoff ?? DEFAULTS.maxBackoff,
      'processing.maxBackoff',
    ),
    startupTimeoutMs: toMilliseconds(
      options.startupTimeout ?? DEFAULTS.startupTimeout,
      'startupTimeout',
    ),
  };
}
