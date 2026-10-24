import {
  ConfigurationError,
  systemClock,
  NonRetryableError,
  toErrorInfo,
  toMilliseconds,
  type Duration,
  type EventEnvelope,
  type RelayBusLogger,
  type Transport,
  type TransportMessageHandler,
} from '@relaybus/core';
import { assertPositiveInteger, schedule, type CancelTimer } from '@relaybus/core/internal';
import { decodeEnvelope, encodeEnvelope, toOutgoingHeaders } from './cloudevents-codec.js';
import {
  GroupJoinTimeoutError,
  HandlerFailedError,
  InvalidKafkaMessageError,
  PublishTimeoutError,
  TransportClosedError,
} from './errors.js';
import { completeDecodedEvent, type KafkaMessageDecoder } from './inbound-decoders.js';
import type {
  KafkaClientLike,
  KafkaConsumerEvent,
  KafkaConsumerLike,
  KafkaEachMessagePayload,
  KafkaProducerLike,
  KafkaProducerRecord,
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
 * What to do with a record that cannot be processed for good (it cannot be
 * decoded, or the bus rejected it with a `NonRetryableError`):
 * - `'dead-letter'` (default): the original record is produced to
 *   `<source topic>.dead` with `relaybus-invalid-reason`, `relaybus-source-topic`,
 *   `relaybus-source-partition` and `relaybus-source-offset` headers added to the
 *   original ones; the offset advances only after the broker acked the copy.
 * - `{ deadLetterTopic }`: same, with a fixed topic name or a function of the
 *   source topic.
 * - a function: called with the record; if it rejects, the record is retried
 *   like any other failure, so nothing is dropped unless the function returns.
 * - `'skip'`: log an error and drop the record. Explicit opt-in only.
 *
 * The dead-letter topic must exist or the broker must auto-create topics;
 * otherwise the send fails and the record stays uncommitted (nothing is lost).
 */
export type InvalidMessagePolicy =
  | 'dead-letter'
  | 'skip'
  | ((message: InvalidKafkaMessage) => void | Promise<void>)
  | { readonly deadLetterTopic: string | ((sourceTopic: string) => string) };

/** Bounded in-process retries around the inbound handler (the bus's `receive`). */
export interface KafkaProcessingOptions {
  /** Attempts per delivery of a record before the error is handed to the client. Default 5. */
  readonly maxAttempts?: number;
  /** Delay before the second attempt; doubles each attempt. Default `'200ms'`. */
  readonly initialBackoff?: Duration;
  /** Upper bound of the delay between attempts. Default `'5s'`. */
  readonly maxBackoff?: Duration;
  /** Interval of the heartbeats sent while sleeping between attempts. Default `'1s'`. */
  readonly heartbeatInterval?: Duration;
}

/** Backoff of the consumer restart loop that runs after an unrecoverable crash. */
export interface KafkaRestartOptions {
  /** Delay before the second restart attempt; doubles each failure. Default `'1s'`. */
  readonly initialBackoff?: Duration;
  /** Upper bound of the delay between restart attempts. Default `'30s'`. */
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
  /** How long `publish` waits for the broker ack before rejecting. Default `'30s'`. */
  readonly publishTimeout?: Duration;
  readonly restart?: KafkaRestartOptions;
  /** Handling of records that cannot be processed for good. Default `'dead-letter'`. */
  readonly onInvalidMessage?: InvalidMessagePolicy;
  /**
   * How consumed records become events. Default: strict CloudEvents binary
   * mode ({@link decodeEnvelope}), which is what relaybus producers write; any
   * other record is malformed and goes to `onInvalidMessage`. Set it to read
   * topics written by services that do not use relaybus:
   * {@link decodeCloudEvent} also accepts structured mode, {@link plainJsonDecoder}
   * maps plain JSON records, and a function of your own can do anything else
   * (switch on `record.topic` to decode topics differently). Whatever a custom
   * decoder throws marks the record as malformed, never crashes the consumer.
   */
  readonly decode?: KafkaMessageDecoder;
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
  publishTimeout: '30s',
  heartbeatInterval: '1s',
  restartInitialBackoff: '1s',
  restartMaxBackoff: '30s',
} as const;

type TransportState = 'idle' | 'subscribed' | 'closed';

interface Settings {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly startupTimeoutMs: number;
  readonly publishTimeoutMs: number;
  readonly heartbeatIntervalMs: number;
  readonly restartInitialBackoffMs: number;
  readonly restartMaxBackoffMs: number;
}

/**
 * Kafka {@link Transport}. Events travel as CloudEvents binary-mode records
 * (`ce_*` headers, JSON value), so any CloudEvents-aware consumer can read them.
 * Records from producers that do not use relaybus are read through the
 * `decode` option.
 *
 * Publishing: idempotent producer, `acks=-1`, resolves after the broker ack and
 * rejects with {@link PublishTimeoutError} after `publishTimeout`. A timed-out
 * send may still complete later; the outbox then retries it and the duplicate is
 * absorbed by the consumer's inbox dedup. The record key is the envelope key, so events sharing a key land on one
 * partition and keep their order; events without a key have no ordering.
 *
 * Subscribing: one consumer, one group, one `subscribe` call. `eachMessage`
 * awaits the handler; the client commits the offset only after it resolved. A
 * failing handler is retried in-process (bounded, with backoff) and then the
 * error is rethrown (as a {@link HandlerFailedError}, so the client never treats
 * it as fatal) and the client does not commit the offset and redelivers the
 * record. A record is therefore never skipped because of a handler failure; the
 * partition simply waits. Records that can never succeed (undecodable, or
 * rejected with a `NonRetryableError`) are dead-lettered instead. If the client
 * reports an unrecoverable crash anyway, the consumer run loop is restarted with
 * bounded backoff until the transport is closed.
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
  readonly #decode: KafkaMessageDecoder | undefined;
  readonly #logger: RelayBusLogger;
  readonly #settings: Settings;
  readonly #sleepers = new Set<() => void>();
  #state: TransportState = 'idle';
  #producer: Promise<KafkaProducerLike> | undefined;
  #consumer: KafkaConsumerLike | undefined;
  #groupJoined = false;
  #consuming = false;
  #producerConnected = false;
  #restartLoop: Promise<void> | undefined;
  readonly #stalledPartitions = new Set<string>();
  #startConsuming: (() => Promise<void>) | undefined;
  #joinWaiter: { resolve(): void; reject(error: unknown): void } | undefined;

  constructor(options: KafkaTransportOptions) {
    this.name = options.name ?? DEFAULTS.name;
    this.#kafka = options.kafka;
    this.#groupId = options.groupId;
    this.#fromBeginning = options.fromBeginning ?? true;
    this.#maxInFlightRequests = assertMaxInFlightRequests(
      options.maxInFlightRequests ?? DEFAULTS.maxInFlightRequests,
    );
    this.#producerConfig = options.producerConfig ?? {};
    this.#consumerConfig = options.consumerConfig ?? {};
    this.#invalidPolicy = options.onInvalidMessage ?? 'dead-letter';
    this.#decode = options.decode;
    this.#logger = options.logger ?? console;
    this.#settings = resolveSettings(options);
  }

  /**
   * True while the consumer is joined to its group, not between a crash or
   * rebalance and the next join, and no partition is stalled on a record whose
   * handler keeps failing. Use it for readiness so a consumer that silently
   * stopped is noticed.
   */
  isConsuming(): boolean {
    return this.#state === 'subscribed' && this.#groupJoined && this.#stalledPartitions.size === 0;
  }

  /**
   * True once the producer is connected and, if the transport subscribed, the
   * consumer is consuming. False before `connect`, after `close` and while a
   * dead consumer is being restarted.
   */
  isReady(): boolean {
    if (this.#state === 'closed' || !this.#producerConnected) {
      return false;
    }
    return this.#state === 'idle' || this.isConsuming();
  }

  /** Connects the producer. Called by the bus on start; `publish` also connects on demand. */
  async connect(): Promise<void> {
    await this.#connectedProducer();
  }

  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    const producer = await this.#connectedProducer();
    const { key, value, headers } = encodeEnvelope(envelope);
    await this.#send(producer, {
      topic: destination,
      acks: -1,
      messages: [{ key, value, headers }],
    });
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
    const start = (): Promise<void> =>
      this.#startConsumer(consumer, { destinations: [...destinations], onMessage });
    this.#startConsuming = start;
    try {
      await start();
    } catch (error) {
      // A consumer left half-started would keep running (or retrying) with nobody watching it.
      await consumer.disconnect().catch(() => undefined);
      this.#groupJoined = false;
      this.#consumer = undefined;
      this.#state = 'idle';
      throw error;
    }
    this.#consuming = true;
    this.#logger.log(
      `[KafkaTransport](subscribe) Consuming | transport=${this.name} group=${this.#groupId} topics=${destinations.join(',')}`,
    );
  }

  async #startConsumer(
    consumer: KafkaConsumerLike,
    {
      destinations,
      onMessage,
    }: { readonly destinations: string[]; readonly onMessage: TransportMessageHandler },
  ): Promise<void> {
    await consumer.connect();
    await consumer.subscribe({ topics: destinations, fromBeginning: this.#fromBeginning });
    const joined = this.#waitForGroupJoin(consumer);
    try {
      await consumer.run({ eachMessage: (payload) => this.#handleMessage(payload, onMessage) });
      await joined.promise;
    } finally {
      joined.dispose();
    }
  }

  /** Stops consuming and disconnects the consumer and the producer. Safe to call twice. */
  async close(): Promise<void> {
    if (this.#state === 'closed') {
      return;
    }
    this.#state = 'closed';
    const pendingProducer = this.#producer;
    this.#groupJoined = false;
    this.#producerConnected = false;
    for (const wake of this.#sleepers) {
      wake();
    }
    this.#joinWaiter?.reject(new TransportClosedError(`Transport '${this.name}' is closed`));
    await this.#restartLoop;
    const producer = await pendingProducer?.catch(() => undefined);
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
      throw new TransportClosedError(`Transport '${this.name}' is closed`);
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
    for (const topic of destinations) {
      const deadLetterTopic = this.#deadLetterTopicFor(topic);
      if (deadLetterTopic !== undefined && destinations.includes(deadLetterTopic)) {
        throw new ConfigurationError(
          `Dead letter topic '${deadLetterTopic}' is also consumed, which would loop`,
        );
      }
    }
  }

  /** The dead-letter topic for records of `sourceTopic`, or undefined when the policy does not use one. */
  #deadLetterTopicFor(sourceTopic: string): string | undefined {
    const policy = this.#invalidPolicy;
    if (policy === 'dead-letter') {
      return `${sourceTopic}.dead`;
    }
    if (typeof policy === 'object') {
      const { deadLetterTopic } = policy;
      return typeof deadLetterTopic === 'function' ? deadLetterTopic(sourceTopic) : deadLetterTopic;
    }
    return undefined;
  }

  /** Sends with the publish timeout; the late outcome of a timed-out send is ignored. */
  async #send(producer: KafkaProducerLike, record: KafkaProducerRecord): Promise<void> {
    let cancel: CancelTimer | undefined;
    const timeout = new Promise<never>((_resolve, reject) => {
      cancel = schedule(systemClock, {
        run: () => {
          reject(
            new PublishTimeoutError(
              `Transport '${this.name}' got no broker ack for topic '${record.topic}' within ${this.#settings.publishTimeoutMs}ms`,
            ),
          );
        },
        delayMs: this.#settings.publishTimeoutMs,
      });
    });
    const sending = producer.send(record);
    sending.catch(() => undefined);
    try {
      await Promise.race([sending, timeout]);
    } finally {
      cancel?.();
    }
  }

  #connectedProducer(): Promise<KafkaProducerLike> {
    if (this.#state === 'closed') {
      return Promise.reject(new TransportClosedError(`Transport '${this.name}' is closed`));
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
    this.#producerConnected = this.#state !== 'closed';
    return producer;
  }

  async #handleMessage(
    payload: KafkaEachMessagePayload,
    onMessage: TransportMessageHandler,
  ): Promise<void> {
    let envelope: EventEnvelope;
    try {
      envelope = this.#decodeRecord(payload);
    } catch (error) {
      // The default decoder throws only InvalidKafkaMessageError for bad input; anything else is a bug.
      if (this.#decode === undefined && !(error instanceof InvalidKafkaMessageError)) {
        throw toHandlerFailure(error);
      }
      await this.#withRetries(payload, {
        method: 'handleInvalid',
        operation: () => this.#handleInvalid(payload, error),
      });
      return;
    }
    await this.#withRetries(payload, {
      method: 'deliver',
      operation: async () => {
        try {
          await onMessage(envelope);
        } catch (error) {
          // The bus rejected the event for good; redelivering it would block the partition forever.
          if (!(error instanceof NonRetryableError)) {
            throw error;
          }
          await this.#handleInvalid(payload, error);
        }
      },
    });
  }

  #decodeRecord({ topic, partition, message }: KafkaEachMessagePayload): EventEnvelope {
    if (this.#decode === undefined) {
      return decodeEnvelope(message);
    }
    const record = {
      topic,
      partition,
      offset: message.offset,
      key: message.key,
      value: message.value,
      headers: message.headers,
      timestamp: message.timestamp,
    };
    return completeDecodedEvent(this.#decode(record), record);
  }

  async #handleInvalid(payload: KafkaEachMessagePayload, error: unknown): Promise<void> {
    const { topic, partition, message } = payload;
    const { message: reason, name, code } = toErrorInfo(error);
    const location = `topic=${topic} partition=${partition} offset=${message.offset}`;
    const errorFields = `errorName=${name}${code === undefined ? '' : ` code=${code}`}`;
    const policy = this.#invalidPolicy;
    const deadLetterTopic = this.#deadLetterTopicFor(topic);
    if (policy === 'skip') {
      this.#logger.error(
        `[KafkaTransport](handleInvalid) Skipping malformed message | ${location} reason=${reason} ${errorFields}`,
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
      const headers: Record<string, Buffer | string | (Buffer | string)[]> = {
        ...toOutgoingHeaders(message.headers),
        'relaybus-invalid-reason': reason,
        'relaybus-source-topic': topic,
        'relaybus-source-partition': String(partition),
        'relaybus-source-offset': message.offset,
        'relaybus-error-name': name,
      };
      if (code === undefined) {
        delete headers['relaybus-error-code'];
      } else {
        headers['relaybus-error-code'] = code;
      }
      const producer = await this.#connectedProducer();
      await this.#send(producer, {
        topic: deadLetterTopic!,
        acks: -1,
        messages: [
          {
            key: message.key === null ? null : Buffer.from(message.key),
            value: message.value === null ? null : Buffer.from(message.value),
            headers,
          },
        ],
      });
      this.#logger.error(
        `[KafkaTransport](handleInvalid) Dead-lettered malformed message | ${location} deadLetterTopic=${deadLetterTopic} reason=${reason} ${errorFields}`,
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
    { method, operation }: { readonly method: string; readonly operation: () => Promise<void> },
  ): Promise<void> {
    const { maxAttempts, initialBackoffMs, maxBackoffMs } = this.#settings;
    const location = `topic=${payload.topic} partition=${payload.partition} offset=${payload.message.offset}`;
    const partitionKey = `${payload.topic}:${payload.partition}`;
    for (let attempt = 1; ; attempt += 1) {
      try {
        await operation();
        this.#stalledPartitions.delete(partitionKey);
        return;
      } catch (error) {
        const reason = toErrorInfo(error).message;
        if (attempt >= maxAttempts || this.#state === 'closed') {
          this.#stalledPartitions.add(partitionKey);
          this.#logger.error(
            `[KafkaTransport](withRetries) Giving up for now, the record stays uncommitted and will be redelivered | operation=${method} ${location} attempts=${attempt} error=${reason}`,
          );
          throw toHandlerFailure(error);
        }
        this.#logger.warn(
          `[KafkaTransport](withRetries) Attempt failed, retrying | operation=${method} ${location} attempt=${attempt} error=${reason}`,
        );
        await this.#sleepWithHeartbeats(
          Math.min(initialBackoffMs * 2 ** (attempt - 1), maxBackoffMs),
          payload,
        );
      }
    }
  }

  /** Sleeps in slices and heartbeats after each, so a long backoff never outlives the session timeout. */
  async #sleepWithHeartbeats(
    milliseconds: number,
    payload: KafkaEachMessagePayload,
  ): Promise<void> {
    let remaining = milliseconds;
    do {
      const slice = Math.min(remaining, this.#settings.heartbeatIntervalMs);
      await this.#sleep(slice);
      await payload.heartbeat();
      remaining -= slice;
    } while (remaining > 0 && this.#state !== 'closed');
  }

  #sleep(milliseconds: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = (): void => {
        cancel();
        this.#sleepers.delete(wake);
        resolve();
      };
      const cancel = schedule(systemClock, { run: wake, delayMs: milliseconds });
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
      // The partition assignment is about to change; a stall is re-detected if it persists.
      this.#stalledPartitions.clear();
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
    if (restart === false && this.#consuming && this.#restartLoop === undefined) {
      this.#restartLoop = this.#restartConsumer().finally(() => {
        this.#restartLoop = undefined;
      });
    }
  }

  /**
   * The client gave up on the consumer (`restart=false`): reconnect it and run
   * again, with bounded backoff, until it joined or the transport closed.
   */
  async #restartConsumer(): Promise<void> {
    const { restartInitialBackoffMs, restartMaxBackoffMs } = this.#settings;
    const consumer = this.#consumer!;
    for (let attempt = 1; this.#state === 'subscribed'; attempt += 1) {
      await this.#sleep(
        Math.min(restartInitialBackoffMs * 2 ** (attempt - 1), restartMaxBackoffMs),
      );
      if (this.#state !== 'subscribed') {
        return;
      }
      await consumer.disconnect().catch(() => undefined);
      try {
        await this.#startConsuming!();
        this.#logger.log(
          `[KafkaTransport](restartConsumer) Consumer restarted | group=${this.#groupId} attempt=${attempt}`,
        );
        return;
      } catch (error) {
        this.#logger.error(
          `[KafkaTransport](restartConsumer) Restart failed, will retry | group=${this.#groupId} attempt=${attempt} error=${toErrorInfo(error).message}`,
        );
      }
    }
  }

  #waitForGroupJoin(consumer: KafkaConsumerLike): { promise: Promise<void>; dispose(): void } {
    if (!this.#tracksMembership(consumer)) {
      return { promise: Promise.resolve(), dispose: () => undefined };
    }
    let cancel: CancelTimer | undefined;
    const promise = new Promise<void>((resolve, reject) => {
      this.#joinWaiter = { resolve, reject };
      cancel = schedule(systemClock, {
        run: () => {
          reject(
            new GroupJoinTimeoutError(
              `Consumer of transport '${this.name}' did not join group '${this.#groupId}' within ${this.#settings.startupTimeoutMs}ms`,
            ),
          );
        },
        delayMs: this.#settings.startupTimeoutMs,
      });
    });
    // The rejection may land while `run` is still pending; the caller awaits it afterwards.
    promise.catch(() => undefined);
    return {
      promise,
      dispose: () => {
        cancel?.();
        this.#joinWaiter = undefined;
      },
    };
  }
}

function resolveSettings(options: KafkaTransportOptions): Settings {
  const processing: KafkaProcessingOptions = options.processing ?? {};
  const maxAttempts = assertPositiveInteger(
    processing.maxAttempts ?? DEFAULTS.maxAttempts,
    'processing.maxAttempts',
  );
  return {
    maxAttempts,
    initialBackoffMs: toMilliseconds(
      processing.initialBackoff ?? DEFAULTS.initialBackoff,
      'processing.initialBackoff',
    ),
    maxBackoffMs: toMilliseconds(
      processing.maxBackoff ?? DEFAULTS.maxBackoff,
      'processing.maxBackoff',
    ),
    startupTimeoutMs: toMilliseconds(
      options.startupTimeout ?? DEFAULTS.startupTimeout,
      'startupTimeout',
    ),
    publishTimeoutMs: toMilliseconds(
      options.publishTimeout ?? DEFAULTS.publishTimeout,
      'publishTimeout',
    ),
    heartbeatIntervalMs: Math.max(
      1,
      toMilliseconds(
        processing.heartbeatInterval ?? DEFAULTS.heartbeatInterval,
        'processing.heartbeatInterval',
      ),
    ),
    ...resolveRestartSettings(options.restart ?? {}),
  };
}

function resolveRestartSettings(
  restart: KafkaRestartOptions,
): Pick<Settings, 'restartInitialBackoffMs' | 'restartMaxBackoffMs'> {
  return {
    restartInitialBackoffMs: toMilliseconds(
      restart.initialBackoff ?? DEFAULTS.restartInitialBackoff,
      'restart.initialBackoff',
    ),
    restartMaxBackoffMs: toMilliseconds(
      restart.maxBackoff ?? DEFAULTS.restartMaxBackoff,
      'restart.maxBackoff',
    ),
  };
}

/** Largest `maxInFlightRequests`. */
const MAXIMUM_IN_FLIGHT_REQUESTS = 5;

/** Returns `value` when it is an integer from 1 to 5. */
function assertMaxInFlightRequests(value: number): number {
  if (!Number.isInteger(value) || value < 1 || value > MAXIMUM_IN_FLIGHT_REQUESTS) {
    throw new ConfigurationError(
      `maxInFlightRequests must be an integer from 1 to 5, received ${value}`,
    );
  }
  return value;
}

/**
 * Wraps a failure in a fresh error before it goes back to the Kafka client.
 * kafkajs classifies errors by type and cause chain: a handler that throws a
 * `TypeError` would be marked unrecoverable and kill the consumer for good.
 * The wrapper carries the original name and message but no `cause`.
 */
function toHandlerFailure(error: unknown): Error {
  const { name, message } = toErrorInfo(error);
  return new HandlerFailedError(`${name}: ${message}`);
}
