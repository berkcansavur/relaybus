import {
  NonRetryableError,
  toErrorInfo,
  type EventEnvelope,
  type TransportMessageHandler,
  type TransportSubscriptionHooks,
} from '@relaybus/core';
import { SubscribingTransport, type TransportSubscription } from '@relaybus/core/internal';
import { cloudEventsBinary } from './codecs.js';
import { InvalidKafkaMessageError } from './errors.js';
import { GroupJoinWatch } from './kafka-group-join.js';
import type { MembershipListeners } from './kafka-client-dialects.js';
import type {
  KafkaConsumerEvent,
  KafkaConsumerLike,
  KafkaEachMessagePayload,
} from './kafka-client.js';
import type { KafkaTransportOptions } from './kafka-options.js';
import { KafkaProducerSession } from './kafka-producer-session.js';
import {
  deadLetterTopicFor,
  resolveKafkaSettings,
  toHandlerFailure,
  type KafkaSettings,
} from './kafka-settings.js';
import {
  assertCanSubscribe,
  withDeclaredWireTypeSettings,
  type KafkaTransportState,
} from './kafka-subscription-rules.js';
import { decodeRecord, deadLetterRecord } from './kafka-records.js';
import { WakeableSleeps } from './wakeable-sleeps.js';

export type {
  InvalidKafkaMessage,
  InvalidMessagePolicy,
  KafkaInjectedProducer,
  KafkaProcessingOptions,
  KafkaRestartOptions,
  KafkaTransportOptions,
} from './kafka-options.js';

/** Where a consumed record goes, and what may hear about the records that cannot. */
type MessageRoute = {
  readonly onMessage: TransportMessageHandler;
  readonly hooks: TransportSubscriptionHooks | undefined;
};

/**
 * Kafka {@link Transport}. Events travel as CloudEvents binary-mode records
 * (`ce_*` headers, JSON value), so any CloudEvents-aware consumer can read them.
 * The `codec` option changes the wire format, for example to read and write
 * the JSON format of services that do not use relaybus.
 *
 * Publishing: idempotent producer, `acks=-1`, resolves after the broker ack and
 * rejects with {@link PublishTimeoutError} after `publishTimeout`. A timed-out
 * send may still complete later; the outbox then retries it and the duplicate is
 * absorbed by the consumer's inbox dedup. The record key is the codec's: the event key by default, so events
 * sharing a key land on one partition and keep their order; records without a key have no ordering.
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
export class KafkaTransport extends SubscribingTransport {
  readonly name: string;
  readonly system = 'kafka';
  #settings: KafkaSettings;
  readonly #producer: KafkaProducerSession;
  readonly #groupJoin: GroupJoinWatch;
  readonly #sleeps = new WakeableSleeps();
  #state: KafkaTransportState = 'idle';
  #consumer: KafkaConsumerLike | undefined;
  #groupJoined = false;
  #consuming = false;
  #restartLoop: Promise<void> | undefined;
  readonly #stalledPartitions = new Set<string>();
  #startConsuming: (() => Promise<void>) | undefined;

  constructor(options: KafkaTransportOptions) {
    super();
    this.#settings = resolveKafkaSettings(options);
    this.name = this.#settings.name;
    this.#producer = new KafkaProducerSession(this.#settings, () => this.#state === 'closed');
    this.#groupJoin = new GroupJoinWatch(this.#settings);
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
    if (this.#state === 'closed' || !this.#producer.isConnected) {
      return false;
    }
    return this.#state === 'idle' || this.isConsuming();
  }

  /** Connects the producer. Called by the bus on start; `publish` also connects on demand. */
  async connect(): Promise<void> {
    await this.#producer.connected();
  }

  /**
   * Checks the wire names a route or catalog declares and returns the function that makes the codec
   * write and read events under them. The bus calls it when it is created, before the transport
   * connects.
   *
   * @throws WireTypesUnsupportedError when the codec does not map type names (use `jsonEnvelope`),
   * WireTypeCollisionError when a name clashes with one of the codec's own `wireTypes`.
   */
  declareWireTypes(wireTypes: Readonly<Record<string, string>>): () => void {
    const settings = withDeclaredWireTypeSettings(this.#settings, {
      wireTypes,
      started: this.#state !== 'idle' || this.#producer.pending !== undefined,
    });
    return () => void (this.#settings = settings);
  }

  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    const producer = await this.#producer.connected();
    await this.#producer.send(producer, {
      topic: destination,
      acks: -1,
      messages: [this.#settings.codec.encode(envelope)],
    });
  }

  /**
   * Starts the consumer and resolves once it joined its group, so a start-up
   * that cannot consume fails loudly instead of looking healthy.
   *
   * @throws ConfigurationError when `groupId` is missing, `destinations` is empty
   * or the transport already subscribed (create one transport per subscription).
   */
  protected openSubscription(subscription: TransportSubscription): Promise<void> {
    return this.#subscribe(subscription);
  }

  async #subscribe({ destinations, onMessage, hooks }: TransportSubscription): Promise<void> {
    assertCanSubscribe(this.#state, { settings: this.#settings, destinations });
    this.#state = 'subscribed';
    const { kafka, dialect, groupId } = this.#settings;
    const membership = this.#observeConsumer();
    const consumer = kafka!.consumer(dialect.consumerConfig(this.#settings, membership));
    this.#consumer = consumer;
    this.#groupJoined = !dialect.tracksMembership(consumer);
    dialect.observe(consumer, membership);
    const start = (): Promise<void> =>
      this.#groupJoin.start(consumer, {
        topics: [...destinations],
        fromBeginning: this.#settings.fromBeginning,
        eachMessage: (payload) => this.#handleMessage(payload, { onMessage, hooks }),
      });
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
    this.#settings.logger.log(
      `[KafkaTransport](subscribe) Consuming | transport=${this.name} group=${groupId} topics=${destinations.join(',')}`,
    );
  }

  /** Stops consuming and disconnects the consumer and the producer. Safe to call twice. */
  async close(): Promise<void> {
    if (this.#state === 'closed') {
      return;
    }
    this.#state = 'closed';
    const pendingProducer = this.#producer.pending;
    this.#groupJoined = false;
    this.#producer.markClosed();
    this.#sleeps.wakeAll();
    this.#groupJoin.closed();
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

  async #handleMessage(payload: KafkaEachMessagePayload, route: MessageRoute): Promise<void> {
    let envelope: EventEnvelope;
    try {
      envelope = decodeRecord(this.#settings.codec, payload);
    } catch (error) {
      // The default codec throws only InvalidKafkaMessageError for bad input; anything else is a bug.
      if (
        this.#settings.codec === cloudEventsBinary &&
        !(error instanceof InvalidKafkaMessageError)
      ) {
        throw toHandlerFailure(error);
      }
      route.hooks?.onInvalidMessage({ destination: payload.topic, error });
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
          await route.onMessage(envelope, { destination: payload.topic });
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

  async #handleInvalid(payload: KafkaEachMessagePayload, error: unknown): Promise<void> {
    const { topic, partition, message } = payload;
    const { message: reason, name, code } = toErrorInfo(error);
    const location = `topic=${topic} partition=${partition} offset=${message.offset}`;
    const errorFields = `errorName=${name}${code === undefined ? '' : ` code=${code}`}`;
    const policy = this.#settings.invalidPolicy;
    const deadLetterTopic = deadLetterTopicFor(policy, topic);
    if (policy === 'skip') {
      this.#settings.logger.error(
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
      const record = deadLetterRecord(payload, {
        reason,
        name,
        code,
        deadLetterTopic: deadLetterTopic!,
      });
      const producer = await this.#producer.connected();
      await this.#producer.send(producer, record);
      this.#settings.logger.error(
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
    const { maxAttempts, processingBackoff } = this.#settings.timings;
    const { logger } = this.#settings;
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
          logger.error(
            `[KafkaTransport](withRetries) Giving up for now, the record stays uncommitted and will be redelivered | operation=${method} ${location} attempts=${attempt} error=${reason}`,
          );
          throw toHandlerFailure(error);
        }
        logger.warn(
          `[KafkaTransport](withRetries) Attempt failed, retrying | operation=${method} ${location} attempt=${attempt} error=${reason}`,
        );
        await this.#sleeps.sleepInSlices(processingBackoff.delayFor(attempt), {
          sliceMs: this.#settings.timings.heartbeatIntervalMs,
          between: () => payload.heartbeat(),
          stopped: () => this.#state === 'closed',
        });
      }
    }
  }

  /** What the transport does when the consumer joins, rebalances, crashes or leaves its group. */
  #observeConsumer(): MembershipListeners {
    const { logger, groupId } = this.#settings;
    return {
      joined: () => {
        this.#groupJoined = true;
        this.#groupJoin.joined();
        logger.log(`[KafkaTransport](observeConsumer) Joined group | group=${groupId}`);
      },
      rebalancing: () => {
        this.#groupJoined = false;
        // The partition assignment is about to change; a stall is re-detected if it persists.
        this.#stalledPartitions.clear();
        logger.warn(`[KafkaTransport](observeConsumer) Rebalancing | group=${groupId}`);
      },
      crashed: (event) => this.#handleCrash(event),
      left: () => {
        this.#groupJoined = false;
      },
    };
  }

  #handleCrash(event: KafkaConsumerEvent): void {
    const { error, restart } = (event.payload ?? {}) as { error?: unknown; restart?: unknown };
    this.#groupJoined = false;
    this.#settings.logger.error(
      `[KafkaTransport](handleCrash) Consumer crashed | group=${this.#settings.groupId} restart=${String(restart)} error=${toErrorInfo(error).message}`,
    );
    this.#groupJoin.failed(error);
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
    const { logger, groupId, timings } = this.#settings;
    const consumer = this.#consumer!;
    for (let attempt = 1; this.#state === 'subscribed'; attempt += 1) {
      await this.#sleeps.sleep(timings.restartBackoff.delayFor(attempt));
      if (this.#state !== 'subscribed') {
        return;
      }
      await consumer.disconnect().catch(() => undefined);
      try {
        await this.#startConsuming!();
        logger.log(
          `[KafkaTransport](restartConsumer) Consumer restarted | group=${groupId} attempt=${attempt}`,
        );
        return;
      } catch (error) {
        logger.error(
          `[KafkaTransport](restartConsumer) Restart failed, will retry | group=${groupId} attempt=${attempt} error=${toErrorInfo(error).message}`,
        );
      }
    }
  }
}
