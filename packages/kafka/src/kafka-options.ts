import type { Duration, RelayBusLogger } from '@relaybus/core';
import type { KafkaCodec } from './codecs.js';
import type { KafkaMessageDecoder } from './inbound-decoders.js';
import type { KafkaClientLike, KafkaProducerLike } from './kafka-client.js';
import type { KafkaClientName } from './kafka-client-dialects.js';

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

/**
 * A producer the application already connected and owns, such as the one of its own Kafka
 * service: a kafkajs `Producer` or a Confluent `KafkaJS.Producer`. Only `send` is called.
 */
export type KafkaInjectedProducer = Pick<KafkaProducerLike, 'send'>;

export interface KafkaTransportOptions {
  /**
   * A `kafkajs` `Kafka` or `@confluentinc/kafka-javascript` `KafkaJS.Kafka` instance: the
   * factory of the transport's own consumer and, unless `producer` is given, producer.
   * Required unless `producer` is given; required to subscribe.
   */
  readonly kafka?: KafkaClientLike;
  /**
   * Publish (and dead-letter) through this already-connected producer instead of one of
   * `kafka`, reusing the application's connection and its SSL/SASL settings. The transport never
   * connects or disconnects it; `close` leaves it open. Make it idempotent with `acks` -1 (all
   * in-sync replicas) and at most 5 in-flight requests (1 to keep retries from reordering), as
   * the transport's own producer is: `producerConfig` and `maxInFlightRequests` cannot be given
   * with it. With Confluent, set `client: 'confluent'` too.
   */
  readonly producer?: KafkaInjectedProducer;
  /**
   * Which client `kafka` is. Default `'kafkajs'`; set `'confluent'` for a
   * `@confluentinc/kafka-javascript` `KafkaJS.Kafka`. The two KafkaJS-style APIs differ in where
   * settings go and how the consumer reports its group membership.
   */
  readonly client?: KafkaClientName;
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
  /**
   * Extra producer settings; `idempotent` and `maxInFlightRequests` always win. With Confluent,
   * KafkaJS-style settings go in a `kafkaJS` block and librdkafka properties at the top level;
   * `acks: -1` also wins, and so do `enable.idempotence`, `max.in.flight` and `acks` there.
   */
  readonly producerConfig?: object;
  /**
   * Extra consumer settings; `groupId` always wins. With Confluent, KafkaJS-style settings go in
   * a `kafkaJS` block and librdkafka properties at the top level; `fromBeginning` also wins, and
   * so do `group.id` and `auto.offset.reset` there. A `rebalance_cb` of yours is still called.
   */
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
   * The wire format, both ways: how events become records and consumed records
   * become events. Default {@link cloudEventsBinary}: CloudEvents binary mode,
   * strict on read, so any other record is malformed and goes to
   * `onInvalidMessage`. {@link cloudEventsStructured} writes structured mode
   * and reads both, {@link jsonEnvelope} maps a JSON format of your own (an
   * envelope with the payload inside, or the bare payload), and an object with
   * `encode` and `decode` can do anything else. Whatever a non-default codec's
   * `decode` throws marks the record as malformed, never crashes the consumer.
   */
  readonly codec?: KafkaCodec;
  /**
   * How consumed records become events; events are still written in
   * CloudEvents binary mode.
   *
   * @deprecated Use `codec`: `decode: fn` is `codec: { encode: encodeEnvelope, decode: fn }`.
   * Giving both `decode` and `codec` is a configuration error.
   */
  readonly decode?: KafkaMessageDecoder;
  /** Default `console`. */
  readonly logger?: RelayBusLogger;
}
