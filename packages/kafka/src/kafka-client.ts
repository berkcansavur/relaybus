/**
 * Structural views of the Kafka client API used by this package.
 *
 * They are satisfied by both `kafkajs` (`new Kafka(...)`) and
 * `@confluentinc/kafka-javascript` (`new KafkaJS.Kafka(...)`, the KafkaJS
 * compatible API), so neither is a hard dependency. Only the members listed
 * here are ever called. Methods are declared with method syntax on purpose:
 * it keeps the real client classes assignable despite their richer signatures.
 */

/** Bytes as delivered by a consumer: `Buffer` in both supported clients. */
export type KafkaBytes = Uint8Array;

/** Header values a consumer may hand over: bytes, a string, a list of either, or nothing. */
export type KafkaIncomingHeaders = Readonly<
  Record<string, KafkaBytes | string | readonly (KafkaBytes | string)[] | undefined>
>;

/** Header values accepted when producing. */
export type KafkaOutgoingHeaders = Readonly<Record<string, Buffer | string | (Buffer | string)[]>>;

/** One record handed to the producer. */
export interface KafkaOutgoingMessage {
  readonly key: string | Buffer | null;
  readonly value: string | Buffer | null;
  readonly headers: KafkaOutgoingHeaders;
}

/** Argument of `producer.send`. */
export interface KafkaProducerRecord {
  readonly topic: string;
  readonly messages: KafkaOutgoingMessage[];
  /** Always `-1`: wait for all in-sync replicas. */
  readonly acks: number;
}

/** Producer settings this package decides itself; user settings are merged below them. */
export interface KafkaProducerConfig {
  readonly idempotent: boolean;
  readonly maxInFlightRequests: number;
}

/** Consumer settings this package decides itself; user settings are merged below them. */
export interface KafkaConsumerConfig {
  readonly groupId: string;
}

/** Subset of the producer API: `connect`, `send`, `disconnect`. */
export interface KafkaProducerLike {
  connect(): Promise<void>;
  send(record: KafkaProducerRecord): Promise<unknown>;
  disconnect(): Promise<void>;
}

/** A message as delivered to `eachMessage`. */
export interface KafkaIncomingMessage {
  readonly key: KafkaBytes | null;
  readonly value: KafkaBytes | null;
  readonly headers?: KafkaIncomingHeaders | undefined;
  readonly offset: string;
  /** Record timestamp as epoch milliseconds in a string (kafkajs and Confluent). */
  readonly timestamp?: string | undefined;
}

/** Argument of the `eachMessage` callback. `pause` and others are not used. */
export interface KafkaEachMessagePayload {
  readonly topic: string;
  readonly partition: number;
  readonly message: KafkaIncomingMessage;
  heartbeat(): Promise<void>;
}

/** Argument of `consumer.run`. Offsets are committed automatically after `eachMessage` resolves. */
export interface KafkaRunConfig {
  eachMessage(payload: KafkaEachMessagePayload): Promise<void>;
}

/**
 * Instrumentation event names exposed as `consumer.events`. Optional: a client
 * without them still works, but start-up cannot wait for the group join and
 * `isConsuming()` cannot observe crashes.
 */
export interface KafkaConsumerEventNames {
  readonly GROUP_JOIN?: string;
  readonly CRASH?: string;
  readonly REBALANCING?: string;
  readonly STOP?: string;
  readonly DISCONNECT?: string;
}

/** Instrumentation event; `payload` carries `error` and `restart` for a crash. */
export interface KafkaConsumerEvent {
  readonly payload?: unknown;
}

/** Subset of the consumer API: `connect`, `subscribe`, `run`, `disconnect`, optional `on`/`events`. */
export interface KafkaConsumerLike {
  readonly events?: KafkaConsumerEventNames | undefined;
  on?(eventName: string, listener: (event: KafkaConsumerEvent) => void): unknown;
  connect(): Promise<void>;
  subscribe(subscription: { topics: string[]; fromBeginning: boolean }): Promise<void>;
  run(config: KafkaRunConfig): Promise<void>;
  disconnect(): Promise<void>;
}

/** Subset of the client API: the `producer` and `consumer` factories. */
export interface KafkaClientLike {
  producer(config: KafkaProducerConfig): KafkaProducerLike;
  consumer(config: KafkaConsumerConfig): KafkaConsumerLike;
}
