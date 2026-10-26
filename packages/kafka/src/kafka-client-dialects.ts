import { ConfigurationError } from '@relaybus/core';
import type {
  KafkaConsumerConfig,
  KafkaConsumerEvent,
  KafkaConsumerLike,
  KafkaProducerConfig,
  KafkaProducerRecord,
  KafkaTopicPartition,
} from './kafka-client.js';
import type { KafkaSettings } from './kafka-settings.js';

/** The Kafka client a transport drives: `kafkajs`, or `@confluentinc/kafka-javascript`'s KafkaJS API. */
export type KafkaClientName = 'kafkajs' | 'confluent';

/** What the transport does when the consumer's group membership changes. */
export interface MembershipListeners {
  joined(): void;
  rebalancing(): void;
  crashed(event: KafkaConsumerEvent): void;
  left(): void;
}

/**
 * How the transport talks to one client: where its settings go, what `subscribe` and `send`
 * accept and how the client reports group membership.
 */
export interface KafkaClientDialect {
  producerConfig(settings: KafkaSettings): KafkaProducerConfig;
  consumerConfig(settings: KafkaSettings, membership: MembershipListeners): KafkaConsumerConfig;
  subscription(
    topics: string[],
    fromBeginning: boolean,
  ): { topics: string[]; fromBeginning?: boolean };
  record(record: KafkaProducerRecord): KafkaProducerRecord;
  /** Whether the consumer reports joining its group, so start-up can wait for it. */
  tracksMembership(consumer: KafkaConsumerLike): boolean;
  /** Starts listening to the consumer's membership events, if it reports them as events. */
  observe(consumer: KafkaConsumerLike, membership: MembershipListeners): void;
}

/** kafkajs: settings at the top level, membership through instrumentation events. */
const KAFKAJS_DIALECT: KafkaClientDialect = {
  producerConfig: ({ producerConfig, maxInFlightRequests }) => ({
    ...producerConfig,
    idempotent: true,
    maxInFlightRequests,
  }),
  consumerConfig: ({ consumerConfig, groupId }) => ({ ...consumerConfig, groupId: groupId! }),
  subscription: (topics, fromBeginning) => ({ topics, fromBeginning }),
  record: (record) => record,
  tracksMembership: (consumer) =>
    consumer.on !== undefined && consumer.events?.GROUP_JOIN !== undefined,
  observe: (consumer, membership) => {
    if (!KAFKAJS_DIALECT.tracksMembership(consumer)) {
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
    listen(names.GROUP_JOIN, () => membership.joined());
    listen(names.REBALANCING, () => membership.rebalancing());
    listen(names.CRASH, (event) => membership.crashed(event));
    listen(names.STOP, () => membership.left());
    listen(names.DISCONNECT, () => membership.left());
  },
};

/** librdkafka's `rebalance_cb` codes: partitions assigned (the group is joined) and revoked. */
const ASSIGN_PARTITIONS = -175;
const REVOKE_PARTITIONS = -174;

/** Arguments Confluent hands to `rebalance_cb`; the third carries the assignment functions. */
type RebalanceArguments = [{ readonly code: number }, readonly KafkaTopicPartition[], ...unknown[]];

/** The user's settings with their own `kafkaJS` block, if any. */
type WithKafkaJsBlock = { readonly kafkaJS?: object; readonly rebalance_cb?: unknown };

/**
 * Confluent's KafkaJS-compatible API: KafkaJS settings inside the `kafkaJS` block (it rejects
 * them at the top level), `fromBeginning` in the consumer config instead of `subscribe`, `acks`
 * on the producer instead of `send`, and membership through `rebalance_cb` (it has no
 * instrumentation events, and it never gives up on a consumer, so there is no crash to report).
 * The same settings are also given as librdkafka properties, which Confluent applies after the
 * `kafkaJS` block, so the user's librdkafka properties cannot override them either.
 */
const CONFLUENT_DIALECT: KafkaClientDialect = {
  producerConfig: ({ producerConfig, maxInFlightRequests }) => {
    const user = producerConfig as WithKafkaJsBlock;
    return {
      ...user,
      kafkaJS: { ...user.kafkaJS, idempotent: true, maxInFlightRequests, acks: -1 },
      'enable.idempotence': true,
      'max.in.flight': maxInFlightRequests,
      acks: -1,
    };
  },
  consumerConfig: ({ consumerConfig, groupId, fromBeginning }, membership) => {
    const user = consumerConfig as WithKafkaJsBlock;
    const userCallback =
      typeof user.rebalance_cb === 'function'
        ? (user.rebalance_cb as (...args: RebalanceArguments) => unknown)
        : undefined;
    return {
      ...user,
      kafkaJS: { ...user.kafkaJS, groupId: groupId!, fromBeginning },
      'group.id': groupId!,
      'auto.offset.reset': fromBeginning ? 'earliest' : 'latest',
      rebalance_cb: (...args: RebalanceArguments): unknown => {
        if (args[0].code === ASSIGN_PARTITIONS) {
          membership.joined();
        } else if (args[0].code === REVOKE_PARTITIONS) {
          membership.rebalancing();
        }
        return userCallback?.(...args);
      },
    };
  },
  subscription: (topics) => ({ topics }),
  record: ({ acks: _acks, ...record }) => record,
  tracksMembership: () => true,
  observe: () => undefined,
};

/** The dialect of `client`, kafkajs by default. */
export function clientDialect(client: KafkaClientName | undefined): KafkaClientDialect {
  if (client === undefined || client === 'kafkajs') {
    return KAFKAJS_DIALECT;
  }
  if (client === 'confluent') {
    return CONFLUENT_DIALECT;
  }
  throw new ConfigurationError(
    `Unknown Kafka client '${String(client)}'; use 'kafkajs' or 'confluent'`,
  );
}
