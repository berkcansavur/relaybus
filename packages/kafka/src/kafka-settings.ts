import { ConfigurationError, toErrorInfo, type RelayBusLogger } from '@relaybus/core';
import {
  BoundedInteger,
  ExponentialBackoff,
  Milliseconds,
  PositiveInteger,
} from '@relaybus/core/internal';
import { encodeEnvelope } from './cloudevents-codec.js';
import { cloudEventsBinary, type KafkaCodec } from './codecs.js';
import { HandlerFailedError } from './errors.js';
import type { KafkaClientLike } from './kafka-client.js';
import type {
  InvalidMessagePolicy,
  KafkaProcessingOptions,
  KafkaRestartOptions,
  KafkaTransportOptions,
} from './kafka-options.js';

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

/** Largest `maxInFlightRequests`. */
const MAXIMUM_IN_FLIGHT_REQUESTS = 5;

/** The timing of record processing and of the consumer restart loop. */
export interface KafkaTimings {
  readonly maxAttempts: number;
  readonly processingBackoff: ExponentialBackoff;
  readonly startupTimeoutMs: number;
  readonly publishTimeoutMs: number;
  readonly heartbeatIntervalMs: number;
  readonly restartBackoff: ExponentialBackoff;
}

/** Every option of a {@link KafkaTransport}, with its default, checked. */
export interface KafkaSettings {
  readonly name: string;
  readonly kafka: KafkaClientLike;
  readonly groupId: string | undefined;
  readonly fromBeginning: boolean;
  readonly maxInFlightRequests: number;
  readonly producerConfig: object;
  readonly consumerConfig: object;
  readonly invalidPolicy: InvalidMessagePolicy;
  readonly codec: KafkaCodec;
  readonly logger: RelayBusLogger;
  readonly timings: KafkaTimings;
}

/** Resolves the options of a transport, in the order they were always checked. */
export function resolveKafkaSettings(options: KafkaTransportOptions): KafkaSettings {
  return {
    name: options.name ?? DEFAULTS.name,
    kafka: options.kafka,
    groupId: options.groupId,
    fromBeginning: options.fromBeginning ?? true,
    maxInFlightRequests: BoundedInteger.of(
      options.maxInFlightRequests ?? DEFAULTS.maxInFlightRequests,
      { name: 'maxInFlightRequests', minimum: 1, maximum: MAXIMUM_IN_FLIGHT_REQUESTS },
    ),
    producerConfig: options.producerConfig ?? {},
    consumerConfig: options.consumerConfig ?? {},
    invalidPolicy: options.onInvalidMessage ?? 'dead-letter',
    codec: resolveCodec(options),
    logger: options.logger ?? console,
    timings: resolveTimings(options),
  };
}

/** The codec of the options; the deprecated `decode` pairs with the default encoding. */
function resolveCodec({ codec, decode }: KafkaTransportOptions): KafkaCodec {
  if (decode === undefined) {
    return codec ?? cloudEventsBinary;
  }
  if (codec !== undefined) {
    throw new ConfigurationError("Give 'codec' or the deprecated 'decode', not both");
  }
  return { encode: encodeEnvelope, decode };
}

function resolveTimings(options: KafkaTransportOptions): KafkaTimings {
  const processing: KafkaProcessingOptions = options.processing ?? {};
  const maxAttempts = PositiveInteger.of(
    processing.maxAttempts ?? DEFAULTS.maxAttempts,
    'processing.maxAttempts',
  );
  return {
    maxAttempts,
    processingBackoff: new ExponentialBackoff({
      initialMs: Milliseconds.of(
        processing.initialBackoff ?? DEFAULTS.initialBackoff,
        'processing.initialBackoff',
      ),
      maxMs: Milliseconds.of(processing.maxBackoff ?? DEFAULTS.maxBackoff, 'processing.maxBackoff'),
    }),
    startupTimeoutMs: Milliseconds.of(
      options.startupTimeout ?? DEFAULTS.startupTimeout,
      'startupTimeout',
    ),
    publishTimeoutMs: Milliseconds.of(
      options.publishTimeout ?? DEFAULTS.publishTimeout,
      'publishTimeout',
    ),
    heartbeatIntervalMs: Math.max(
      1,
      Milliseconds.of(
        processing.heartbeatInterval ?? DEFAULTS.heartbeatInterval,
        'processing.heartbeatInterval',
      ),
    ),
    restartBackoff: resolveRestartBackoff(options.restart ?? {}),
  };
}

function resolveRestartBackoff(restart: KafkaRestartOptions): ExponentialBackoff {
  return new ExponentialBackoff({
    initialMs: Milliseconds.of(
      restart.initialBackoff ?? DEFAULTS.restartInitialBackoff,
      'restart.initialBackoff',
    ),
    maxMs: Milliseconds.of(restart.maxBackoff ?? DEFAULTS.restartMaxBackoff, 'restart.maxBackoff'),
  });
}

/** The dead-letter topic for records of `sourceTopic`, or undefined when the policy does not use one. */
export function deadLetterTopicFor(
  policy: InvalidMessagePolicy,
  sourceTopic: string,
): string | undefined {
  if (policy === 'dead-letter') {
    return `${sourceTopic}.dead`;
  }
  if (typeof policy === 'object') {
    const { deadLetterTopic } = policy;
    return typeof deadLetterTopic === 'function' ? deadLetterTopic(sourceTopic) : deadLetterTopic;
  }
  return undefined;
}

/**
 * Wraps a failure in a fresh error before it goes back to the Kafka client.
 * kafkajs classifies errors by type and cause chain: a handler that throws a
 * `TypeError` would be marked unrecoverable and kill the consumer for good.
 * The wrapper carries the original name and message but no `cause`.
 */
export function toHandlerFailure(error: unknown): Error {
  const { name, message } = toErrorInfo(error);
  return new HandlerFailedError(`${name}: ${message}`);
}
