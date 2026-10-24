import { ConfigurationError, type RetryPolicy } from '@relaybus/core';
import type { AmqpMessage, AmqpPublishOptions } from './amqp-types.js';

/** Number of delayed redeliveries a message went through. Set on every retry copy. */
export const REDELIVERIES_HEADER = 'x-relaybus-redeliveries';
/** Epoch milliseconds of the first failure that needed a delayed redelivery. */
export const FIRST_FAILURE_HEADER = 'x-relaybus-first-failure-at';
/** Why a message was moved to the dead-letter queue by the consumer. */
export const DEAD_LETTER_REASON_HEADER = 'x-relaybus-dead-letter-reason';
/**
 * The routing key a message was originally published with. Set on retry copies,
 * which travel through the default exchange and return with the queue name as
 * routing key, so decoders still see the original one.
 */
export const ORIGINAL_ROUTING_KEY_HEADER = 'x-relaybus-routing-key';

const RELAYBUS_STATE_HEADERS: readonly string[] = [REDELIVERIES_HEADER, FIRST_FAILURE_HEADER];
/** Counters the broker stamps on its own redeliveries; they must not travel in a copy. */
const BROKER_COUNTER_HEADERS: readonly string[] = ['x-delivery-count', 'x-acquired-count'];
const MAXIMUM_LADDER_TIERS = 16;

/**
 * The fixed delays of the retry queues, ascending: the policy's delay grown by
 * its factor until the cap. Each tier is its own queue with a queue-level TTL,
 * so messages in one queue always expire in arrival order; one queue with
 * mixed per-message TTLs would let a long delay hold back shorter ones.
 *
 * @throws {ConfigurationError} the policy has no usable delays or too many tiers
 */
export function buildRetryLadder(policy: RetryPolicy): readonly number[] {
  if (policy.initialDelayMs < 1) {
    throw new ConfigurationError('redelivery.initialDelay must be at least 1ms');
  }
  const tiers: number[] = [];
  let delay = policy.initialDelayMs;
  for (;;) {
    const tier = Math.round(Math.min(delay, policy.maxDelayMs));
    tiers.push(tier);
    if (tier >= policy.maxDelayMs || policy.factor === 1) {
      return tiers;
    }
    if (tiers.length === MAXIMUM_LADDER_TIERS) {
      throw new ConfigurationError(
        `redelivery needs more than ${MAXIMUM_LADDER_TIERS} retry queues; raise redelivery.factor or lower redelivery.maxDelay`,
      );
    }
    delay *= policy.factor;
  }
}

/**
 * Maps a jittered delay onto the ladder. A delay between two tiers goes to the
 * upper one with the probability that keeps the expected delay equal to
 * `delayMs`, so jitter still spreads a cohort of failures across tiers.
 */
export function chooseRetryTier(
  ladder: readonly number[],
  delayMs: number,
  random: () => number,
): number {
  const upperIndex = ladder.findIndex((tier) => tier >= delayMs);
  if (upperIndex === -1) {
    return ladder.at(-1)!;
  }
  if (upperIndex === 0) {
    return ladder[0]!;
  }
  const lower = ladder[upperIndex - 1]!;
  const upper = ladder[upperIndex]!;
  return random() < (delayMs - lower) / (upper - lower) ? upper : lower;
}

/** Reads a non-negative integer that a copy carries as a header (string or number). */
export function readCounterHeader(message: AmqpMessage, name: string): number | undefined {
  // A message decoded by a custom decoder (plain JSON, structured mode) may have no headers.
  const value = message.properties.headers?.[name];
  const parsed = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
  return typeof parsed === 'number' && Number.isSafeInteger(parsed) && parsed >= 0
    ? parsed
    : undefined;
}

/**
 * A byte-faithful copy of a received message for publishing to another queue:
 * same body and properties, headers carried over except the broker's delivery
 * counters and relaybus' own redelivery state, then `headerChanges` applied.
 */
export function copyForRepublish(
  message: AmqpMessage,
  headerChanges: Readonly<Record<string, string>>,
): { content: Buffer; options: AmqpPublishOptions } {
  const { properties } = message;
  const headers: Record<string, unknown> = { ...properties.headers };
  for (const name of [
    ...RELAYBUS_STATE_HEADERS,
    DEAD_LETTER_REASON_HEADER,
    ...BROKER_COUNTER_HEADERS,
  ]) {
    delete headers[name];
  }
  Object.assign(headers, headerChanges);
  return {
    content: message.content,
    options: {
      contentType:
        typeof properties.contentType === 'string' ? properties.contentType : 'application/json',
      persistent: true,
      ...(typeof properties.messageId === 'string' ? { messageId: properties.messageId } : {}),
      ...(typeof properties.type === 'string' ? { type: properties.type } : {}),
      ...(typeof properties.appId === 'string' ? { appId: properties.appId } : {}),
      ...(typeof properties.correlationId === 'string'
        ? { correlationId: properties.correlationId }
        : {}),
      ...(typeof properties.timestamp === 'number' ? { timestamp: properties.timestamp } : {}),
      headers,
    },
  };
}

/** The routing key the message was published with: the original one for a retry copy. */
export function originalRoutingKey(message: AmqpMessage): string {
  const original = message.properties.headers?.[ORIGINAL_ROUTING_KEY_HEADER];
  return typeof original === 'string' ? original : message.fields.routingKey;
}

/** The message as first published: a retry copy gets its original routing key back. */
export function withOriginalRoutingKey(message: AmqpMessage): AmqpMessage {
  const routingKey = originalRoutingKey(message);
  return routingKey === message.fields.routingKey
    ? message
    : { ...message, fields: { ...message.fields, routingKey } };
}
