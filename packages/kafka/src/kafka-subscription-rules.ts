import { ConfigurationError } from '@relaybus/core';
import { TransportClosedError } from './errors.js';
import { deadLetterTopicFor, type KafkaSettings } from './kafka-settings.js';

/** Where a transport is: before its one subscription, subscribed, or closed for good. */
export type KafkaTransportState = 'idle' | 'subscribed' | 'closed';

/**
 * Refuses a subscription the transport cannot take: it is closed or subscribed already, it has
 * no group, no topic, or would consume its own dead-letter topic.
 */
export function assertCanSubscribe(
  state: KafkaTransportState,
  {
    settings,
    destinations,
  }: { readonly settings: KafkaSettings; readonly destinations: readonly string[] },
): void {
  const { name, groupId, invalidPolicy, kafka } = settings;
  if (state === 'closed') {
    throw new TransportClosedError(`Transport '${name}' is closed`);
  }
  if (state === 'subscribed') {
    throw new ConfigurationError(
      `Transport '${name}' already subscribed; list every topic in one subscription or use one KafkaTransport per consumer group`,
    );
  }
  if (kafka === undefined) {
    throw new ConfigurationError(`Transport '${name}' needs 'kafka' to create its consumer`);
  }
  if (groupId === undefined || groupId.length === 0) {
    throw new ConfigurationError(`Transport '${name}' needs a groupId to subscribe`);
  }
  if (destinations.length === 0) {
    throw new ConfigurationError(`Transport '${name}' subscribed to no topics`);
  }
  for (const topic of destinations) {
    const deadLetterTopic = deadLetterTopicFor(invalidPolicy, topic);
    if (deadLetterTopic !== undefined && destinations.includes(deadLetterTopic)) {
      throw new ConfigurationError(
        `Dead letter topic '${deadLetterTopic}' is also consumed, which would loop`,
      );
    }
  }
}
