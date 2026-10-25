import type { ErrorInfo } from '@relaybus/core';

/** Instrumentation scope of every meter and tracer of this package. */
export const INSTRUMENTATION_SCOPE = '@relaybus/otel';

/** `messaging.system` of operations on the outbox itself (publish, claim, process). */
export const RELAYBUS_SYSTEM = 'relaybus';

/** OpenTelemetry semantic convention attributes for messaging, and relaybus's own. */
export const ATTRIBUTES = {
  system: 'messaging.system',
  destination: 'messaging.destination.name',
  operationName: 'messaging.operation.name',
  operationType: 'messaging.operation.type',
  consumerGroup: 'messaging.consumer.group.name',
  messageId: 'messaging.message.id',
  conversationId: 'messaging.message.conversation_id',
  batchCount: 'messaging.batch.message_count',
  errorType: 'error.type',
  eventType: 'relaybus.event.type',
  deliveryKind: 'relaybus.delivery.kind',
  deliveryAttempt: 'relaybus.delivery.attempt',
  receiveOutcome: 'relaybus.receive.outcome',
  failureStage: 'relaybus.failure.stage',
} as const;

/** Metric names: the messaging semantic conventions where they exist, else `relaybus.*`. */
export const METRICS = {
  operationDuration: 'messaging.client.operation.duration',
  sentMessages: 'messaging.client.sent.messages',
  consumedMessages: 'messaging.client.consumed.messages',
  processDuration: 'messaging.process.duration',
  published: 'relaybus.events.published',
  claimedDeliveries: 'relaybus.claim.deliveries',
  claimedLanes: 'relaybus.claim.lanes',
  pending: 'relaybus.deliveries.pending',
  oldestAge: 'relaybus.deliveries.oldest_age',
  deadLettered: 'relaybus.deliveries.dead_lettered',
  retried: 'relaybus.deliveries.retried',
  leaseLost: 'relaybus.lease.lost',
  endToEndLatency: 'relaybus.end_to_end.latency',
  failures: 'relaybus.failures',
} as const;

/** `error.type`: the relaybus error code when there is one, else the error's class name. */
export function errorType(error: ErrorInfo): string {
  return error.code ?? error.name;
}
