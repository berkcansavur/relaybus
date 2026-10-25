import { assertEventType, type EventClass, type EventReference } from '../event.js';
import type { HandlerDescriptor, HandlerRegistration } from '../handler-registry.js';
import type { StandardSchemaV1 } from '../standard-schema.js';

/** A registration as `bus.handlers()` lists it, once per event type. */
export function toDescriptor(
  registration: HandlerRegistration,
  eventType: string,
): HandlerDescriptor {
  return {
    id: registration.id,
    eventType,
    retry: registration.retry,
    timeoutMs: registration.timeoutMs,
    concurrency: registration.concurrency,
    transactional: registration.transactional,
    forwardsToTransport: registration.forwardsToTransport,
  };
}

/** The type and schema of an event class, or of an event type given by name. */
export function describeEvent(event: EventReference): {
  type: string;
  schema: StandardSchemaV1 | undefined;
} {
  if (typeof event === 'string') {
    assertEventType(event);
    return { type: event, schema: undefined };
  }
  assertEventType(event.type);
  return { type: event.type, schema: (event as Partial<EventClass>).schema };
}
