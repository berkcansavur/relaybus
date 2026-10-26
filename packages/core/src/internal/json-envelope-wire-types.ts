import { ConfigurationError } from '../errors.js';
import { assertEventType, type EventEnvelope } from '../event.js';
import { isJsonObject } from './cloudevents.js';
import type { Context } from './json-envelope-plan.js';

// The type names a format uses on the wire instead of relaybus event types (`wireTypes`).

/** Event type to wire name, and wire name back to event type. */
export type WireTypePlan = {
  readonly wireTypes: ReadonlyMap<string, string>;
  readonly eventTypes: ReadonlyMap<string, string>;
};

/**
 * Compiles `wireTypes`: every key a valid event type, every value a non-empty name no other type
 * uses, and none with a fixed type, which is written nowhere.
 */
export function compileWireTypes(
  wireTypes: unknown,
  { fixed }: { readonly fixed: boolean },
): WireTypePlan {
  if (wireTypes === undefined) {
    return { wireTypes: new Map(), eventTypes: new Map() };
  }
  if (!isJsonObject(wireTypes) || fixed) {
    throw new ConfigurationError(
      'jsonEnvelope wireTypes must be an object of event type to wire name, with a type at { body } or { header }',
    );
  }
  const entries = Object.entries(wireTypes);
  const eventTypes = new Map<string, string>();
  for (const [type, wire] of entries) {
    assertEventType(type);
    if (typeof wire !== 'string' || wire.length === 0 || eventTypes.has(wire)) {
      throw new ConfigurationError(
        `jsonEnvelope wireTypes needs a non-empty wire name for '${type}' that no other type uses`,
      );
    }
    eventTypes.set(wire, type);
  }
  return { wireTypes: new Map(entries as [string, string][]), eventTypes };
}

/** The type as the wire carries it; an unlisted type that another type's wire name equals is refused. */
export function wireTypeOf(envelope: EventEnvelope, { plan, failures }: Context): string {
  const wire = plan.wireTypes.get(envelope.type);
  if (wire !== undefined) {
    return wire;
  }
  const other = plan.eventTypes.get(envelope.type);
  if (other !== undefined) {
    throw failures.unencodable(
      `Event ${envelope.id} has type '${envelope.type}', which wireTypes uses as the wire name of '${other}'`,
    );
  }
  return envelope.type;
}

/** The event type a wire type value names: a listed wire name, or a valid event type itself. */
export function eventTypeOf(wire: string, { plan, failures }: Context): string {
  const type = plan.eventTypes.get(wire);
  if (type !== undefined) {
    return type;
  }
  try {
    assertEventType(wire);
  } catch (error) {
    throw failures.invalid(`The event type is not a valid event type: ${(error as Error).message}`);
  }
  return wire;
}
