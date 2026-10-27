import {
  ConfigurationError,
  WireTypeCollisionError,
  WireTypesUnsupportedError,
} from '../errors.js';
import { assertEventType, type EventEnvelope } from '../event.js';
import { isJsonObject } from './cloudevents.js';
import type { WireTypes } from './json-envelope.js';
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

/** A wire format that can take more wire names after it was created; see `declareWireTypes`. */
export type WireTypeAware<TCodec> = {
  readonly withWireTypes?: (wireTypes: Readonly<Record<string, string>>) => TCodec;
};

/**
 * The codec with `wireTypes` added to its own, for a transport's `declareWireTypes`. Throws
 * {@link WireTypesUnsupportedError} for a codec that cannot map type names.
 */
export function withDeclaredWireTypes<TCodec extends WireTypeAware<TCodec>>(
  codec: TCodec,
  {
    wireTypes,
    transport,
  }: { readonly wireTypes: Readonly<Record<string, string>>; readonly transport: string },
): TCodec {
  if (codec.withWireTypes === undefined) {
    throw new WireTypesUnsupportedError(
      `The codec of transport '${transport}' cannot map event types to wire names; use jsonEnvelope with a type at { body } or { header }`,
    );
  }
  return codec.withWireTypes(wireTypes);
}

/**
 * The `wireTypes` of a mapping together with `declared` ones, for the `withWireTypes` of a
 * `jsonEnvelope` codec. A type may be declared again with the name it has; any other clash, or a
 * mapping with a fixed type, throws.
 */
export function mergeWireTypes(
  mapping: { readonly type: unknown; readonly wireTypes?: WireTypes | undefined },
  declared: Readonly<Record<string, string>>,
): WireTypes {
  if (isJsonObject(mapping.type) && 'fixed' in mapping.type) {
    throw new WireTypesUnsupportedError(
      'A jsonEnvelope with a fixed type writes no type, so it has no wire names to declare',
    );
  }
  const merged = new Map(Object.entries(mapping.wireTypes ?? {}));
  for (const [type, wire] of Object.entries(declared)) {
    assertDeclarable(merged, { type, wire });
    merged.set(type, wire);
  }
  return Object.fromEntries(merged);
}

function assertDeclarable(
  wireTypes: ReadonlyMap<string, string>,
  { type, wire }: { readonly type: string; readonly wire: string },
): void {
  const current = wireTypes.get(type);
  if (current !== undefined && current !== wire) {
    throw new WireTypeCollisionError(
      `Event type '${type}' has the wire name '${current}' in the wireTypes of the codec and '${wire}' from a route or catalog`,
    );
  }
  const owner = [...wireTypes].find(([other, name]) => name === wire && other !== type);
  if (owner !== undefined) {
    throw new WireTypeCollisionError(
      `Event types '${owner[0]}' and '${type}' share the wire name '${wire}' in the codec and a route or catalog`,
    );
  }
}
