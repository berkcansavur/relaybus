import { WireTypeCollisionError } from './errors.js';

/** One wire name a route declares for an event type on a transport. */
export type DeclaredWireType = {
  readonly transport: string;
  readonly type: string;
  readonly wire: string;
};

/** Event type to wire name of each transport that has any, as the routes declared them. */
export type TransportWireTypes = ReadonlyMap<string, Readonly<Record<string, string>>>;

/**
 * Merges the wire names that routes declare per transport. A codec has one set of wire names, so
 * an event type needs the same wire name wherever the transport sends it, and no wire name may
 * stand for two event types. Throws {@link WireTypeCollisionError} otherwise.
 */
export function mergeRouteWireTypes(
  declared: readonly DeclaredWireType[],
  routed: readonly { readonly transport: string; readonly type: string }[],
): TransportWireTypes {
  const merged = new Map<string, Map<string, string>>();
  for (const { transport, type, wire } of declared) {
    const types = merged.get(transport) ?? new Map<string, string>();
    merged.set(transport, types);
    addWireType(types, { type, wire, transport });
  }
  for (const [transport, types] of merged) {
    assertDistinctOnTheWire(types, {
      transport,
      routedTypes: routed.filter((item) => item.transport === transport).map((item) => item.type),
    });
  }
  return new Map([...merged].map(([transport, types]) => [transport, Object.fromEntries(types)]));
}

/** Every routed type travels under its wire name or, without one, as itself; two must not share a name. */
function assertDistinctOnTheWire(
  types: ReadonlyMap<string, string>,
  {
    transport,
    routedTypes,
  }: { readonly transport: string; readonly routedTypes: readonly string[] },
): void {
  const owners = new Map<string, string>();
  for (const type of new Set([...types.keys(), ...routedTypes])) {
    const name = types.get(type) ?? type;
    const owner = owners.get(name);
    if (owner !== undefined) {
      throw new WireTypeCollisionError(
        `Event types '${owner}' and '${type}' both travel as '${name}' on transport '${transport}'`,
      );
    }
    owners.set(name, type);
  }
}

function addWireType(
  types: Map<string, string>,
  {
    type,
    wire,
    transport,
  }: { readonly type: string; readonly wire: string; readonly transport: string },
): void {
  const current = types.get(type);
  if (current !== undefined && current !== wire) {
    throw new WireTypeCollisionError(
      `Event type '${type}' has the wire names '${current}' and '${wire}' on transport '${transport}'; a transport writes one`,
    );
  }
  const owner = [...types].find(
    ([otherType, otherWire]) => otherWire === wire && otherType !== type,
  );
  if (owner !== undefined) {
    throw new WireTypeCollisionError(
      `Event types '${owner[0]}' and '${type}' share the wire name '${wire}' on transport '${transport}'`,
    );
  }
  types.set(type, wire);
}
