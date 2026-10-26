import { ConfigurationError, UnknownCatalogEventError, WireTypeCollisionError } from './errors.js';

/** Wire names by catalog event name, for the events that have one. */
export type WireNames<TName extends string = string> = Readonly<Partial<Record<TName, string>>>;

/**
 * Checks wire names given by event name: every name is one of `events`, every wire name is a
 * non-empty string and no two events share one. Returns them as a map of event name to wire name.
 * `label` names the catalog or route in messages.
 */
export function checkWireNames(
  wireNames: WireNames | undefined,
  { events, label }: { readonly events: readonly string[]; readonly label: string },
): ReadonlyMap<string, string> {
  const checked = new Map<string, string>();
  const owners = new Map<string, string>();
  for (const [event, wire] of Object.entries(wireNames ?? {})) {
    if (!events.includes(event)) {
      throw new UnknownCatalogEventError(
        `${label} has no event named '${event}' to give a wire name; its events are ${events.join(', ')}`,
      );
    }
    if (typeof wire !== 'string' || wire.length === 0) {
      throw new ConfigurationError(`${label} needs a non-empty wire name for '${event}'`);
    }
    const owner = owners.get(wire);
    if (owner !== undefined) {
      throw new WireTypeCollisionError(
        `${label} gives events '${owner}' and '${event}' the same wire name '${wire}'`,
      );
    }
    owners.set(wire, event);
    checked.set(event, wire);
  }
  return checked;
}
