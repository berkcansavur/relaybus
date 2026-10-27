import { WireTypesUnsupportedError } from '../errors.js';
import type { Transport } from '../transport.js';

type Claims = { buses: number; declared: boolean };

const claims = new WeakMap<Transport, Claims>();

/**
 * Notes that one more bus uses `transports`, and throws {@link WireTypesUnsupportedError} when a
 * transport would then have two buses and one of them declared wire names: the names live in the
 * transport's codec, so they would change what the other bus writes. Nothing is recorded until the
 * returned function is called.
 */
export function claimTransports(
  transports: readonly Transport[],
  declaring: ReadonlySet<string>,
): () => void {
  const next = transports.map((transport) => {
    const current = claims.get(transport) ?? { buses: 0, declared: false };
    const claim = {
      buses: current.buses + 1,
      declared: current.declared || declaring.has(transport.name),
    };
    if (claim.buses > 1 && claim.declared) {
      throw new WireTypesUnsupportedError(
        `Transport '${transport.name}' is used by more than one bus, and wire names are declared for it; they would change what the other bus writes. Give each bus its own transport`,
      );
    }
    return [transport, claim] as const;
  });
  return () => {
    for (const [transport, claim] of next) {
      claims.set(transport, claim);
    }
  };
}
