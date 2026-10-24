import { ConfigurationError, type RelayBus } from '@relaybus/core';

/**
 * Buses of the applications that are running in this process, so `@RelayBusTransaction()` finds
 * its bus without the class injecting it. A module registers its bus when it initialises and
 * removes it after the application shut down.
 */
const runningBuses = new Set<RelayBus>();

/** Provider instances of a running application with the bus of that application. */
const busOfInstance = new WeakMap<object, RelayBus>();

export function registerBus(bus: RelayBus): void {
  runningBuses.add(bus);
}

export function unregisterBus(bus: RelayBus): void {
  runningBuses.delete(bus);
}

/** Ties a provider instance to the bus of its application, which wins when several applications run. */
export function bindInstance(instance: object, bus: RelayBus): void {
  busOfInstance.set(instance, bus);
}

/**
 * The bus a `@RelayBusTransaction()` method of `instance` uses: the bus of the instance's
 * application, else the only running bus.
 *
 * @throws ConfigurationError when no RelayBusModule runs, its application was closed, or several
 * applications run and `instance` is not a provider of any of them.
 */
export function resolveBus(instance: unknown, method: string): RelayBus {
  const bound =
    typeof instance === 'object' && instance !== null ? busOfInstance.get(instance) : undefined;
  if (bound !== undefined) {
    if (!runningBuses.has(bound)) {
      throw new ConfigurationError(
        `@RelayBusTransaction() ${method} was called after its application shut down`,
      );
    }
    return bound;
  }
  const [only, ...others] = runningBuses;
  if (only === undefined) {
    throw new ConfigurationError(
      `@RelayBusTransaction() ${method} found no running RelayBusModule: import RelayBusModule.forRoot(...) ` +
        `and call the method after the application initialised (app.init() or app.listen())`,
    );
  }
  if (others.length > 0) {
    throw new ConfigurationError(
      `@RelayBusTransaction() ${method} cannot tell which of ${runningBuses.size} running applications ` +
        `it belongs to; call it on a singleton provider of one of them`,
    );
  }
  return only;
}
