import type { BusObservation, ReceiveOrigin } from '../bus-observation.js';
import { ConfigurationError, toErrorInfo } from '../errors.js';
import type { EventEnvelope } from '../event.js';
import type { HandlerRegistry } from '../handler-registry.js';
import type { Notifier } from '../notifier.js';
import type { InboundSubscriptionOptions, RouteOptions } from '../relay-bus-options.js';
import { DEFAULTS } from '../relay-bus-settings.js';
import { DEFAULT_ROUTE_RETRY_POLICY } from '../retry-policy.js';
import { groupRoutes, type RouteGroup } from '../route-groups.js';
import type { Transport, TransportSubscriptionHooks } from '../transport.js';

/** What the connections of a bus need. */
export type BusConnectionsParts = {
  readonly registry: HandlerRegistry;
  readonly notifier: Notifier;
  readonly observation: BusObservation | undefined;
};

/** The transports, routes and subscriptions a bus was configured with. */
export type BusConnectionsOptions = {
  readonly transports: readonly Transport[];
  readonly routes: readonly RouteOptions[];
  readonly subscriptions: readonly InboundSubscriptionOptions[];
};

/**
 * The transports of a bus by name, its routes (registered as forwarding handlers) and its inbound
 * subscriptions: checked when the bus is built, connected when it starts.
 */
export class BusConnections {
  readonly #parts: BusConnectionsParts;
  readonly #transports = new Map<string, Transport>();
  readonly #subscriptions: readonly InboundSubscriptionOptions[];

  constructor(parts: BusConnectionsParts, subscriptions: readonly InboundSubscriptionOptions[]) {
    this.#parts = parts;
    this.#subscriptions = subscriptions;
  }

  /** Every transport, in the order it was configured. */
  get transports(): Iterable<Transport> {
    return this.#transports.values();
  }

  /** Registers the transports, then the routes, and checks the subscriptions. */
  register({ transports, routes }: Omit<BusConnectionsOptions, 'subscriptions'>): void {
    for (const transport of transports) {
      if (this.#transports.has(transport.name)) {
        throw new ConfigurationError(`Transport name '${transport.name}' is used twice`);
      }
      this.#transports.set(transport.name, transport);
    }
    this.#registerRoutes(routes);
    for (const subscription of this.#subscriptions) {
      const transport = this.#requireTransport(subscription.transport);
      if (transport.subscribe === undefined) {
        throw new ConfigurationError(`Transport '${transport.name}' cannot subscribe`);
      }
    }
  }

  /** True while every transport reports itself ready. */
  allReady(): boolean {
    for (const transport of this.#transports.values()) {
      if (transport.isReady?.() === false) {
        return false;
      }
    }
    return true;
  }

  /** The inbound subscriptions, each with a transport that can subscribe. */
  get subscriptions(): readonly InboundSubscriptionOptions[] {
    return this.#subscriptions;
  }

  /** Subscribes one subscription, handing its messages to `receive`; the transport's own promise. */
  subscribe(
    subscription: InboundSubscriptionOptions,
    receive: (envelope: EventEnvelope, origin: ReceiveOrigin) => Promise<unknown>,
  ): Promise<void> {
    const transport = this.#transports.get(subscription.transport)!;
    return transport.subscribe!(
      subscription.destinations,
      async (envelope, message) => {
        await receive(envelope, { transport, destination: message?.destination });
      },
      this.#subscriptionHooks(transport),
    );
  }

  /** What a transport may report back while subscribed; undefined when no observer wants it. */
  #subscriptionHooks(transport: Transport): TransportSubscriptionHooks | undefined {
    const { notifier } = this.#parts;
    if (!notifier.wants('onInvalidMessage')) {
      return undefined;
    }
    return {
      onInvalidMessage: ({ destination, error }) => {
        const errorInfo = toErrorInfo(error);
        notifier.notify('onInvalidMessage', {
          transport: transport.name,
          system: transport.system ?? transport.name,
          destination,
          reason: errorInfo.message,
          error: errorInfo,
        });
      },
    };
  }

  #registerRoutes(routes: readonly RouteOptions[]): void {
    for (const route of routes) {
      this.#requireTransport(route.transport);
    }
    for (const group of groupRoutes(routes)) {
      this.#registerRoute(group);
    }
  }

  /** Registers one handler for every route to a destination, so a key's events reach it in commit order. */
  #registerRoute(group: RouteGroup): void {
    const transport = this.#transports.get(group.transport)!;
    const { route, destination } = group;
    this.#parts.registry.register(
      {
        eventTypes: group.eventTypes,
        legacyIds: group.legacyIds,
        schema: undefined,
        handle:
          this.#parts.observation?.sender(transport, destination) ??
          ((envelope) => transport.publish(envelope, destination)),
        options: {
          id: group.id,
          ...(route.retry === undefined ? {} : { retry: route.retry }),
          ...(route.timeout === undefined ? {} : { timeout: route.timeout }),
        },
        forwardsToTransport: true,
      },
      { retry: DEFAULT_ROUTE_RETRY_POLICY, timeoutMs: DEFAULTS.routeTimeoutMs },
    );
  }

  #requireTransport(name: string): Transport {
    const transport = this.#transports.get(name);
    if (transport === undefined) {
      throw new ConfigurationError(`No transport named '${name}' is configured`);
    }
    return transport;
  }
}
