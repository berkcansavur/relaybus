import { toTimerMilliseconds } from './duration.js';
import { ConfigurationError } from './errors.js';
import { assertEventType, type EventReference } from './event.js';
import { DEFAULTS } from './relay-bus-settings.js';
import { DEFAULT_ROUTE_RETRY_POLICY, createRetryPolicy, type RetryPolicy } from './retry-policy.js';
import type { RouteOptions } from './relay-bus.js';

const TRANSPORT_HANDLER_PREFIX = 'relaybus.transport';

/**
 * Every route to one destination of one transport. They share one handler id, so their deliveries
 * form one queue per ordering key: events of a key reach the destination in commit order, whatever
 * their types.
 */
export interface RouteGroup {
  readonly transport: string;
  readonly destination: string;
  /** `relaybus.transport:<transport>:<destination>`. */
  readonly id: string;
  /** The routed event types, in route order. */
  readonly eventTypes: string[];
  /** The 0.2.0 per-type ids (`<id>:<eventType>`), whose stored deliveries the group still sends. */
  readonly legacyIds: string[];
  /** Retry and timeout of the first route; every other route must resolve to the same. */
  readonly route: RouteOptions;
  readonly retry: RetryPolicy;
  readonly timeoutMs: number;
}

/** Groups `routes` by transport and destination, in order of first appearance. */
export function groupRoutes(routes: readonly RouteOptions[]): RouteGroup[] {
  const groups = new Map<string, RouteGroup>();
  for (const route of routes) {
    const type = routedType(route.event);
    const destination = route.destination ?? type;
    const id = `${TRANSPORT_HANDLER_PREFIX}:${route.transport}:${destination}`;
    const group = groups.get(id);
    if (group === undefined) {
      groups.set(id, createGroup(route, { id, destination, type }));
    } else {
      addRoute(group, { route, type, destination });
    }
  }
  return [...groups.values()];
}

function createGroup(
  route: RouteOptions,
  {
    id,
    destination,
    type,
  }: { readonly id: string; readonly destination: string; readonly type: string },
): RouteGroup {
  return {
    transport: route.transport,
    destination,
    id,
    eventTypes: [type],
    legacyIds: [`${id}:${type}`],
    route,
    ...resolveSettings(route, id),
  };
}

function addRoute(
  group: RouteGroup,
  {
    route,
    type,
    destination,
  }: { readonly route: RouteOptions; readonly type: string; readonly destination: string },
): void {
  const target = `'${group.destination}' of transport '${group.transport}'`;
  // Equal ids of one transport imply one destination; another transport can only collide through a ':' in a name.
  if (route.transport !== group.transport) {
    throw new ConfigurationError(
      `Routes to ${target} and to '${destination}' of transport '${route.transport}' would share the handler id '${group.id}'; rename the transport or the destination`,
    );
  }
  if (group.eventTypes.includes(type)) {
    throw new ConfigurationError(`Event type ${type} is routed to ${target} twice`);
  }
  const { retry, timeoutMs } = resolveSettings(route, group.id);
  if (timeoutMs !== group.timeoutMs || !sameRetryPolicy(retry, group.retry)) {
    throw new ConfigurationError(
      `Routes to ${target} must share retry and timeout, because their events are sent in one queue per key | types=${group.eventTypes[0]},${type}`,
    );
  }
  group.eventTypes.push(type);
  group.legacyIds.push(`${group.id}:${type}`);
}

/** The retry policy and timeout a route resolves to, validated as the registry validates them. */
function resolveSettings(
  route: RouteOptions,
  id: string,
): { retry: RetryPolicy; timeoutMs: number } {
  return {
    retry: createRetryPolicy(route.retry, DEFAULT_ROUTE_RETRY_POLICY),
    timeoutMs:
      route.timeout === undefined
        ? DEFAULTS.routeTimeoutMs
        : toTimerMilliseconds(route.timeout, `Timeout of handler '${id}'`),
  };
}

function sameRetryPolicy(left: RetryPolicy, right: RetryPolicy): boolean {
  return (Object.keys(left) as (keyof RetryPolicy)[]).every(
    (field) => left[field] === right[field],
  );
}

function routedType(event: EventReference): string {
  const type = typeof event === 'string' ? event : event.type;
  assertEventType(type);
  return type;
}
