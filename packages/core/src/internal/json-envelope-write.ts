import type { EventEnvelope } from '../event.js';
import type { JsonEnvelopeFailures, JsonWireMessage } from './json-envelope.js';
import {
  ARRAY_INDEX,
  type Context,
  type Location,
  type OptionalName,
  type Plan,
} from './json-envelope-plan.js';

// Writing an envelope as a JSON message by a compiled mapping.

/** RFC 3339 years have four digits. */
const MAXIMUM_RFC3339_YEAR = 9999;

export function writeMessage(
  envelope: EventEnvelope,
  { plan, failures }: Context,
): JsonWireMessage {
  const root: Record<string, unknown> = {};
  const headers: [string, string][] = [];
  const put = (location: Location, value: string): void => {
    if ('header' in location) {
      headers.push([location.header, value]);
    } else {
      setAt(root, { path: location.path, value });
    }
  };
  if ('fixed' in plan.type) {
    if (envelope.type !== plan.type.fixed) {
      throw failures.unencodable(
        `Event ${envelope.id} has type '${envelope.type}', but this route carries only '${plan.type.fixed}'`,
      );
    }
  } else {
    put(plan.type, envelope.type);
  }
  if (plan.id !== undefined) {
    put(plan.id, envelope.id);
  }
  for (const [name, value] of optionalValues(envelope, failures)) {
    const location = plan.optional[name];
    if (location !== undefined && value !== null) {
      put(location, value);
    }
  }
  return {
    body: writeBody(envelope, { plan, root }),
    headers: Object.fromEntries([...eventHeaderEntries(envelope, { plan, failures }), ...headers]),
  };
}

function optionalValues(
  envelope: EventEnvelope,
  failures: JsonEnvelopeFailures,
): [OptionalName, string | null][] {
  return [
    ['key', envelope.key],
    ['source', envelope.source],
    ['occurredAt', formatTime(envelope, failures)],
    ['correlationId', envelope.correlationId],
    ['causationId', envelope.causationId],
  ];
}

function writeBody(
  envelope: EventEnvelope,
  { plan, root }: { readonly plan: Plan; readonly root: Record<string, unknown> },
): unknown {
  if (plan.payload === undefined) {
    return envelope.payload;
  }
  setAt(root, { path: plan.payload, value: envelope.payload });
  if (plan.headers !== undefined) {
    setAt(root, {
      path: plan.headers,
      value: Object.fromEntries(Object.entries(envelope.headers)),
    });
  }
  return root;
}

/** The event headers that travel as message headers; a name a mapped field uses is refused. */
function eventHeaderEntries(
  envelope: EventEnvelope,
  { plan, failures }: Context,
): [string, string][] {
  if (plan.headers !== undefined) {
    return [];
  }
  const entries = Object.entries(envelope.headers);
  const taken = entries.find(([name]) => plan.mappedHeaders.has(name));
  if (taken !== undefined) {
    throw failures.unencodable(
      `Event ${envelope.id} has a header '${taken[0]}', which the mapping uses for another field`,
    );
  }
  return entries;
}

function formatTime(envelope: EventEnvelope, failures: JsonEnvelopeFailures): string {
  const year = envelope.occurredAt.getUTCFullYear();
  if (Number.isNaN(year) || year < 0 || year > MAXIMUM_RFC3339_YEAR) {
    throw failures.unencodable(
      `Event ${envelope.id} has an occurredAt that is not an RFC 3339 instant (invalid date or year outside 0..9999)`,
    );
  }
  return envelope.occurredAt.toISOString();
}

/** Sets a value at a path, creating objects (arrays before an index segment) as own properties. */
function setAt(
  root: Record<string, unknown>,
  { path, value }: { readonly path: readonly string[]; readonly value: unknown },
): void {
  let container = root;
  for (const [index, segment] of path.slice(0, -1).entries()) {
    if (!Object.hasOwn(container, segment)) {
      defineValue(container, {
        name: segment,
        value: ARRAY_INDEX.test(path[index + 1]!) ? [] : {},
      });
    }
    container = container[segment] as Record<string, unknown>;
  }
  defineValue(container, { name: path.at(-1)!, value });
}

/** defineProperty, because assigning `__proto__` would set the prototype instead of a field. */
function defineValue(
  target: Record<string, unknown>,
  { name, value }: { readonly name: string; readonly value: unknown },
): void {
  Object.defineProperty(target, name, {
    value,
    enumerable: true,
    writable: true,
    configurable: true,
  });
}
