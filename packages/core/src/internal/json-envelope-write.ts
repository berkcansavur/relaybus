import type { EventEnvelope } from '../event.js';
import type { JsonEnvelopeFailures, JsonWireMessage, TimeFormat } from './json-envelope.js';
import {
  ARRAY_INDEX,
  type Context,
  type Location,
  type OptionalName,
  type Plan,
} from './json-envelope-plan.js';
import { splitOtherFields } from './json-envelope-other-fields.js';

// Writing an envelope as a JSON message by a compiled mapping.

/** RFC 3339 years have four digits. */
const MAXIMUM_RFC3339_YEAR = 9999;
const MILLISECONDS_PER_SECOND = 1000;

export function writeMessage(
  envelope: EventEnvelope,
  { plan, failures }: Context,
): JsonWireMessage {
  const root: Record<string, unknown> = {};
  const headers: [string, string][] = [];
  const put = (location: Location, value: string | number): void => {
    if ('header' in location) {
      headers.push([location.header, String(value)]);
    } else {
      setAt(root, { path: location.path, value });
    }
  };
  const typeAt = typeLocation(envelope, { plan, failures });
  if (typeAt !== undefined) {
    put(typeAt, envelope.type);
  }
  if (plan.id !== undefined) {
    put(plan.id, envelope.id);
  }
  for (const [name, value] of optionalValues(envelope, { plan, failures })) {
    const location = plan.optional[name];
    if (location !== undefined && value !== null) {
      put(location, value);
    }
  }
  const { payload, fields } = splitOtherFields(envelope, { plan, failures });
  const body = writeBody(envelope, { plan, root, payload });
  if (plan.topLevelHeaders) {
    spreadHeaders(envelope, { plan, failures, root: body as Record<string, unknown> });
  }
  spreadOtherFields(envelope, { failures, fields, root: body as Record<string, unknown> });
  return {
    body,
    headers: Object.fromEntries([
      ...eventHeaderEntries(envelope, { plan, failures }),
      ...headers,
      ...extraHeaderEntries(envelope, plan),
    ]),
  };
}

/** Where the type is written; a fixed type is written nowhere and refuses any other type. */
function typeLocation(envelope: EventEnvelope, { plan, failures }: Context): Location | undefined {
  if (!('fixed' in plan.type)) {
    return plan.type;
  }
  if (envelope.type !== plan.type.fixed) {
    throw failures.unencodable(
      `Event ${envelope.id} has type '${envelope.type}', but this route carries only '${plan.type.fixed}'`,
    );
  }
  return undefined;
}

function extraHeaderEntries(envelope: EventEnvelope, plan: Plan): [string, string][] {
  return plan.extraHeaders.map(([name, value]) => [
    name,
    typeof value === 'function' ? value(envelope) : value,
  ]);
}

function optionalValues(
  envelope: EventEnvelope,
  { plan, failures }: Context,
): [OptionalName, string | number | null][] {
  return [
    ['key', envelope.key],
    ['source', envelope.source],
    [
      'occurredAt',
      timeValue(formatTime(envelope, failures), { envelope, format: plan.timeFormat }),
    ],
    ['correlationId', envelope.correlationId],
    ['causationId', envelope.causationId],
  ];
}

function writeBody(
  envelope: EventEnvelope,
  {
    plan,
    root,
    payload,
  }: { readonly plan: Plan; readonly root: Record<string, unknown>; readonly payload: unknown },
): unknown {
  if (plan.payload === undefined) {
    return payload;
  }
  setAt(root, { path: plan.payload, value: payload });
  if (plan.headers !== undefined) {
    setAt(root, {
      path: plan.headers,
      value: Object.fromEntries(Object.entries(envelope.headers)),
    });
  }
  for (const { path, value } of plan.extraBody) {
    setAt(root, { path, value: typeof value === 'function' ? value(envelope) : value });
  }
  return ordered(root, plan.bodyOrder);
}

/** Event headers spread over the top level of the body, last; a name another value uses is refused. */
function spreadHeaders(
  envelope: EventEnvelope,
  { plan, failures, root }: Context & { readonly root: Record<string, unknown> },
): void {
  for (const [name, value] of Object.entries(envelope.headers)) {
    if (plan.topLevelNames.has(name)) {
      throw failures.unencodable(
        `Event ${envelope.id} has a header '${name}', which the mapping uses for another body field`,
      );
    }
    setAt(root, { path: [name], value });
  }
}

/** The fields `otherFields` kept, spread over the top level after everything else; a taken name is refused. */
function spreadOtherFields(
  envelope: EventEnvelope,
  {
    failures,
    fields,
    root,
  }: {
    readonly failures: JsonEnvelopeFailures;
    readonly fields: readonly [string, unknown][];
    readonly root: Record<string, unknown>;
  },
): void {
  for (const [name, value] of fields) {
    if (Object.hasOwn(root, name)) {
      throw failures.unencodable(
        `Event ${envelope.id} keeps a top-level field '${name}' that an event header also writes`,
      );
    }
    setAt(root, { path: [name], value });
  }
}

/** The time as the format writes it; the RFC 3339 text is checked first either way. */
function timeValue(
  text: string,
  { envelope, format }: { readonly envelope: EventEnvelope; readonly format: TimeFormat },
): string | number {
  const milliseconds = envelope.occurredAt.getTime();
  if (format === 'epoch-millis') {
    return milliseconds;
  }
  return format === 'epoch-seconds' ? Math.floor(milliseconds / MILLISECONDS_PER_SECOND) : text;
}

/** The body with the listed top-level fields first, in the listed order. */
function ordered(root: Record<string, unknown>, order: readonly string[]): Record<string, unknown> {
  if (order.length === 0) {
    return root;
  }
  const result: Record<string, unknown> = {};
  const names = [...order.filter((name) => Object.hasOwn(root, name)), ...Object.keys(root)];
  for (const name of names) {
    if (!Object.hasOwn(result, name)) {
      defineValue(result, { name, value: root[name] });
    }
  }
  return result;
}

/** The event headers that travel as message headers; a name a mapped field uses is refused. */
function eventHeaderEntries(
  envelope: EventEnvelope,
  { plan, failures }: Context,
): [string, string][] {
  if (plan.headers !== undefined || plan.topLevelHeaders) {
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
