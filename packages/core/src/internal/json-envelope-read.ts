import { assertEventType, type EventHeaders } from '../event.js';
import { isJsonObject } from './cloudevents.js';
import type { JsonEnvelopeFailures, JsonEnvelopeFields, JsonWireMessage } from './json-envelope.js';
import type { Context, Location, Plan } from './json-envelope-plan.js';

// Reading the envelope fields of a JSON message by a compiled mapping.

const RFC3339_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/i;
const EPOCH_PATTERN = /^\d+$/;
const MILLISECONDS_PER_SECOND = 1000;

export function readMessage(message: JsonWireMessage, context: Context): JsonEnvelopeFields {
  const { plan, failures } = context;
  if (plan.payload !== undefined && !isJsonObject(message.body)) {
    throw failures.invalid('The message body must be a JSON object');
  }
  const read = (location: Location | undefined): string | undefined =>
    location === undefined ? undefined : readText(message, { location, failures });
  const fields: Record<string, unknown> = {
    id: readId(read(plan.id), context),
    type: readType(message, context),
    payload: readPayload(message.body, context),
    key: read(plan.optional.key),
    source: read(plan.optional.source),
    headers: readEventHeaders(message, plan),
    correlationId: read(plan.optional.correlationId) ?? null,
    causationId: read(plan.optional.causationId) ?? null,
    occurredAt: readTime(read(plan.optional.occurredAt), context),
  };
  return Object.fromEntries(
    Object.entries(fields).filter((entry) => entry[1] !== undefined),
  ) as JsonEnvelopeFields;
}

function readId(id: string | undefined, { plan, failures }: Context): string | undefined {
  if (plan.id === undefined || plan.idWhenMissing) {
    return id;
  }
  return requireText(id, { location: plan.id, failures });
}

function readType(message: JsonWireMessage, { plan, failures }: Context): string {
  if ('fixed' in plan.type) {
    return plan.type.fixed;
  }
  const location = plan.type;
  const type = requireText(readText(message, { location, failures }), { location, failures });
  try {
    assertEventType(type);
  } catch (error) {
    throw failures.invalid(`The event type is not a valid event type: ${(error as Error).message}`);
  }
  return type;
}

function readPayload(body: unknown, { plan, failures }: Context): unknown {
  if (plan.payload === undefined) {
    return body;
  }
  const payload = valueAt(body, plan.payload);
  if (payload === undefined) {
    throw failures.invalid(`The message has no payload at '${plan.payload.join('.')}'`);
  }
  return payload;
}

function readEventHeaders(message: JsonWireMessage, plan: Plan): EventHeaders {
  if (plan.topLevelHeaders) {
    return textEntries(
      Object.entries(message.body as Record<string, unknown>).filter(
        ([name]) => !plan.topLevelNames.has(name),
      ),
    );
  }
  if (plan.headers === undefined) {
    return Object.fromEntries(
      Object.entries(message.headers).filter(([name]) => !plan.mappedHeaders.has(name)),
    );
  }
  const headers = valueAt(message.body, plan.headers);
  return isJsonObject(headers) ? textEntries(Object.entries(headers)) : {};
}

function textEntries(entries: readonly [string, unknown][]): EventHeaders {
  return Object.fromEntries(
    entries.filter((entry): entry is [string, string] => typeof entry[1] === 'string'),
  );
}

function readTime(text: string | undefined, { plan, failures }: Context): Date | undefined {
  if (text === undefined) {
    return undefined;
  }
  if (plan.timeFormat !== 'rfc3339') {
    const time = new Date(
      Number(text) * (plan.timeFormat === 'epoch-seconds' ? MILLISECONDS_PER_SECOND : 1),
    );
    if (!EPOCH_PATTERN.test(text) || Number.isNaN(time.getTime())) {
      throw failures.invalid(`The event time is not a whole ${plan.timeFormat} number: '${text}'`);
    }
    return time;
  }
  const time = new Date(text);
  if (!RFC3339_PATTERN.test(text) || Number.isNaN(time.getTime())) {
    throw failures.invalid(`The event time is not an RFC 3339 timestamp: '${text}'`);
  }
  return time;
}

/** A string, or a safe integer as its decimal text; undefined when absent, null or empty. */
function readText(
  message: JsonWireMessage,
  { location, failures }: { readonly location: Location; readonly failures: JsonEnvelopeFailures },
): string | undefined {
  const value =
    'header' in location
      ? Object.hasOwn(message.headers, location.header)
        ? message.headers[location.header]
        : undefined
      : valueAt(message.body, location.path);
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' && Number.isSafeInteger(value)) {
    return String(value);
  }
  throw failures.invalid(`The ${location.label} at ${describe(location)} must be a string`);
}

function requireText(
  value: string | undefined,
  { location, failures }: { readonly location: Location; readonly failures: JsonEnvelopeFailures },
): string {
  if (value === undefined) {
    throw failures.invalid(`The message has no ${location.label} at ${describe(location)}`);
  }
  return value;
}

function describe(location: Location): string {
  return 'header' in location ? `header '${location.header}'` : `'${location.path.join('.')}'`;
}

function valueAt(body: unknown, path: readonly string[]): unknown {
  let value = body;
  for (const segment of path) {
    if (typeof value !== 'object' || value === null || !Object.hasOwn(value, segment)) {
      return undefined;
    }
    value = (value as Record<string, unknown>)[segment];
  }
  return value;
}
