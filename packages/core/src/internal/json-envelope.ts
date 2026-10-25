import { ConfigurationError } from '../errors.js';
import { assertEventType, type EventEnvelope, type EventHeaders } from '../event.js';
import { isJsonObject } from './cloudevents.js';

/**
 * A field of the JSON body, by dotted path: `'eventName'`, `'meta.correlationId'`. A segment that
 * is a whole number indexes an array: `'messageType.0'` is the first element of `messageType`.
 */
export type BodyField = { readonly body: string };
/** A message header (Kafka record header, AMQP message header), by exact name. */
export type HeaderField = { readonly header: string };
/** Where one envelope value lives on the wire. */
export type WireField = BodyField | HeaderField;
/** The one event type every message of the route carries: written nowhere, used for every message read. */
export type FixedType = { readonly fixed: string };

type MappingFields<TField extends WireField, TTransportId extends string> = {
  /** Where the event type lives, or the one type of the route. Required. */
  readonly type: TField | FixedType;
  /**
   * Where the event id lives, or the transport's own message identity (see the transport's
   * mapping type). Required, so the inbox dedup key is always a deliberate choice. A message
   * whose mapped id is missing is malformed.
   */
  readonly id: TField | TTransportId;
  /** Ordering key. Default: the transport's own (the Kafka record key; none on RabbitMQ). */
  readonly key?: TField;
  /** Producing service. Default: the transport's own (Kafka topic; RabbitMQ `appId`, else the routing key). */
  readonly source?: TField;
  /** RFC 3339 time. Default: the transport's own (Kafka record timestamp; RabbitMQ `timestamp`, seconds). */
  readonly occurredAt?: TField;
  readonly correlationId?: TField;
  readonly causationId?: TField;
  /** Media type written with the message. Default `application/json`. Not checked on read. */
  readonly contentType?: string;
};

/**
 * A JSON object that wraps the payload with event metadata, such as
 * `{ "eventName": ..., "messageId": ..., "data": {...}, "meta": {...} }`.
 * Each value lives at a body path or in a header.
 */
export type BodyEnvelopeMapping<TTransportId extends string> = MappingFields<
  WireField,
  TTransportId
> & {
  /** Body path of the payload. Required. */
  readonly payload: BodyField;
  /**
   * Event headers: a body path holding an object of strings, or `'message-headers'` (default),
   * every message header that no other field maps.
   */
  readonly headers?: BodyField | 'message-headers';
};

/**
 * The body is the payload itself, as a plain JSON producer writes it; every other value lives in
 * a header, so body paths are a compile error here.
 */
export type PayloadOnlyMapping<TTransportId extends string> = MappingFields<
  HeaderField,
  TTransportId
> & {
  readonly payload: 'whole-body';
  /** Event headers: every message header that no other field maps. */
  readonly headers?: 'message-headers';
};

/** Where every envelope value lives in a JSON message. `type`, `id` and `payload` are required. */
export type JsonEnvelopeMapping<TTransportId extends string> =
  BodyEnvelopeMapping<TTransportId> | PayloadOnlyMapping<TTransportId>;

/** A JSON message: the parsed body and the text headers. */
export type JsonWireMessage = { readonly body: unknown; readonly headers: EventHeaders };

/** The values read from a message. An absent optional value is left to the transport's default. */
export type JsonEnvelopeFields = {
  readonly id?: string;
  readonly type: string;
  readonly payload: unknown;
  readonly key?: string;
  readonly source?: string;
  readonly headers: EventHeaders;
  readonly correlationId: string | null;
  readonly causationId: string | null;
  readonly occurredAt?: Date;
};

/** The errors of the calling transport. */
export type JsonEnvelopeFailures = {
  /** For a message that cannot be read: it goes to the invalid-message path. */
  readonly invalid: (reason: string) => Error;
  /** For an event that cannot be written: never retried. */
  readonly unencodable: (reason: string) => Error;
};

/** Writes and reads one mapping. Created once per codec; both methods are pure. */
export interface JsonEnvelopeFormat {
  /** The mapped headers and the body; the id is left out when the transport supplies it. */
  write(envelope: EventEnvelope): JsonWireMessage;
  read(message: JsonWireMessage): JsonEnvelopeFields;
}

type Location =
  | { readonly label: string; readonly header: string }
  | { readonly label: string; readonly path: readonly string[] };

type OptionalName = 'key' | 'source' | 'occurredAt' | 'correlationId' | 'causationId';
const OPTIONAL_NAMES: readonly OptionalName[] = [
  'key',
  'source',
  'occurredAt',
  'correlationId',
  'causationId',
];

type Plan = {
  readonly type: Location | FixedType;
  /** Undefined when the transport supplies the id. */
  readonly id: Location | undefined;
  readonly optional: Readonly<Partial<Record<OptionalName, Location>>>;
  /** Undefined when the payload is the whole body. */
  readonly payload: readonly string[] | undefined;
  /** Undefined when event headers are the unmapped message headers. */
  readonly headers: readonly string[] | undefined;
  readonly mappedHeaders: ReadonlySet<string>;
};

/** RFC 3339 years have four digits. */
const MAXIMUM_RFC3339_YEAR = 9999;
const RFC3339_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/i;
const ARRAY_INDEX = /^(?:0|[1-9]\d*)$/;

/**
 * Compiles `mapping` once. `transportIds` lists the transport's own id strategies.
 *
 * @throws ConfigurationError for a mapping that is incomplete, names a header twice, nests one
 * body path inside another or mixes array indexes and names under one body field.
 */
export function createJsonEnvelopeFormat(
  mapping: JsonEnvelopeMapping<string>,
  {
    transportIds,
    failures,
  }: { readonly transportIds: readonly string[]; readonly failures: JsonEnvelopeFailures },
): JsonEnvelopeFormat {
  const plan = compile(mapping, transportIds);
  return {
    write: (envelope) => writeMessage(envelope, { plan, failures }),
    read: (message) => readMessage(message, { plan, failures }),
  };
}

function compile(mapping: JsonEnvelopeMapping<string>, transportIds: readonly string[]): Plan {
  if (!isJsonObject(mapping)) {
    throw new ConfigurationError('jsonEnvelope needs a mapping object');
  }
  const optional: Partial<Record<OptionalName, Location>> = {};
  for (const name of OPTIONAL_NAMES) {
    const field = mapping[name];
    if (field !== undefined) {
      optional[name] = toLocation(field, name);
    }
  }
  const plan: Plan = {
    type: compileType(mapping.type),
    id: compileId(mapping.id, transportIds),
    optional,
    payload: compilePayload(mapping.payload),
    headers: compileHeaders(mapping.headers),
    mappedHeaders: new Set(),
  };
  return assertDisjoint(plan);
}

function compileType(type: unknown): Location | FixedType {
  if (isJsonObject(type) && typeof type['fixed'] === 'string') {
    assertEventType(type['fixed']);
    return { fixed: type['fixed'] };
  }
  if (type === undefined) {
    throw new ConfigurationError('jsonEnvelope needs a type: { body }, { header } or { fixed }');
  }
  return toLocation(type, 'type');
}

function compileId(id: unknown, transportIds: readonly string[]): Location | undefined {
  if (typeof id === 'string' && transportIds.includes(id)) {
    return undefined;
  }
  if (!isJsonObject(id)) {
    throw new ConfigurationError(
      `jsonEnvelope needs an id: { body }, { header } or one of ${transportIds.map((name) => `'${name}'`).join(', ')}`,
    );
  }
  return toLocation(id, 'id');
}

function compilePayload(payload: unknown): readonly string[] | undefined {
  if (payload === 'whole-body') {
    return undefined;
  }
  if (!isJsonObject(payload) || typeof payload['body'] !== 'string') {
    throw new ConfigurationError(
      "jsonEnvelope needs a payload: { body: '<path>' } or 'whole-body'",
    );
  }
  return toPath(payload['body'], 'payload');
}

function compileHeaders(headers: unknown): readonly string[] | undefined {
  if (headers === undefined || headers === 'message-headers') {
    return undefined;
  }
  if (!isJsonObject(headers) || typeof headers['body'] !== 'string') {
    throw new ConfigurationError(
      "jsonEnvelope headers must be { body: '<path>' } or 'message-headers'",
    );
  }
  return toPath(headers['body'], 'headers');
}

function toLocation(field: unknown, label: string): Location {
  if (isJsonObject(field) && typeof field['header'] === 'string' && field['header'].length > 0) {
    return { label, header: field['header'] };
  }
  if (isJsonObject(field) && typeof field['body'] === 'string') {
    return { label, path: toPath(field['body'], label) };
  }
  throw new ConfigurationError(
    `jsonEnvelope ${label} must be { body: '<path>' } or { header: '<name>' }`,
  );
}

function toPath(path: string, label: string): readonly string[] {
  const segments = path.split('.');
  if (segments.some((segment) => segment.length === 0)) {
    throw new ConfigurationError(
      `jsonEnvelope ${label} needs a dotted path of non-empty names, received '${path}'`,
    );
  }
  return segments;
}

/** Collects the mapped headers and refuses overlapping locations, which would overwrite each other. */
function assertDisjoint(plan: Plan): Plan {
  const locations = [plan.type, plan.id, ...Object.values(plan.optional)].filter(
    (location): location is Location => location !== undefined && !('fixed' in location),
  );
  const headers = locations.flatMap((location) => ('header' in location ? [location.header] : []));
  const duplicate = headers.find((name, index) => headers.indexOf(name) !== index);
  if (duplicate !== undefined) {
    throw new ConfigurationError(`jsonEnvelope maps header '${duplicate}' twice`);
  }
  const paths = [
    ...locations.flatMap((location) => ('path' in location ? [location.path] : [])),
    ...[plan.payload, plan.headers].filter((path) => path !== undefined),
  ];
  assertNoNestedPaths(paths);
  return { ...plan, mappedHeaders: new Set(headers) };
}

function assertNoNestedPaths(paths: readonly (readonly string[])[]): void {
  paths.forEach((path, index) => {
    for (const other of paths.slice(index + 1)) {
      assertCompatiblePaths(path, other);
    }
  });
}

/** Two body paths must part ways, and where they do, both index an array or both name a field. */
function assertCompatiblePaths(path: readonly string[], other: readonly string[]): void {
  const parting = path.findIndex((segment, position) => segment !== other[position]);
  const both = `'${path.join('.')}' and '${other.join('.')}'`;
  if (parting === -1 || parting === other.length) {
    throw new ConfigurationError(`jsonEnvelope body paths ${both} overlap`);
  }
  if (ARRAY_INDEX.test(path[parting]!) !== ARRAY_INDEX.test(other[parting]!)) {
    throw new ConfigurationError(`jsonEnvelope body paths ${both} mix an array index and a name`);
  }
}

type Context = { readonly plan: Plan; readonly failures: JsonEnvelopeFailures };

function writeMessage(envelope: EventEnvelope, { plan, failures }: Context): JsonWireMessage {
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

function readMessage(message: JsonWireMessage, context: Context): JsonEnvelopeFields {
  const { plan, failures } = context;
  if (plan.payload !== undefined && !isJsonObject(message.body)) {
    throw failures.invalid('The message body must be a JSON object');
  }
  const read = (location: Location | undefined): string | undefined =>
    location === undefined ? undefined : readText(message, { location, failures });
  const fields: Record<string, unknown> = {
    id:
      plan.id === undefined
        ? undefined
        : requireText(read(plan.id), { location: plan.id, failures }),
    type: readType(message, context),
    payload: readPayload(message.body, context),
    key: read(plan.optional.key),
    source: read(plan.optional.source),
    headers: readEventHeaders(message, plan),
    correlationId: read(plan.optional.correlationId) ?? null,
    causationId: read(plan.optional.causationId) ?? null,
    occurredAt: readTime(read(plan.optional.occurredAt), failures),
  };
  return Object.fromEntries(
    Object.entries(fields).filter((entry) => entry[1] !== undefined),
  ) as JsonEnvelopeFields;
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
  if (plan.headers === undefined) {
    return Object.fromEntries(
      Object.entries(message.headers).filter(([name]) => !plan.mappedHeaders.has(name)),
    );
  }
  const headers = valueAt(message.body, plan.headers);
  return isJsonObject(headers)
    ? Object.fromEntries(
        Object.entries(headers).filter(
          (entry): entry is [string, string] => typeof entry[1] === 'string',
        ),
      )
    : {};
}

function readTime(text: string | undefined, failures: JsonEnvelopeFailures): Date | undefined {
  if (text === undefined) {
    return undefined;
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
