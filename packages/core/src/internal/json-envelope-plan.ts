import { ConfigurationError } from '../errors.js';
import { assertEventType } from '../event.js';
import { isJsonObject } from './cloudevents.js';
import type {
  ExtraValue,
  FixedType,
  JsonConstant,
  JsonEnvelopeFailures,
  JsonEnvelopeMapping,
  TimeFormat,
} from './json-envelope.js';

// Compiling a jsonEnvelope mapping into the plan that writing and reading follow.

export type Location =
  | { readonly label: string; readonly header: string }
  | { readonly label: string; readonly path: readonly string[] };

export type OptionalName = 'key' | 'source' | 'occurredAt' | 'correlationId' | 'causationId';
export const OPTIONAL_NAMES: readonly OptionalName[] = [
  'key',
  'source',
  'occurredAt',
  'correlationId',
  'causationId',
];

export type Plan = {
  readonly type: Location | FixedType;
  /** Undefined when the transport supplies the id. */
  readonly id: Location | undefined;
  /** Whether a message without the mapped id gets the transport's own id instead. */
  readonly idWhenMissing: boolean;
  readonly optional: Readonly<Partial<Record<OptionalName, Location>>>;
  /** Undefined when the payload is the whole body. */
  readonly payload: readonly string[] | undefined;
  /** Undefined when event headers are the unmapped message headers or top-level fields. */
  readonly headers: readonly string[] | undefined;
  /** Whether event headers are spread over the top level of the body. */
  readonly topLevelHeaders: boolean;
  /** Top-level body names a mapped, payload, headers or extra value uses. */
  readonly topLevelNames: ReadonlySet<string>;
  /** Headers the mapping writes itself: the mapped ones and the extra ones. */
  readonly mappedHeaders: ReadonlySet<string>;
  readonly timeFormat: TimeFormat;
  readonly extraBody: readonly ExtraBodyValue[];
  readonly extraHeaders: readonly (readonly [string, ExtraValue<string>])[];
  readonly bodyOrder: readonly string[];
};

/** An extra body value at its path. */
export type ExtraBodyValue = {
  readonly path: readonly string[];
  readonly value: ExtraValue<JsonConstant>;
};

const TIME_FORMATS: readonly TimeFormat[] = ['rfc3339', 'epoch-millis', 'epoch-seconds'];

export const ARRAY_INDEX = /^(?:0|[1-9]\d*)$/;

/** A compiled mapping with the errors of the calling transport. */
export type Context = { readonly plan: Plan; readonly failures: JsonEnvelopeFailures };

export function compile(
  mapping: JsonEnvelopeMapping<string>,
  transportIds: readonly string[],
): Plan {
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
  const payload = compilePayload(mapping.payload);
  const topLevelHeaders = mapping.headers === 'top-level-fields';
  if (topLevelHeaders && payload === undefined) {
    throw new ConfigurationError("jsonEnvelope headers 'top-level-fields' needs a body payload");
  }
  const plan: Plan = {
    type: compileType(mapping.type),
    id: compileId(mapping.id, transportIds),
    idWhenMissing: isJsonObject(mapping.id) && 'whenMissing' in mapping.id,
    optional,
    payload,
    headers: topLevelHeaders ? undefined : compileHeaders(mapping.headers),
    topLevelHeaders,
    topLevelNames: new Set(),
    mappedHeaders: new Set(),
    timeFormat: compileTimeFormat(mapping.occurredAt),
    ...compileExtras(mapping, payload !== undefined),
  };
  return assertDisjoint(plan);
}

function compileTimeFormat(field: unknown): TimeFormat {
  const format = isJsonObject(field) ? field['format'] : undefined;
  if (format === undefined) {
    return 'rfc3339';
  }
  if (!TIME_FORMATS.includes(format as TimeFormat)) {
    throw new ConfigurationError(
      `jsonEnvelope occurredAt format must be one of ${TIME_FORMATS.map((name) => `'${name}'`).join(', ')}`,
    );
  }
  return format as TimeFormat;
}

/** The extra values and the body order; a payload-only mapping has no body to put them in. */
function compileExtras(
  mapping: JsonEnvelopeMapping<string>,
  hasBody: boolean,
): Pick<Plan, 'extraBody' | 'extraHeaders' | 'bodyOrder'> {
  const extra = (mapping.extra ?? {}) as {
    readonly body?: Readonly<Record<string, ExtraValue<JsonConstant>>>;
    readonly headers?: Readonly<Record<string, ExtraValue<string>>>;
  };
  const bodyOrder = (mapping as { readonly bodyOrder?: readonly string[] }).bodyOrder ?? [];
  if (!hasBody && (extra.body !== undefined || bodyOrder.length > 0)) {
    throw new ConfigurationError(
      'jsonEnvelope with a whole-body payload has no body for extra values or a body order',
    );
  }
  return {
    extraBody: Object.entries(extra.body ?? {}).map(([path, value]) => ({
      path: toPath(path, 'extra body value'),
      value,
    })),
    extraHeaders: Object.entries(extra.headers ?? {}),
    bodyOrder,
  };
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
  const names = transportIds.map((name) => `'${name}'`).join(', ');
  if (!isJsonObject(id)) {
    throw new ConfigurationError(
      `jsonEnvelope needs an id: { body }, { header } or one of ${names}`,
    );
  }
  if ('whenMissing' in id && !transportIds.includes(id['whenMissing'] as string)) {
    throw new ConfigurationError(`jsonEnvelope id whenMissing must be one of ${names}`);
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
  const headers = [
    ...locations.flatMap((location) => ('header' in location ? [location.header] : [])),
    ...plan.extraHeaders.map(([name]) => name),
  ];
  const duplicate = headers.find((name, index) => headers.indexOf(name) !== index);
  if (duplicate !== undefined) {
    throw new ConfigurationError(`jsonEnvelope maps header '${duplicate}' twice`);
  }
  const paths = [
    ...locations.flatMap((location) => ('path' in location ? [location.path] : [])),
    ...[plan.payload, plan.headers].filter((path) => path !== undefined),
    ...plan.extraBody.map(({ path }) => path),
  ];
  assertNoNestedPaths(paths);
  return {
    ...plan,
    mappedHeaders: new Set(headers),
    topLevelNames: new Set(paths.map((path) => path[0]!)),
  };
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
