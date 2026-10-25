import { UnserializablePayloadError } from './errors.js';

/**
 * Throws {@link UnserializablePayloadError} unless `JSON.stringify` represents
 * `payload` faithfully. Only `null`, strings, booleans, finite numbers, arrays
 * and plain objects built from those are accepted. Everything `JSON.stringify`
 * would silently drop (`undefined`, functions, symbols, symbol keys, array holes),
 * convert (NaN and Infinity to `null`, Dates to strings, Maps and Sets to `{}`,
 * class instances to their own properties) or reject late (BigInt, cycles) is
 * refused up front, naming the offending path.
 */
export function assertJsonPayload(payload: unknown, eventId: string): void {
  visit(payload, { path: '$', ancestors: new Set(), eventId });
}

function visit(
  value: unknown,
  {
    path,
    ancestors,
    eventId,
  }: { readonly path: string; readonly ancestors: Set<object>; readonly eventId: string },
): void {
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return;
    case 'number':
      if (!Number.isFinite(value)) {
        throw refuse(eventId, { path, reason: `is ${value}, which JSON turns into null` });
      }
      return;
    case 'object':
      if (value !== null) {
        visitObject(value, { path, ancestors, eventId });
      }
      return;
    default:
      throw refuse(eventId, { path, reason: `is ${typeof value}, which JSON cannot represent` });
  }
}

function visitObject(
  value: object,
  {
    path,
    ancestors,
    eventId,
  }: { readonly path: string; readonly ancestors: Set<object>; readonly eventId: string },
): void {
  if (ancestors.has(value)) {
    throw refuse(eventId, { path, reason: 'is a circular reference' });
  }
  ancestors.add(value);
  if (Array.isArray(value)) {
    visitArray(value, { path, ancestors, eventId });
  } else {
    visitPlainObject(value, { path, ancestors, eventId });
  }
  ancestors.delete(value);
}

/** Where a visit is: the path so far, the objects above it and the event it belongs to. */
type VisitPlace = {
  readonly path: string;
  readonly ancestors: Set<object>;
  readonly eventId: string;
};

function visitArray(value: unknown[], { path, ancestors, eventId }: VisitPlace): void {
  for (let index = 0; index < value.length; index += 1) {
    if (!(index in value)) {
      throw refuse(eventId, {
        path: `${path}[${index}]`,
        reason: 'is an array hole, which JSON turns into null',
      });
    }
    visit(value[index], { path: `${path}[${index}]`, ancestors, eventId });
  }
}

function visitPlainObject(value: object, { path, ancestors, eventId }: VisitPlace): void {
  const prototype = Object.getPrototypeOf(value) as object | null;
  if (prototype !== Object.prototype && prototype !== null) {
    throw refuse(eventId, {
      path,
      reason: 'is not a plain object (class instance, Date, Map or Set)',
    });
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw refuse(eventId, { path, reason: 'has a symbol key, which JSON drops' });
  }
  for (const key of Object.keys(value)) {
    visit((value as Record<string, unknown>)[key], {
      path: `${path}.${key}`,
      ancestors,
      eventId,
    });
  }
}

function refuse(
  eventId: string,
  { path, reason }: { readonly path: string; readonly reason: string },
): UnserializablePayloadError {
  return new UnserializablePayloadError(
    `Event ${eventId} has a payload JSON cannot represent faithfully: ${path} ${reason}`,
  );
}
