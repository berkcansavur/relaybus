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
  visit(payload, '$', new Set(), eventId);
}

function visit(value: unknown, path: string, ancestors: Set<object>, eventId: string): void {
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return;
    case 'number':
      if (!Number.isFinite(value)) {
        throw refuse(eventId, path, `is ${value}, which JSON turns into null`);
      }
      return;
    case 'object':
      if (value !== null) {
        visitObject(value, path, ancestors, eventId);
      }
      return;
    default:
      throw refuse(eventId, path, `is ${typeof value}, which JSON cannot represent`);
  }
}

function visitObject(value: object, path: string, ancestors: Set<object>, eventId: string): void {
  if (ancestors.has(value)) {
    throw refuse(eventId, path, 'is a circular reference');
  }
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      if (!(index in value)) {
        throw refuse(eventId, `${path}[${index}]`, 'is an array hole, which JSON turns into null');
      }
      visit(value[index], `${path}[${index}]`, ancestors, eventId);
    }
  } else {
    const prototype = Object.getPrototypeOf(value) as object | null;
    if (prototype !== Object.prototype && prototype !== null) {
      throw refuse(eventId, path, 'is not a plain object (class instance, Date, Map or Set)');
    }
    if (Object.getOwnPropertySymbols(value).length > 0) {
      throw refuse(eventId, path, 'has a symbol key, which JSON drops');
    }
    for (const key of Object.keys(value)) {
      visit((value as Record<string, unknown>)[key], `${path}.${key}`, ancestors, eventId);
    }
  }
  ancestors.delete(value);
}

function refuse(eventId: string, path: string, reason: string): UnserializablePayloadError {
  return new UnserializablePayloadError(
    `Event ${eventId} has a payload JSON cannot represent faithfully: ${path} ${reason}`,
  );
}
