import type { EventEnvelope } from '../event.js';
import { isJsonObject } from './cloudevents.js';
import type { Context } from './json-envelope-plan.js';

// The top-level body fields no mapped value uses, kept under one payload field (`otherFields`).

/** The top-level fields of `body` no other value uses and that are not read as event headers. */
function unusedFields(body: Record<string, unknown>, { plan }: Context): [string, unknown][] {
  return Object.entries(body).filter(
    ([name, value]) =>
      !plan.topLevelNames.has(name) && !(plan.topLevelHeaders && typeof value === 'string'),
  );
}

/**
 * The payload read from `body` with the unused top-level fields under the `otherFields` field. An
 * object payload always gets the field (empty when there are none), so its shape does not depend on
 * the message; a payload that is not an object stays as it is when there is nothing to keep.
 */
export function withOtherFields(
  payload: unknown,
  { body, context }: { readonly body: Record<string, unknown>; readonly context: Context },
): unknown {
  const field = context.plan.otherFields;
  if (field === undefined) {
    return payload;
  }
  const fields = unusedFields(body, context);
  if (!isJsonObject(payload) && fields.length === 0) {
    return payload;
  }
  if (!isJsonObject(payload) || Object.hasOwn(payload, field)) {
    throw context.failures.invalid(
      `otherFields keeps the unused top-level fields under the payload field '${field}', but the payload is not an object without that field`,
    );
  }
  return Object.fromEntries([...Object.entries(payload), [field, Object.fromEntries(fields)]]);
}

/** The payload to write and the fields to spread over the top level, split at `otherFields`. */
export function splitOtherFields(
  envelope: EventEnvelope,
  context: Context,
): { readonly payload: unknown; readonly fields: readonly [string, unknown][] } {
  const field = context.plan.otherFields;
  const { payload } = envelope;
  if (field === undefined || !isJsonObject(payload) || !Object.hasOwn(payload, field)) {
    return { payload, fields: [] };
  }
  const others = payload[field];
  if (!isJsonObject(others)) {
    throw context.failures.unencodable(
      `Event ${envelope.id} has a payload field '${field}' that is not an object of top-level fields`,
    );
  }
  const fields = Object.entries(others);
  const refused = fields.find((entry) => isRefused(entry, context));
  if (refused !== undefined) {
    throw context.failures.unencodable(
      `Event ${envelope.id} keeps a top-level field '${refused[0]}' under '${field}' that the mapping reads as another value`,
    );
  }
  return {
    payload: Object.fromEntries(Object.entries(payload).filter(([name]) => name !== field)),
    fields,
  };
}

/** A name another value uses, or text that would read back as an event header. */
function isRefused([name, value]: readonly [string, unknown], { plan }: Context): boolean {
  return plan.topLevelNames.has(name) || (plan.topLevelHeaders && typeof value === 'string');
}
