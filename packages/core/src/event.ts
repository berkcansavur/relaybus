import { ConfigurationError, EventValidationError } from './errors.js';
import type { StandardSchemaV1 } from './standard-schema.js';

/** String metadata carried with an event, such as `traceparent`. */
export type EventHeaders = Readonly<Record<string, string>>;

/**
 * An event as stored and delivered. Field names follow CloudEvents where they
 * overlap (`id`, `type`, `source`, `time` as `occurredAt`, `subject` as `key`).
 */
export interface EventEnvelope<TPayload = unknown> {
  /** Globally unique id. Handlers and transports deduplicate on it. */
  readonly id: string;
  /** Event type, for example `order.created.v1`. */
  readonly type: string;
  /** Logical name of the publishing service. */
  readonly source: string;
  /** Ordering and partition key. Events sharing a key are handled in publish order. */
  readonly key: string | null;
  readonly payload: TPayload;
  readonly headers: EventHeaders;
  /** Id shared by every event of one business flow. */
  readonly correlationId: string | null;
  /** Id of the event whose handler published this one. */
  readonly causationId: string | null;
  readonly occurredAt: Date;
}

/** An instance of a class created with {@link defineEvent}. */
export interface DomainEvent<TPayload = unknown> {
  readonly type: string;
  readonly payload: TPayload;
}

/** A class created with {@link defineEvent}. */
export interface EventClass<TPayload = unknown> {
  new (payload: TPayload): DomainEvent<TPayload>;
  readonly type: string;
  readonly schema: StandardSchemaV1<unknown, TPayload> | undefined;
}

/** Payload type of an event class: `PayloadOf<typeof OrderPlaced>`. */
export type PayloadOf<TEvent> = TEvent extends EventClass<infer TPayload> ? TPayload : never;

/** Anything that names an event type: an event class or the type string itself. */
export type EventReference = { readonly type: string } | string;

export interface DefineEventOptions<TSchema extends StandardSchemaV1> {
  /** Validates the payload on publish and before every handler call. */
  readonly schema: TSchema;
}

const EVENT_TYPE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$/;

export function assertEventType(type: string): void {
  if (!EVENT_TYPE_PATTERN.test(type)) {
    throw new ConfigurationError(
      `Event type '${type}' must be 1-255 characters of letters, digits, '.', '_', ':', '/' or '-', starting with a letter or digit`,
    );
  }
}

/**
 * Defines an event type. Extend the returned class to name it:
 *
 * ```ts
 * export class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
 * export class OrderShipped extends defineEvent('order.shipped.v1', { schema: orderShippedSchema }) {}
 * ```
 */
export function defineEvent<TSchema extends StandardSchemaV1>(
  type: string,
  options: DefineEventOptions<TSchema>,
): EventClass<StandardSchemaV1.InferOutput<TSchema>>;
export function defineEvent<TPayload>(type: string): EventClass<TPayload>;
export function defineEvent(
  type: string,
  options?: DefineEventOptions<StandardSchemaV1>,
): EventClass<unknown> {
  assertEventType(type);
  const schema = options?.schema;
  return class DefinedEvent implements DomainEvent<unknown> {
    static readonly type = type;
    static readonly schema = schema;
    readonly type = type;
    constructor(readonly payload: unknown) {}
  };
}

/** Runs a Standard Schema and returns its output, or throws {@link EventValidationError}. */
// eslint-disable-next-line relaybus/max-two-params -- exported API validatePayload keeps its public signature
export async function validatePayload<TPayload>(
  type: string,
  schema: StandardSchemaV1<unknown, TPayload> | undefined,
  payload: unknown,
): Promise<TPayload> {
  if (schema === undefined) {
    return payload as TPayload;
  }
  const result = await schema['~standard'].validate(payload);
  if (result.issues !== undefined) {
    const issues = result.issues.map((issue) => ({
      message: issue.message,
      path: issue.path?.map((segment) => (typeof segment === 'object' ? segment.key : segment)),
    }));
    throw new EventValidationError(
      `Payload of '${type}' failed validation: ${issues.map((issue) => issue.message).join('; ')}`,
      issues,
    );
  }
  return result.value;
}
