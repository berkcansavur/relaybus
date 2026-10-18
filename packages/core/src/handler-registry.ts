import { toMilliseconds, type Duration } from './duration.js';
import { ConfigurationError } from './errors.js';
import type { EventEnvelope } from './event.js';
import { createRetryPolicy, type RetryPolicy, type RetryPolicyOptions } from './retry-policy.js';
import type { StandardSchemaV1 } from './standard-schema.js';

/** Passed to every handler call. */
export interface HandlerContext<TTransaction = unknown> {
  readonly deliveryId: string;
  readonly handler: string;
  /** Attempt number, starting at 1. */
  readonly attempt: number;
  readonly maxAttempts: number;
  /** Aborted when the handler times out, the lease is lost or the bus stops. */
  readonly signal: AbortSignal;
  /**
   * Transaction the delivery completes in, for handlers subscribed with
   * `transactional: true`. Write through it to make the handler's effects and
   * the completion atomic. Undefined otherwise.
   */
  readonly transaction: TTransaction | undefined;
}

export type EventHandler<TPayload = unknown, TTransaction = unknown> = (
  event: EventEnvelope<TPayload>,
  context: HandlerContext<TTransaction>,
) => unknown;

export interface SubscribeOptions {
  /**
   * Stable id of the handler. Deliveries are recorded under it, so renaming it
   * orphans pending deliveries. Letters, digits and `._:/-`.
   */
  readonly id: string;
  /** Overrides the bus retry policy field by field. */
  readonly retry?: RetryPolicyOptions;
  /** Abort and retry when a call takes longer. `false` disables the bus default. */
  readonly timeout?: Duration | false;
  /** Maximum concurrent deliveries of this handler in this process. */
  readonly concurrency?: number;
  /**
   * Run the handler inside a store transaction that also completes the
   * delivery: its writes and the completion commit or roll back together,
   * so a handler that writes only through `context.transaction` (or through
   * events it publishes) takes effect exactly once. Keep such handlers short.
   */
  readonly transactional?: boolean;
}

export interface HandlerRegistration {
  readonly id: string;
  readonly eventType: string;
  readonly schema: StandardSchemaV1 | undefined;
  readonly handle: EventHandler;
  readonly retry: RetryPolicy;
  readonly timeoutMs: number | null;
  readonly concurrency: number | null;
  readonly transactional: boolean;
  /** Route handlers forward events to a transport. Received events never fan out to them. */
  readonly forwardsToTransport: boolean;
}

export interface RegistrationDefaults {
  readonly retry: RetryPolicy;
  readonly timeoutMs: number | null;
}

const HANDLER_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$/;

export interface RegistrationInput {
  readonly eventType: string;
  readonly schema: StandardSchemaV1 | undefined;
  readonly handle: EventHandler;
  readonly options: SubscribeOptions;
  readonly forwardsToTransport: boolean;
}

export class HandlerRegistry {
  readonly #byId = new Map<string, HandlerRegistration>();
  readonly #byEventType = new Map<string, HandlerRegistration[]>();

  register(input: RegistrationInput, defaults: RegistrationDefaults): HandlerRegistration {
    const { options } = input;
    if (!HANDLER_ID_PATTERN.test(options.id)) {
      throw new ConfigurationError(
        `Handler id '${options.id}' must be 1-255 characters of letters, digits, '.', '_', ':', '/' or '-'`,
      );
    }
    if (this.#byId.has(options.id)) {
      throw new ConfigurationError(`Handler id '${options.id}' is already registered`);
    }
    if (
      options.concurrency !== undefined &&
      (!Number.isInteger(options.concurrency) || options.concurrency < 1)
    ) {
      throw new ConfigurationError(
        `Concurrency of handler '${options.id}' must be a positive integer, received ${options.concurrency}`,
      );
    }
    const registration: HandlerRegistration = {
      id: options.id,
      eventType: input.eventType,
      schema: input.schema,
      handle: input.handle,
      retry: createRetryPolicy(options.retry, defaults.retry),
      timeoutMs: resolveTimeout(options.timeout, defaults.timeoutMs, options.id),
      concurrency: options.concurrency ?? null,
      transactional: options.transactional ?? false,
      forwardsToTransport: input.forwardsToTransport,
    };
    this.#byId.set(registration.id, registration);
    const forType = this.#byEventType.get(registration.eventType) ?? [];
    forType.push(registration);
    this.#byEventType.set(registration.eventType, forType);
    return registration;
  }

  get(id: string): HandlerRegistration | undefined {
    return this.#byId.get(id);
  }

  forEventType(eventType: string): readonly HandlerRegistration[] {
    return this.#byEventType.get(eventType) ?? [];
  }

  all(): readonly HandlerRegistration[] {
    return [...this.#byId.values()];
  }
}

function resolveTimeout(
  timeout: Duration | false | undefined,
  fallback: number | null,
  handlerId: string,
): number | null {
  if (timeout === undefined) {
    return fallback;
  }
  if (timeout === false) {
    return null;
  }
  const milliseconds = toMilliseconds(timeout, `timeout of handler '${handlerId}'`);
  if (milliseconds === 0) {
    throw new ConfigurationError(`Timeout of handler '${handlerId}' must be greater than zero`);
  }
  return milliseconds;
}
