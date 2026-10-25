import { toTimerMilliseconds, type Duration } from './duration.js';
import { ConfigurationError } from './errors.js';
import { assertPositiveInteger } from './internal/validation.js';
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
   * events it publishes) takes effect exactly once.
   *
   * The lease is not renewed while the transaction is open. A transactional
   * handler must finish within the lease duration (default 30s), or within its
   * `timeout`, which extends the lease once to cover it; otherwise it is aborted.
   */
  readonly transactional?: boolean;
}

/** Public description of a registered handler or route. */
export interface HandlerDescriptor {
  readonly id: string;
  readonly eventType: string;
  readonly retry: RetryPolicy;
  readonly timeoutMs: number | null;
  readonly concurrency: number | null;
  readonly transactional: boolean;
  /** True for routes that forward events to a transport. */
  readonly forwardsToTransport: boolean;
}

export interface HandlerRegistration extends Omit<HandlerDescriptor, 'eventType'> {
  /** Event types it handles: one for a handler, every type routed to the destination for a route. */
  readonly eventTypes: readonly string[];
  /**
   * Handler ids whose deliveries it runs: its id, then for a route the per-type ids that 0.2.0
   * stored its deliveries under.
   */
  readonly claimIds: readonly string[];
  readonly schema: StandardSchemaV1 | undefined;
  readonly handle: EventHandler;
}

export interface RegistrationDefaults {
  readonly retry: RetryPolicy;
  readonly timeoutMs: number | null;
}

const HANDLER_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$/;

export interface RegistrationInput {
  readonly eventTypes: readonly string[];
  /** Further ids of earlier versions whose stored deliveries this registration runs. */
  readonly legacyIds?: readonly string[];
  readonly schema: StandardSchemaV1 | undefined;
  readonly handle: EventHandler;
  readonly options: SubscribeOptions;
  readonly forwardsToTransport: boolean;
}

export class HandlerRegistry {
  readonly #byId = new Map<string, HandlerRegistration>();
  readonly #byClaimId = new Map<string, HandlerRegistration>();
  readonly #byEventType = new Map<string, HandlerRegistration[]>();

  register(input: RegistrationInput, defaults: RegistrationDefaults): HandlerRegistration {
    const { options } = input;
    if (!HANDLER_ID_PATTERN.test(options.id)) {
      throw new ConfigurationError(
        `Handler id '${options.id}' must be 1-255 characters of letters, digits, '.', '_', ':', '/' or '-'`,
      );
    }
    const claimIds = [options.id, ...(input.legacyIds ?? [])];
    this.#assertUnregistered(claimIds);
    if (options.concurrency !== undefined) {
      assertPositiveInteger(options.concurrency, `Concurrency of handler '${options.id}'`);
    }
    const registration: HandlerRegistration = {
      id: options.id,
      eventTypes: input.eventTypes,
      claimIds,
      schema: input.schema,
      handle: input.handle,
      retry: createRetryPolicy(options.retry, defaults.retry),
      timeoutMs: resolveTimeout(options.timeout, {
        fallback: defaults.timeoutMs,
        handlerId: options.id,
      }),
      concurrency: options.concurrency ?? null,
      transactional: options.transactional ?? false,
      forwardsToTransport: input.forwardsToTransport,
    };
    this.#index(registration);
    return registration;
  }

  #assertUnregistered(claimIds: readonly string[]): void {
    for (const id of claimIds) {
      if (this.#byClaimId.has(id)) {
        throw new ConfigurationError(`Handler id '${id}' is already registered`);
      }
    }
  }

  #index(registration: HandlerRegistration): void {
    this.#byId.set(registration.id, registration);
    for (const id of registration.claimIds) {
      this.#byClaimId.set(id, registration);
    }
    for (const eventType of registration.eventTypes) {
      const forType = this.#byEventType.get(eventType) ?? [];
      forType.push(registration);
      this.#byEventType.set(eventType, forType);
    }
  }

  /** The registration that runs deliveries stored under `id`, its own id or a legacy one. */
  get(id: string): HandlerRegistration | undefined {
    return this.#byClaimId.get(id);
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
  { fallback, handlerId }: { readonly fallback: number | null; readonly handlerId: string },
): number | null {
  if (timeout === undefined) {
    return fallback;
  }
  if (timeout === false) {
    return null;
  }
  return toTimerMilliseconds(timeout, `Timeout of handler '${handlerId}'`);
}
