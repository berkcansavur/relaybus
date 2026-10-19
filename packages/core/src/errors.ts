/**
 * Base class of every error thrown by relaybus.
 */
export class RelayBusError extends Error {
  // Explicit names survive minification, unlike `new.target.name`.
  override name = 'RelayBusError';
}

/** Invalid options passed to a relaybus component. */
export class ConfigurationError extends RelayBusError {
  override name = 'ConfigurationError';
}

/**
 * Thrown by `publish` when no transaction can be found and the bus runs with
 * `transactionMode: 'required'` (the default). Publishing outside a transaction
 * would reintroduce the dual-write problem the outbox exists to prevent.
 */
export class TransactionRequiredError extends RelayBusError {
  override name = 'TransactionRequiredError';
}

/**
 * Throw from a handler to dead-letter the delivery immediately, skipping the
 * remaining retry attempts.
 */
export class NonRetryableError extends RelayBusError {
  override name = 'NonRetryableError';
}

/** A payload failed its event schema. Never retried: the payload will not change. */
export class EventValidationError extends NonRetryableError {
  override name = 'EventValidationError';

  constructor(
    message: string,
    readonly issues: readonly ValidationIssue[],
  ) {
    super(message);
  }
}

/**
 * A message received from a broker is not a valid event. Never retried:
 * transports should dead-letter it instead of redelivering it forever.
 */
export class InvalidEnvelopeError extends NonRetryableError {
  override name = 'InvalidEnvelopeError';
}

/** The worker no longer owns the delivery it was processing. */
export class LeaseLostError extends RelayBusError {
  override name = 'LeaseLostError';
  /**
   * Whether the store said so (a fenced write returned false). A worker that only noticed
   * locally that its lease ran out may still own the delivery in the store and has to give it back.
   */
  readonly storeConfirmed: boolean;

  constructor(message: string, options: { readonly storeConfirmed?: boolean } = {}) {
    super(message);
    this.storeConfirmed = options.storeConfirmed ?? false;
  }
}

/** A handler did not settle within its configured timeout. Retried like any failure. */
export class HandlerTimeoutError extends RelayBusError {
  override name = 'HandlerTimeoutError';
}

/**
 * Reason of a handler's abort signal when the bus stops before the handler
 * finished. The delivery is released and runs again without losing an attempt.
 */
export class DispatcherStoppedError extends RelayBusError {
  override name = 'DispatcherStoppedError';
}

export interface ValidationIssue {
  readonly message: string;
  readonly path?: readonly PropertyKey[] | undefined;
}

/** Serializable snapshot of an error, stored with failed deliveries. */
export interface ErrorInfo {
  readonly name: string;
  readonly message: string;
  readonly stack?: string | undefined;
}

export function toErrorInfo(error: unknown): ErrorInfo {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack };
  }
  return { name: 'NonError', message: safeStringify(error) };
}

function safeStringify(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}
