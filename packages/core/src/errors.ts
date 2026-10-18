/**
 * Base class of every error thrown by relaybus.
 */
export class RelayBusError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** Invalid options passed to a relaybus component. */
export class ConfigurationError extends RelayBusError {}

/**
 * Thrown by `publish` when no transaction can be found and the bus runs with
 * `transactionMode: 'required'` (the default). Publishing outside a transaction
 * would reintroduce the dual-write problem the outbox exists to prevent.
 */
export class TransactionRequiredError extends RelayBusError {}

/**
 * Throw from a handler to dead-letter the delivery immediately, skipping the
 * remaining retry attempts.
 */
export class NonRetryableError extends RelayBusError {}

/** A payload failed its event schema. Never retried: the payload will not change. */
export class EventValidationError extends NonRetryableError {
  constructor(
    message: string,
    readonly issues: readonly ValidationIssue[],
  ) {
    super(message);
  }
}

/** The worker no longer owns the delivery it was processing. */
export class LeaseLostError extends RelayBusError {}

/** A handler did not settle within its configured timeout. Retried like any failure. */
export class HandlerTimeoutError extends RelayBusError {}

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
