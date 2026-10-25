import type { EventEnvelope, EventHeaders } from '../event.js';

/** The event a transport's decoder extracted from a message; see the transport's `DecodedEvent`. */
export interface DecodedEventFields {
  readonly id: string;
  readonly type: string;
  readonly payload: unknown;
  readonly source?: string;
  readonly key?: string | null;
  readonly headers?: EventHeaders;
  readonly correlationId?: string | null;
  readonly causationId?: string | null;
  readonly occurredAt?: Date;
}

/** A fixed value, or an extractor that reads it from a plain message. */
export type TextOption<TPlain, TValue extends string | null> =
  TValue | ((message: TPlain) => TValue | undefined) | undefined;

/**
 * Observes the rejection of a promise an async decoder or extractor returned by
 * mistake: the value is refused as malformed, and an unobserved rejection would
 * otherwise crash the process.
 */
export function observeRejection(value: unknown): void {
  if (typeof value === 'object' && value !== null && 'then' in value) {
    Promise.resolve(value).catch(() => undefined);
  }
}

/**
 * The inbound plumbing a transport shares with every other: completing what a user decoder
 * returned and reading the extractor options of a plain JSON decoder. A transport supplies only its
 * malformed-message error and the defaults its messages carry (source, key, time).
 */
export abstract class DecodedEventReader<TMessage, TPlain> {
  /**
   * Fills the fields a decoder left out and guards against a decoder that returned no object,
   * which would otherwise surface as a retried `TypeError` instead of a malformed message.
   */
  complete(decoded: unknown, message: TMessage): EventEnvelope {
    if (typeof decoded !== 'object' || decoded === null) {
      throw this.invalid('The decoder must return an event object synchronously');
    }
    if ('then' in decoded) {
      observeRejection(decoded);
      throw this.invalid('The decoder must return an event object synchronously');
    }
    const event = decoded as DecodedEventFields;
    return {
      id: event.id,
      type: event.type,
      source: event.source ?? this.defaultSource(message),
      key: event.key === undefined ? this.defaultKey(message) : event.key,
      payload: event.payload,
      headers: event.headers ?? {},
      correlationId: event.correlationId ?? null,
      causationId: event.causationId ?? null,
      occurredAt: event.occurredAt ?? this.defaultTime(message),
    };
  }

  /** The text an option gives for `plain`; undefined when it gives none or `''`. */
  text<TValue extends string | null>(
    option: TextOption<TPlain, TValue>,
    { plain, name }: { readonly plain: TPlain; readonly name: string },
  ): string | undefined {
    const value: unknown = typeof option === 'function' ? option(plain) : option;
    if (value === undefined || value === null) {
      return undefined;
    }
    if (typeof value !== 'string') {
      observeRejection(value);
      throw this.invalid(`The ${name} option returned ${typeof value}, expected a string`);
    }
    return value.length === 0 ? undefined : value;
  }

  /** The error of a malformed message, which the transport dead-letters instead of retrying. */
  abstract invalid(reason: string): Error;

  protected abstract defaultSource(message: TMessage): string;

  protected abstract defaultKey(message: TMessage): string | null;

  protected abstract defaultTime(message: TMessage): Date;
}
