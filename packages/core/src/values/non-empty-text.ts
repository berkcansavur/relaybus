import { ConfigurationError } from '../errors.js';

/** A text option that must not be empty: a name, an exchange, a queue. */
export abstract class NonEmptyText {
  static of(value: string, name: string): string {
    if (value.length === 0) {
      throw new ConfigurationError(`${name} must not be empty`);
    }
    return value;
  }

  /** Like {@link NonEmptyText.of}, for an option that may be left out. */
  static optional(value: string | undefined, name: string): string | undefined {
    return value === undefined ? undefined : NonEmptyText.of(value, name);
  }
}
