import { ConfigurationError } from '../errors.js';

/** A count option that must be a whole number of at least 1: a batch size, attempts, a limit. */
export abstract class PositiveInteger {
  /** Returns `value` when it is an integer of at least 1, else throws a {@link ConfigurationError} naming the option. */
  static of(value: number, name: string): number {
    if (!Number.isInteger(value) || value < 1) {
      throw new ConfigurationError(`${name} must be a positive integer, received ${String(value)}`);
    }
    return value;
  }
}
