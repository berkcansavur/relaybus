import { ConfigurationError } from '../errors.js';

/** Returns `value` when it is an integer of at least 1, else throws a {@link ConfigurationError} naming the option. */
export function assertPositiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new ConfigurationError(`${name} must be a positive integer, received ${String(value)}`);
  }
  return value;
}
