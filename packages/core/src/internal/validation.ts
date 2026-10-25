import { PositiveInteger } from '../values/positive-integer.js';

/** Returns `value` when it is an integer of at least 1, else throws a {@link ConfigurationError} naming the option. */
export function assertPositiveInteger(value: number, name: string): number {
  return PositiveInteger.of(value, name);
}
