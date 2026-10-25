import { ConfigurationError } from '../errors.js';

/** The range a {@link BoundedInteger} option allows. */
export interface IntegerBounds {
  readonly name: string;
  readonly minimum: number;
  readonly maximum: number;
}

/** An integer option with fixed lower and upper bounds. */
export abstract class BoundedInteger {
  /** Returns `value` when it is an integer within the bounds, else throws a {@link ConfigurationError}. */
  static of(value: number, { name, minimum, maximum }: IntegerBounds): number {
    if (!Number.isInteger(value) || value < minimum || value > maximum) {
      throw new ConfigurationError(
        `${name} must be an integer from ${minimum} to ${maximum}, received ${value}`,
      );
    }
    return value;
  }
}
