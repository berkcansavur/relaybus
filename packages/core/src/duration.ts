import { ConfigurationError } from './errors.js';

/**
 * A duration in milliseconds, or a string such as `'250ms'`, `'30s'`, `'5m'`,
 * `'1h'` or `'7d'`.
 */
export type Duration = number | `${number}${DurationUnit}`;

type DurationUnit = 'ms' | 's' | 'm' | 'h' | 'd';

const MILLISECONDS_PER_UNIT: Record<DurationUnit, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;

export function toMilliseconds(value: Duration, name: string): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) {
      throw new ConfigurationError(
        `${name} must be a non-negative finite number of milliseconds, received ${value}`,
      );
    }
    return value;
  }
  const match = DURATION_PATTERN.exec(value);
  if (match === null) {
    throw new ConfigurationError(
      `${name} must look like '500ms', '30s', '5m', '1h' or '7d', received '${value}'`,
    );
  }
  const [, amount, unit] = match as unknown as [string, string, DurationUnit];
  return Number(amount) * MILLISECONDS_PER_UNIT[unit];
}
