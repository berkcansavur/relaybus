import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { ConfigurationError, toMilliseconds, type Duration } from '@relaybus/core';
import {
  BoundedInteger,
  CompositeKey,
  DeliveryLeaseIdentity,
  ExponentialBackoff,
  LaneLeaseTokenFormat,
  Milliseconds,
  NonEmptyText,
  PositiveInteger,
} from '@relaybus/core/internal';

// Each value object replaces code that was written out at its call sites before 0.3.1. The old
// forms are kept here verbatim: the new ones must answer exactly like them.

const OLD_POSTGRES_TOKEN = /^(\d{1,3}):(\d{1,18}):(\d{1,9})$/;
const OLD_MONGO_TOKEN = /^(\d{1,3}):(\d{1,15}):(\d{1,15})$/;
const LANE_COUNT = 256;

function oldParse(pattern: RegExp, token: string): unknown {
  const match = pattern.exec(token);
  if (match === null || Number(match[1]) >= LANE_COUNT) {
    return undefined;
  }
  return { lane: Number(match[1]), epoch: match[2], claims: Number(match[3]) };
}

const tokenPart = fc.oneof(
  fc.nat({ max: 400 }).map(String),
  fc.bigInt({ min: 0n, max: 10n ** 20n }).map(String),
  fc.string({ maxLength: 4 }),
);
const token = fc.oneof(
  fc.tuple(tokenPart, tokenPart, tokenPart).map((parts) => parts.join(':')),
  fc.string({ maxLength: 30 }),
);

describe('CompositeKey and DeliveryLeaseIdentity', () => {
  it('join two parts with NUL, as the call sites did', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), (first, second) => {
        expect(CompositeKey.of(first, second)).toBe(`${first}\u0000${second}`);
        expect(DeliveryLeaseIdentity.of(first, second)).toBe(`${first}\u0000${second}`);
        expect(CompositeKey.startOf(first)).toBe(`${first}\u0000`);
        expect(DeliveryLeaseIdentity.prefixOf(first)).toBe(`${first}\u0000`);
      }),
    );
  });
});

describe('LaneLeaseTokenFormat', () => {
  const postgres = new LaneLeaseTokenFormat({
    laneCount: LANE_COUNT,
    epochDigits: 18,
    claimsDigits: 9,
  });
  const mongo = new LaneLeaseTokenFormat({
    laneCount: LANE_COUNT,
    epochDigits: 15,
    claimsDigits: 15,
  });

  it('parses exactly the tokens the stores parsed before', () => {
    fc.assert(
      fc.property(token, (text) => {
        expect(postgres.parse(text)).toEqual(oldParse(OLD_POSTGRES_TOKEN, text));
        expect(mongo.parse(text)).toEqual(oldParse(OLD_MONGO_TOKEN, text));
      }),
      { numRuns: 2_000 },
    );
  });

  it('formats lane, epoch and claims as the stores did', () => {
    expect(postgres.format(7, { epoch: '1730000000000', claims: 2 })).toBe('7:1730000000000:2');
    expect(mongo.format(255, { epoch: 3, claims: 0 })).toBe('255:3:0');
    expect(postgres.parse('256:1:1')).toBeUndefined();
  });
});

/** What a validator answers: its value, or the class and message of its refusal. */
function answer(work: () => unknown): unknown {
  try {
    return { value: work() };
  } catch (error) {
    return { error: `${(error as Error).constructor.name}: ${(error as Error).message}` };
  }
}

const durations: Duration[] = [
  0,
  1,
  250,
  2_147_483_647,
  2_147_483_648,
  -1,
  Number.NaN,
  '0ms',
  '30s',
  '1.5m',
  '25d',
  'soon' as Duration,
];

describe('Milliseconds', () => {
  function oldPositive(value: Duration, name: string): number {
    const milliseconds = toMilliseconds(value, name);
    if (milliseconds === 0) {
      throw new ConfigurationError(`${name} must be greater than zero`);
    }
    return milliseconds;
  }

  function oldTimer(value: Duration, name: string): number {
    const milliseconds = toMilliseconds(value, name);
    if (milliseconds === 0) {
      throw new ConfigurationError(`${name} must be greater than zero`);
    }
    if (milliseconds > 2_147_483_647) {
      throw new ConfigurationError(
        `${name} must not exceed 2147483647ms (about 24.8 days), received ${milliseconds}ms`,
      );
    }
    return milliseconds;
  }

  it.each(durations)('converts %s like the code it replaces', (value) => {
    expect(answer(() => Milliseconds.of(value, 'opt'))).toEqual(
      answer(() => toMilliseconds(value, 'opt')),
    );
    expect(answer(() => Milliseconds.positive(value, 'opt'))).toEqual(
      answer(() => oldPositive(value, 'opt')),
    );
    expect(answer(() => Milliseconds.forTimer(value, 'opt'))).toEqual(
      answer(() => oldTimer(value, 'opt')),
    );
  });
});

describe('ExponentialBackoff', () => {
  it('doubles from the initial delay up to the maximum, like the inline formula', () => {
    fc.assert(
      fc.property(
        fc.nat({ max: 100_000 }),
        fc.nat({ max: 1_000_000 }),
        fc.integer({ min: 1, max: 80 }),
        (initialMs, maxMs, attempt) => {
          expect(new ExponentialBackoff({ initialMs, maxMs }).delayFor(attempt)).toBe(
            Math.min(initialMs * 2 ** (attempt - 1), maxMs),
          );
        },
      ),
    );
  });
});

describe('PositiveInteger, BoundedInteger and NonEmptyText', () => {
  const numbers = [-1, 0, 0.5, 1, 3, 5, 6, Number.NaN, Number.POSITIVE_INFINITY];

  function oldMaxInFlight(value: number): number {
    if (!Number.isInteger(value) || value < 1 || value > 5) {
      throw new ConfigurationError(
        `maxInFlightRequests must be an integer from 1 to 5, received ${value}`,
      );
    }
    return value;
  }

  it.each(numbers)('checks %s like the code it replaces', (value) => {
    expect(
      answer(() =>
        BoundedInteger.of(value, { name: 'maxInFlightRequests', minimum: 1, maximum: 5 }),
      ),
    ).toEqual(answer(() => oldMaxInFlight(value)));
    expect(answer(() => PositiveInteger.of(value, 'n'))).toEqual(
      Number.isInteger(value) && value >= 1
        ? { value }
        : { error: `ConfigurationError: n must be a positive integer, received ${String(value)}` },
    );
  });

  it('refuses empty text and passes everything else', () => {
    expect(answer(() => NonEmptyText.of('', 'queue'))).toEqual({
      error: 'ConfigurationError: queue must not be empty',
    });
    expect(NonEmptyText.of('q', 'queue')).toBe('q');
    expect(NonEmptyText.optional(undefined, 'queue')).toBeUndefined();
    expect(answer(() => NonEmptyText.optional('', 'queue'))).toEqual({
      error: 'ConfigurationError: queue must not be empty',
    });
  });
});
