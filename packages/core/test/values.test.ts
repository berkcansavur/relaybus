import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { CompositeKey, DeliveryLeaseIdentity, LaneLeaseTokenFormat } from '@relaybus/core/internal';

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
