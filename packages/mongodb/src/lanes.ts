import { LaneLeaseTokenFormat } from '@relaybus/core/internal';

/**
 * Number of lanes. Every delivery belongs to one of them, and workers lease
 * whole lanes instead of single deliveries. Same value as in the PostgreSQL store.
 */
export const LANE_COUNT = 256;

const FNV_OFFSET_BASIS = 0x811c9dc5;
const FNV_PRIME = 0x01000193;
const utf8 = new TextEncoder();

/**
 * Lane of a delivery: the 32-bit FNV-1a hash of the UTF-8 bytes of its
 * ordering key, or of its event id when the event has no key, reduced to
 * `0..LANE_COUNT - 1`. All deliveries of one key therefore share a lane and
 * are owned by one worker at a time, which is what keeps their order. The
 * function is pure and platform independent, so every process agrees on it.
 */
export function laneOf(key: string | null, eventId: string): number {
  let hash = FNV_OFFSET_BASIS;
  for (const byte of utf8.encode(key ?? eventId)) {
    hash = Math.imul(hash ^ byte, FNV_PRIME);
  }
  return hash & (LANE_COUNT - 1);
}

/**
 * The claim a lease token names: the lane, the epoch the lane had, and the
 * claim generation the delivery had when it was claimed.
 */
export interface LaneLease {
  readonly laneId: number;
  readonly epoch: number;
  /**
   * Claim generation of the delivery. It grows whenever a claim of the delivery
   * ends (retry, release, burial, lost lease), so a lease of an earlier claim
   * never matches a later claim of the same delivery in the same lane epoch.
   */
  readonly claims: number;
}

/** Lease tokens of this store: epochs and claim generations of up to 15 digits. */
const LEASE_TOKENS = new LaneLeaseTokenFormat({
  laneCount: LANE_COUNT,
  epochDigits: 15,
  claimsDigits: 15,
});

/**
 * The fencing token of a claimed delivery: `<laneId>:<epoch>:<claims>`. Every
 * state change checks the lane epoch and the delivery's claim generation.
 */
export function formatLeaseToken(
  laneId: number,
  { epoch, claims }: { readonly epoch: number; readonly claims: number },
): string {
  return LEASE_TOKENS.format(laneId, { epoch, claims });
}

/** Splits a lease token; undefined when this store cannot have issued it. */
export function parseLeaseToken(token: string): LaneLease | undefined {
  const parts = LEASE_TOKENS.parse(token);
  if (parts === undefined) {
    return undefined;
  }
  return { laneId: parts.lane, epoch: Number(parts.epoch), claims: parts.claims };
}
