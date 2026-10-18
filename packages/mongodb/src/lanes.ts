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

/** The lane lease a lease token names: the lane and the epoch it had when the delivery was claimed. */
export interface LaneLease {
  readonly laneId: number;
  readonly epoch: number;
}

/**
 * The fencing token of a claimed delivery: `<laneId>:<epoch>`. Every state
 * change checks the lane with it, without reading the delivery first.
 */
export function formatLeaseToken(laneId: number, epoch: number): string {
  return `${laneId}:${epoch}`;
}

const LEASE_TOKEN_PATTERN = /^(\d{1,3}):(\d{1,15})$/;

/** Splits a lease token; undefined when this store cannot have issued it. */
export function parseLeaseToken(token: string): LaneLease | undefined {
  const match = LEASE_TOKEN_PATTERN.exec(token);
  if (match === null || Number(match[1]) >= LANE_COUNT) {
    return undefined;
  }
  return { laneId: Number(match[1]), epoch: Number(match[2]) };
}
