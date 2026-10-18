import { randomBytes } from 'node:crypto';

/**
 * RFC 9562 UUID version 7: 48-bit Unix millisecond timestamp followed by random
 * bits. Time-ordered, so it keeps B-tree indexes compact, and globally unique,
 * so it is safe as an idempotency key across services.
 */
export function createUuidV7(timestamp: number = Date.now()): string {
  const bytes = randomBytes(16);
  let remaining = timestamp;
  for (let index = 5; index >= 0; index -= 1) {
    bytes[index] = remaining % 256;
    remaining = Math.floor(remaining / 256);
  }
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Produces unique identifiers for events, deliveries and lease tokens. */
export type IdentifierGenerator = () => string;
