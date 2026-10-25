import { randomBytes } from 'node:crypto';

const UUID_BYTES = 16;
/** The 48-bit millisecond timestamp fills the first six bytes. */
const TIMESTAMP_BYTES = 6;
const BYTE_VALUES = 256;
const LOW_NIBBLE = 0x0f;
/** Version 7 in the high nibble of byte 6. */
const VERSION_7 = 0x70;
const VARIANT_PAYLOAD = 0x3f;
/** The `10` variant bits in the high bits of byte 8. */
const RFC_9562_VARIANT = 0x80;

/**
 * RFC 9562 UUID version 7: 48-bit Unix millisecond timestamp followed by random
 * bits. Time-ordered, so it keeps B-tree indexes compact, and globally unique,
 * so it is safe as an idempotency key across services.
 */
export function createUuidV7(timestamp: number = Date.now()): string {
  const bytes = randomBytes(UUID_BYTES);
  let remaining = timestamp;
  for (let index = TIMESTAMP_BYTES - 1; index >= 0; index -= 1) {
    bytes[index] = remaining % BYTE_VALUES;
    remaining = Math.floor(remaining / BYTE_VALUES);
  }
  bytes[6] = (bytes[6]! & LOW_NIBBLE) | VERSION_7;
  bytes[8] = (bytes[8]! & VARIANT_PAYLOAD) | RFC_9562_VARIANT;
  // 8-4-4-4-12 hex digits.
  return bytes.toString('hex').replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, '$1-$2-$3-$4-$5');
}

/** Produces unique identifiers for events, deliveries and lease tokens. */
export type IdentifierGenerator = () => string;
