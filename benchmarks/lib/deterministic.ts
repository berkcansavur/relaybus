// Everything random that a publisher worker feeds into the database, made reproducible. The gated counters (lane updates and
// WAL bytes per delivery) depend on the lane of every delivery, and a lane is a hash of the event key or id. With random ids two
// databases built from the same plan have different lane layouts, so base and candidate would differ even for identical code.
import { hashSeed, mulberry32 } from './stats.ts';
import type { KeySpec } from './cells.ts';

/** Seed of every publisher worker; changing it changes the layout of every template, so it is part of a baseline's fingerprint. */
export const WORKLOAD_SEED = 20_261_027;

/** Fixed time part of the generated ids (2023-11-14), so ids stay time ordered like production ones but never depend on the clock. */
const ID_EPOCH_MS = 1_700_000_000_000;
const TIMESTAMP_BYTES = 6;
const BYTE_VALUES = 256;
const UUID_BYTES = 16;

/** The stream of a worker is a function of the seed and the worker number only. */
const streamSeed = (seed: number, worker: number, stream: string): number =>
  (seed ^ hashSeed(`${stream}/${worker}`)) >>> 0;

/**
 * Identifiers shaped like RFC 9562 version 7 UUIDs: a counter in the 48 bit time part and seeded random bits after it.
 * The n-th identifier of a worker is the same in every run, whichever code is loaded.
 */
export function deterministicIdentifiers(seed: number, worker: number): () => string {
  const random = mulberry32(streamSeed(seed, worker, 'id'));
  let counter = 0;
  return () => {
    const bytes = Buffer.alloc(UUID_BYTES);
    for (let index = TIMESTAMP_BYTES; index < UUID_BYTES; index += 1)
      bytes[index] = Math.floor(random() * BYTE_VALUES);
    let remaining = ID_EPOCH_MS + counter;
    counter += 1;
    for (let index = TIMESTAMP_BYTES - 1; index >= 0; index -= 1) {
      bytes[index] = remaining % BYTE_VALUES;
      remaining = Math.floor(remaining / BYTE_VALUES);
    }
    bytes[6] = (bytes[6]! & 0x0f) | 0x70;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    return bytes.toString('hex').replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, '$1-$2-$3-$4-$5');
  };
}

/** Picks the ordering key of the next transaction; `undefined` when the cell publishes without keys. */
export function deterministicKeys(
  seed: number,
  worker: number,
  keys: KeySpec,
): () => string | undefined {
  if (keys.mode === 'none') return () => undefined;
  const random = mulberry32(streamSeed(seed, worker, 'key'));
  return () => `k${Math.floor(random() * keys.count)}`;
}
