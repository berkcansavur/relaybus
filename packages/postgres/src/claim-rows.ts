import { CompositeKey } from '@relaybus/core/internal';
import type { PostgresRow } from './executor.js';

// Reading the rows of the claim statements: what they left behind, and their order.

/** Whether the claim statement saw work it could not take: a takeover in progress, unexamined free lanes, or a stale snapshot. */
export function leftWorkBehind(row: PostgresRow): boolean {
  return (
    row['takeover_waiting'] === true ||
    row['unexamined_free_lanes'] === true ||
    row['stale'] === true
  );
}

/**
 * The first `limit` picks in due order, keeping only the lowest sequence of each (handler, key):
 * two statements that read at different moments may each have picked a head of the same key.
 */
export function keepKeyHeads(picked: readonly PostgresRow[], limit: number): PostgresRow[] {
  const seenKeys = new Set<string>();
  const kept = [...picked]
    .sort((left, right) => compareSequence(left, right))
    .filter((row) => {
      if (row['delivery_key'] === null) {
        return true;
      }
      const group = CompositeKey.of(row['handler'] as string, row['delivery_key'] as string);
      return !seenKeys.has(group) && Boolean(seenKeys.add(group));
    });
  kept.sort(compareDue);
  return kept.slice(0, limit);
}

function compareSequence(left: PostgresRow, right: PostgresRow): number {
  const first = sequenceOf(left);
  const second = sequenceOf(right);
  return Number(first > second) - Number(first < second);
}

/** The key sequence of a picked row; 0 for an unkeyed delivery, which has none. */
function sequenceOf(row: PostgresRow): bigint {
  return BigInt((row['sequence'] as string | null) ?? 0);
}

export function compareDue(left: PostgresRow, right: PostgresRow): number {
  return (
    compareText(left['due_at'] as string, right['due_at'] as string) ||
    compareText(left['delivery_id'] as string, right['delivery_id'] as string)
  );
}

function compareText(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}
