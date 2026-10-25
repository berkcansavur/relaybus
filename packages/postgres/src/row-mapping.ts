import type {
  ClaimedDelivery,
  DeliveryRecord,
  DeliveryStatus,
  ErrorInfo,
  EventEnvelope,
  EventHeaders,
} from '@relaybus/core';
import type { PostgresRow } from './executor.js';
import { LANE_COUNT } from './names.js';

// Rows come from statements written in this package, so their shape is known.
// Casting here is the single place where driver output is trusted.

function text(row: PostgresRow, column: string): string {
  return row[column] as string;
}

function nullableText(row: PostgresRow, column: string): string | null {
  return row[column] as string | null;
}

function nullableDate(row: PostgresRow, column: string): Date | null {
  const value = nullableText(row, column);
  return value === null ? null : new Date(value);
}

/** Count columns arrive as number, string or bigint depending on the driver. */
export function readCount(row: PostgresRow, column: string): number {
  return Number(row[column]);
}

export function mapEventEnvelope(row: PostgresRow): EventEnvelope {
  return {
    id: text(row, 'event_id'),
    type: text(row, 'event_type'),
    source: text(row, 'event_source'),
    key: nullableText(row, 'event_key'),
    payload: JSON.parse(text(row, 'event_payload')) as unknown,
    headers: JSON.parse(text(row, 'event_headers')) as EventHeaders,
    correlationId: nullableText(row, 'event_correlation_id'),
    causationId: nullableText(row, 'event_causation_id'),
    occurredAt: new Date(text(row, 'event_occurred_at')),
  };
}

/**
 * The fencing token of a claimed delivery names its lane and the lane's epoch,
 * so every state change can check the lane without reading the delivery first.
 */
export function formatLeaseToken(
  lane: number,
  { epoch, claims }: { readonly epoch: string; readonly claims: number },
): string {
  return `${lane}:${epoch}:${claims}`;
}

export interface LaneLease {
  readonly lane: number;
  readonly epoch: string;
  /** How many claims of this delivery ended before this one; every end of a claim bumps it, so an old lease can never match a newer claim. */
  readonly claims: number;
}

const LEASE_TOKEN_PATTERN = /^(\d{1,3}):(\d{1,18}):(\d{1,9})$/;

/** Splits a lease token; undefined when it was not issued by this store. */
export function parseLeaseToken(token: string): LaneLease | undefined {
  const match = LEASE_TOKEN_PATTERN.exec(token);
  if (match === null || Number(match[1]) >= LANE_COUNT) {
    return undefined;
  }
  return { lane: Number(match[1]), epoch: match[2]!, claims: Number(match[3]) };
}

export function mapClaimedDelivery(row: PostgresRow, leaseExpiresAt: Date): ClaimedDelivery {
  return {
    deliveryId: text(row, 'delivery_id'),
    leaseToken: formatLeaseToken(readCount(row, 'lane'), {
      epoch: text(row, 'epoch'),
      claims: readCount(row, 'claims'),
    }),
    handler: text(row, 'handler'),
    attempt: readCount(row, 'attempt'),
    leaseExpiresAt,
    event: mapEventEnvelope(row),
  };
}

export function mapDeliveryRecord(row: PostgresRow): DeliveryRecord {
  const lastError = nullableText(row, 'last_error');
  return {
    id: text(row, 'delivery_id'),
    handler: text(row, 'handler'),
    status: text(row, 'status') as DeliveryStatus,
    attempts: readCount(row, 'attempts'),
    availableAt: new Date(text(row, 'available_at')),
    leaseOwner: nullableText(row, 'lease_owner'),
    leaseExpiresAt: nullableDate(row, 'lease_expires_at'),
    lastError: lastError === null ? null : (JSON.parse(lastError) as ErrorInfo),
    createdAt: new Date(text(row, 'created_at')),
    updatedAt: new Date(text(row, 'updated_at')),
    completedAt: nullableDate(row, 'completed_at'),
    event: mapEventEnvelope(row),
  };
}

export function mapOldestDueAt(rows: readonly PostgresRow[]): Date | null {
  return nullableDate(rows[0]!, 'oldest_due_at');
}
