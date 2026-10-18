import type {
  ClaimedDelivery,
  DeliveryRecord,
  DeliveryStatus,
  ErrorInfo,
  EventEnvelope,
  EventHeaders,
} from '@relaybus/core';
import type { PostgresRow } from './executor.js';

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

export function mapClaimedDelivery(row: PostgresRow, leaseExpiresAt: Date): ClaimedDelivery {
  return {
    deliveryId: text(row, 'delivery_id'),
    leaseToken: text(row, 'lease_token'),
    handler: text(row, 'handler'),
    attempt: readCount(row, 'attempts'),
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
  return rows.length === 0 ? null : new Date(text(rows[0]!, 'oldest_due_at'));
}
