import { createUuidV7, type EventEnvelope, type NewDelivery } from '@relaybus/core';

// The fixed clock, events and deliveries of the store contract suite.

export interface Deferred {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
}

export function createDeferred(): Deferred {
  let resolve!: () => void;
  const promise = new Promise<void>((onResolve) => {
    resolve = onResolve;
  });
  return { promise, resolve };
}

const BASE_TIME = Date.parse('2026-01-01T00:00:00.000Z');
export const LEASE_MS = 30_000;

export function at(offsetMs: number): Date {
  return new Date(BASE_TIME + offsetMs);
}

export function createEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: createUuidV7(),
    type: 'contract.event.v1',
    source: 'contract-suite',
    key: null,
    payload: { value: 1 },
    headers: {},
    correlationId: null,
    causationId: null,
    occurredAt: at(0),
    ...overrides,
  };
}

export function createDelivery(handler: string, availableAt: Date = at(0)): NewDelivery {
  return { id: createUuidV7(), handler, availableAt };
}
