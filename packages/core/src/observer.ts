import type { ErrorInfo } from './errors.js';
import type { EventEnvelope } from './event.js';
import type { ClaimedDelivery, PurgeResult } from './event-store.js';

export type FailureStage =
  | 'claim'
  | 'dispatch'
  | 'complete'
  | 'reschedule'
  | 'bury'
  | 'release'
  | 'relinquish'
  | 'extend-lease'
  | 'purge'
  | 'observer';

/**
 * Hooks for logging, metrics and tracing. Every method is optional, runs
 * synchronously on the hot path and must not throw; errors it throws are
 * reported to `onFailure` with stage `observer` and otherwise ignored.
 */
export interface RelayBusObserver {
  /**
   * Events were written into the publishing transaction. Called before that
   * transaction commits, so it can still roll back. Duplicates are not reported.
   */
  onPublished?(event: {
    readonly envelopes: readonly EventEnvelope[];
    readonly deliveries: number;
  }): void;
  onReceived?(event: {
    readonly envelope: EventEnvelope;
    readonly status: 'accepted' | 'duplicate' | 'ignored';
  }): void;
  /** A delivery was given back without using an attempt, for example at shutdown. */
  onDeliveryReleased?(event: { readonly delivery: ClaimedDelivery }): void;
  onDeliveryStarted?(event: { readonly delivery: ClaimedDelivery }): void;
  onDeliverySucceeded?(event: {
    readonly delivery: ClaimedDelivery;
    readonly durationMs: number;
  }): void;
  onDeliveryRetryScheduled?(event: {
    readonly delivery: ClaimedDelivery;
    readonly error: ErrorInfo;
    readonly availableAt: Date;
  }): void;
  onDeliveryDead?(event: { readonly delivery: ClaimedDelivery; readonly error: ErrorInfo }): void;
  onLeaseLost?(event: { readonly delivery: ClaimedDelivery }): void;
  onPurged?(event: { readonly result: PurgeResult }): void;
  onFailure?(event: { readonly stage: FailureStage; readonly error: ErrorInfo }): void;
}

/** Minimal logger. Nest's `Logger` and `console` both satisfy it. */
export interface RelayBusLogger {
  log(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}
