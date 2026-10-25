import type { ErrorInfo } from './errors.js';
import type { EventEnvelope } from './event.js';
import type { ClaimedDelivery, EventStoreStatistics, PurgeResult } from './event-store.js';
import type { TraceContext } from './tracing.js';

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
  | 'watch'
  | 'backlog'
  | 'observer';

/** How a timed operation ended. On `error` the event carries the error. */
export type OperationOutcome = 'success' | 'error';

/** Whether a delivery runs a local handler or forwards its event to a transport (a route). */
export type DeliveryKind = 'handler' | 'route';

/** A `publish` or `publishAll` call returned or threw. */
export interface PublishFinishedEvent {
  /** The type of every event of the call, in order. */
  readonly eventTypes: readonly string[];
  readonly durationMs: number;
  readonly outcome: OperationOutcome;
  readonly error?: ErrorInfo;
}

/** One claim query of the dispatcher answered or failed. */
export interface ClaimFinishedEvent {
  /** Handler ids the claim asked for. */
  readonly handlers: readonly string[];
  /** Most deliveries the claim asked for (the batch size of this query). */
  readonly requested: number;
  readonly claimed: number;
  /** Distinct leases among the claimed deliveries: a store that leases lanes counts each lane once. */
  readonly lanes: number;
  readonly durationMs: number;
  readonly outcome: OperationOutcome;
  readonly error?: ErrorInfo;
}

/** A handler (or a route's send) is about to run for a claimed delivery. */
export interface HandlerStartedEvent {
  readonly delivery: ClaimedDelivery;
  readonly kind: DeliveryKind;
  /** From the event's `occurredAt` to this start, across services for a received event. */
  readonly endToEndLatencyMs: number;
}

/**
 * A handler run ended: it resolved (`success`) or threw, timed out, lost its lease or was
 * stopped (`error`). What happens to the delivery next is reported by the delivery hooks.
 */
export interface HandlerFinishedEvent {
  readonly delivery: ClaimedDelivery;
  readonly kind: DeliveryKind;
  readonly durationMs: number;
  readonly outcome: OperationOutcome;
  readonly error?: ErrorInfo;
}

/** A route handed an event to its transport, which acknowledged it or failed. */
export interface TransportSentEvent {
  readonly envelope: EventEnvelope;
  /** Name of the transport. */
  readonly transport: string;
  /** The messaging system of the transport (`kafka`, `rabbitmq`), else its name. */
  readonly system: string;
  readonly destination: string;
  readonly durationMs: number;
  readonly outcome: OperationOutcome;
  readonly error?: ErrorInfo;
}

/**
 * What `receive` did with an event from a broker: stored it (`accepted`), found it stored already
 * (`duplicate`), had no handler for it (`ignored`), rejected it as malformed (`invalid`) or failed
 * (`error`, the broker redelivers it).
 */
export type ReceiveOutcome = 'accepted' | 'duplicate' | 'ignored' | 'invalid' | 'error';

/** One event from a broker went through `receive`. */
export interface ReceiveFinishedEvent {
  /** Name of the transport that delivered it; undefined when `bus.receive` was called directly. */
  readonly transport: string | undefined;
  /** Messaging system of that transport (`kafka`, `rabbitmq`), else its name. */
  readonly system: string | undefined;
  /** Topic, routing key or queue it arrived on, when the transport tells. */
  readonly destination: string | undefined;
  /** The event's type and source as received; undefined when missing or not text. */
  readonly eventType: string | undefined;
  readonly source: string | undefined;
  /** W3C trace context the event carries, to continue the trace of its publisher. */
  readonly traceContext: TraceContext | undefined;
  readonly outcome: ReceiveOutcome;
  readonly durationMs: number;
  readonly error?: ErrorInfo;
}

/** A periodic reading of the store's backlog (`RelayBusOptions.backlogInterval`). */
export interface BacklogEvent {
  /** Deliveries waiting to run, due or not. */
  readonly pending: number;
  /** How long the oldest due delivery has waited; 0 when none is due. */
  readonly oldestPendingAgeMs: number;
  readonly statistics: EventStoreStatistics;
}

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
  /** A publish call ended; called before the publishing transaction commits. */
  onPublishFinished?(event: PublishFinishedEvent): void;
  onClaimFinished?(event: ClaimFinishedEvent): void;
  onHandlerStarted?(event: HandlerStartedEvent): void;
  onHandlerFinished?(event: HandlerFinishedEvent): void;
  onTransportSent?(event: TransportSentEvent): void;
  onReceiveFinished?(event: ReceiveFinishedEvent): void;
  /** Called every `backlogInterval` while the bus runs, when that option is set. */
  onBacklog?(event: BacklogEvent): void;
}

/** Minimal logger. Nest's `Logger` and `console` both satisfy it. */
export interface RelayBusLogger {
  log(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}
