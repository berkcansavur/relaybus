import type { CatalogDeclaration } from './catalog-coverage.js';
import type { RouteDefinition } from './catalog-routes.js';
import type { Catalog } from './catalog.js';
import type { Clock } from './clock.js';
import type { Duration } from './duration.js';
import type { EventStore, TransactionIsolationLevel } from './event-store.js';
import type { EventHeaders, EventReference } from './event.js';
import type { IdentifierGenerator } from './identifier.js';
import type { RelayBusLogger, RelayBusObserver } from './observer.js';
import type { RetryPolicyOptions } from './retry-policy.js';
import type { RelayBusTracer } from './tracing.js';
import type { Transport } from './transport.js';

// The options of RelayBus, kept apart to hold relay-bus.ts under the file size limit.

/**
 * - `required`: `publish` throws {@link TransactionRequiredError} when it finds no transaction.
 * - `optional`: `publish` opens its own transaction instead. The event is still durable,
 *   but no longer atomic with business writes made outside that transaction.
 */
export type TransactionMode = 'required' | 'optional';

/** Options of one `bus.transaction` (and `@RelayBusTransaction()` method). */
export interface TransactionOptions {
  /**
   * Runs the transaction at this isolation level; the store must support it, else the call
   * throws a {@link ConfigurationError} (PostgreSQL stores do; MongoDB and the in-memory store
   * do not). Without it the transaction runs exactly as before, at the database default.
   *
   * With `'repeatable read'` or `'serializable'`, a serialization failure (PostgreSQL SQLSTATE
   * `40001`) rolls the transaction back and `work` runs again in a new one, up to
   * {@link TransactionOptions.maxAttempts} times, so **`work` may run more than once**: keep
   * everything but database writes through the transaction (HTTP calls, broker publishes,
   * counters, emails) out of it. Events published by a failed attempt are rolled back with it.
   */
  readonly isolationLevel?: TransactionIsolationLevel;
  /**
   * How many times `work` may run when `isolationLevel` is `'repeatable read'` or `'serializable'`
   * and the transaction fails with a serialization failure; the last failure is rethrown unchanged.
   * Each attempt is held to its own `transactionTimeout`. A positive integer. Default 3.
   */
  readonly maxAttempts?: number;
}

/**
 * What `publish` does with an event that has no handler or route in this
 * process. Deliveries are created from the publisher's registrations, so a
 * process must register every handler and route of the events it publishes.
 * - `warn`: log a warning and store nothing.
 * - `throw`: throw a {@link ConfigurationError}, rolling back the caller's transaction.
 */
export type UnroutedEventPolicy = 'warn' | 'throw';

export interface DispatcherOptions {
  /** Run handlers in this process. Disable on publish-only processes. Default true. */
  readonly enabled?: boolean;
  /** Maximum handlers running at once. Default 10. */
  readonly concurrency?: number;
  /** Maximum deliveries claimed per query. Default equals concurrency. */
  readonly batchSize?: number;
  /** Wait between claims when idle. Retries are picked up on this cadence. Default `'1s'`. */
  readonly pollInterval?: Duration;
  /** Lease length; renewed every third of it while the handler runs. Default `'30s'`. */
  readonly leaseDuration?: Duration;
  /**
   * Longest `stop` takes in all, and its default `timeout`: waiting for running handlers, handing
   * back leases, closing transports and the store. Past it, `stop` aborts the handlers still
   * running, force-closes what relaybus opened itself and resolves. Default `'30s'`.
   */
  readonly shutdownTimeout?: Duration;
  /**
   * Longest a finished delivery waits so that deliveries finishing around the
   * same time are completed with one store call (when the store supports it).
   * A delivery that finishes while nothing else runs is completed at once.
   * `0` completes every delivery on its own. Default `'2ms'`.
   */
  readonly completionBatchWindow?: Duration;
  /** Most deliveries completed in one store call. Default 200. */
  readonly completionBatchSize?: number;
}

export interface RetentionOptions {
  /**
   * Delete completed deliveries older than this. Default `'7d'`; `false` keeps them.
   * Deduplication of received events and of publishes with a caller-supplied id
   * only lasts as long as the event is kept: keep this longer than any broker
   * redelivery or publish retry window.
   */
  readonly completed?: Duration | false;
  /** Delete dead deliveries older than this. Default `false` (keep for inspection). */
  readonly dead?: Duration | false;
  /** How often the cleanup runs. Default `'1h'`. */
  readonly interval?: Duration;
}

/**
 * Every route to one destination of one transport is one handler,
 * `relaybus.transport:<transport>:<destination>`: its events of one key are sent in commit
 * order whatever their types. Such routes must resolve to the same `retry` and `timeout`.
 */
export interface RouteOptions {
  /** Event class or type forwarded to the transport. */
  readonly event: EventReference;
  /** Name of a configured transport. */
  readonly transport: string;
  /** Topic, exchange or queue. Default: the event type. */
  readonly destination?: string;
  /** Default: retry until the broker accepts the event, at most every 5 minutes. */
  readonly retry?: RetryPolicyOptions;
  /** Abort a publish that hangs and retry it. Default `'30s'`. */
  readonly timeout?: Duration;
}

export interface InboundSubscriptionOptions {
  readonly transport: string;
  readonly destinations: readonly string[];
}

export interface RelayBusOptions<TTransaction = unknown> {
  readonly store: EventStore<TTransaction>;
  /** Name of this service, recorded on every event. Default `'relaybus'`. */
  readonly source?: string;
  /** Default `'required'`. */
  readonly transactionMode?: TransactionMode;
  /**
   * Time budget of every `bus.transaction` (and `@RelayBusTransaction()` method). The store makes
   * the database enforce it where it can (PostgreSQL: `statement_timeout`, plus
   * `idle_in_transaction_session_timeout` and on PostgreSQL 17+ `transaction_timeout`; MongoDB:
   * `maxCommitTimeMS`), and a transaction that still runs past
   * it is rolled back with a {@link TransactionTimeoutError}. Keep calls to other services out of
   * transactions instead of raising it. Default `'5s'`.
   */
  readonly transactionTimeout?: Duration;
  /**
   * Finds the caller's active transaction when none is passed to `publish`
   * and none was opened with `bus.transaction`, for example from
   * `@nestjs-cls/transactional`. Return undefined when there is none.
   */
  readonly transactionResolver?: () => TTransaction | undefined;
  /** Default `'warn'`. */
  readonly unroutedEvents?: UnroutedEventPolicy;
  /** Default retry policy of every handler. Routes have their own default. */
  readonly retry?: RetryPolicyOptions;
  /** Default handler timeout. Default `false` (no timeout). */
  readonly handlerTimeout?: Duration | false;
  readonly dispatcher?: DispatcherOptions;
  readonly retention?: RetentionOptions;
  readonly transports?: readonly Transport[];
  /**
   * Events forwarded to brokers after they commit: a route per event class or type, or
   * `route(produce(Catalog, { events }), { transport, destination, ... })` for several events of a
   * catalog with shared settings.
   */
  readonly routes?: readonly RouteDefinition[];
  /** Broker destinations whose events are stored and dispatched to local handlers. */
  readonly subscriptions?: readonly InboundSubscriptionOptions[];
  /** Create tables and indexes on start. Default true. */
  readonly setupStore?: boolean;
  /**
   * Hooks for logging and metrics. The hooks an observer implements are read when the bus is
   * created; the newer ones (`onPublishFinished` and the other timed hooks) are skipped entirely,
   * clock reads included, when no observer implements them.
   */
  readonly observers?: readonly RelayBusObserver[];
  /**
   * Spans around publish, send and handler runs, and the W3C trace context written into published
   * events (`traceparent`, `tracestate` headers). `@relaybus/otel` provides one. Default: none.
   */
  readonly tracer?: RelayBusTracer;
  /**
   * Reads `statistics()` this often while the bus runs and reports the backlog (pending deliveries,
   * age of the oldest due one) to the observers' `onBacklog`. Each reading is a store query, so it
   * is off unless set; `'15s'` to `'1m'` suits metrics scraped every minute. Default: off.
   */
  readonly backlogInterval?: Duration;
  /** Default: `console`. */
  readonly logger?: RelayBusLogger;
  readonly clock?: Clock;
  readonly generateIdentifier?: IdentifierGenerator;
  readonly random?: () => number;
  /**
   * Catalogs this process is checked against when it starts. `start()` throws a
   * {@link ConfigurationError} listing every event that is not covered:
   * - a catalog: every event needs a handler or a route;
   * - `consume(catalog, { events })`: each declared event needs a local handler;
   * - `produce(catalog, { events })`: each declared event needs a route to a transport.
   *
   * See `registerCatalog`.
   */
  readonly catalogs?: readonly (Catalog | CatalogDeclaration)[];
}

export interface PublishOptions<TTransaction = unknown> {
  /** Transaction to write into. Default: the current scope, then `transactionResolver`. */
  readonly transaction?: TTransaction;
  /**
   * Ordering and partition key. Events with the same key are handled one at a
   * time per handler, in commit order. A delayed or retrying event holds back
   * later events of its key. Default: the `key` of the event's definition
   * (`defineEvent(type, { key })`), else none. An explicit key wins over the definition's, and in
   * `publishAll` applies to every event of the call.
   */
  readonly key?: string;
  /** Deliver no earlier than this delay from now. */
  readonly delay?: Duration;
  readonly headers?: EventHeaders;
  /**
   * Default: inherited from the event being handled, otherwise the id of the
   * first published event (shared by every event of a `publishAll` call).
   */
  readonly correlationId?: string;
}

export interface PublishOneOptions<TTransaction = unknown> extends PublishOptions<TTransaction> {
  /**
   * Event id. Pass a deterministic id (for example derived from a business key)
   * to make publishing idempotent: when an event with this id is already
   * stored, nothing is written and the stored event is left unchanged.
   */
  readonly id?: string;
}

export type ReceiveStatus = 'accepted' | 'duplicate' | 'ignored';
