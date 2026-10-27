import { AsyncLocalStorage } from 'node:async_hooks';
import type { TransactionIsolationLevel } from './event-store.js';
import type { EventEnvelope } from './event.js';

export interface ExecutionScope {
  /** Transaction that `publish` joins when none is passed explicitly. */
  readonly transaction?: unknown;
  /**
   * Set by `bus.transaction({ isolationLevel })` for the attempt it runs: the transaction, its level,
   * and what `publish` calls inside it announce (observers, dispatcher wake-up) once the attempt
   * commits, since an attempt may be rolled back and rerun.
   */
  readonly isolation?: {
    readonly transaction: unknown;
    readonly level: TransactionIsolationLevel;
    readonly afterCommit: Array<() => void>;
  };
  /** Event being handled. Events published while handling it inherit correlation. */
  readonly event?: EventEnvelope;
}

const storage = new AsyncLocalStorage<ExecutionScope>();

/** Runs `work` in `scope`, layered over the caller's scope. */
export function runInScope<T>(scope: ExecutionScope, work: () => T): T {
  return storage.run({ ...storage.getStore(), ...scope }, work);
}

/** Runs `work` in exactly `scope`, ignoring the caller's scope. */
export function runInIsolatedScope<T>(scope: ExecutionScope, work: () => T): T {
  return storage.run(scope, work);
}

export function getCurrentScope(): ExecutionScope | undefined {
  return storage.getStore();
}

/**
 * The isolation level of the transaction `publish` would join, when `bus.transaction` opened it with
 * one; absent when the scope's transaction is another one (a nested `bus.transaction` without a level).
 */
export function getCurrentIsolationLevel(): TransactionIsolationLevel | undefined {
  const scope = storage.getStore();
  return scope?.isolation !== undefined && scope.isolation.transaction === scope.transaction
    ? scope.isolation.level
    : undefined;
}
