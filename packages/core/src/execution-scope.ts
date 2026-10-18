import { AsyncLocalStorage } from 'node:async_hooks';
import type { EventEnvelope } from './event.js';

export interface ExecutionScope {
  /** Transaction that `publish` joins when none is passed explicitly. */
  readonly transaction?: unknown;
  /** Event being handled. Events published while handling it inherit correlation. */
  readonly event?: EventEnvelope;
}

const storage = new AsyncLocalStorage<ExecutionScope>();

export function runInScope<T>(scope: ExecutionScope, work: () => T): T {
  return storage.run({ ...storage.getStore(), ...scope }, work);
}

export function getCurrentScope(): ExecutionScope | undefined {
  return storage.getStore();
}
