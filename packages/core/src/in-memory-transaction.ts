import { ConfigurationError } from './errors.js';

/** Transaction handle of {@link InMemoryEventStore}. Writes apply on commit only. */
export class InMemoryTransaction {
  readonly #operations: Array<() => void> = [];
  readonly #finishers: Array<() => void> = [];
  #state: 'open' | 'committed' | 'rolled-back' = 'open';

  get state(): 'open' | 'committed' | 'rolled-back' {
    return this.#state;
  }

  enqueue(operation: () => void): void {
    if (this.#state !== 'open') {
      throw new ConfigurationError(`Cannot write through a ${this.#state} in-memory transaction`);
    }
    this.#operations.push(operation);
  }

  /** Runs `finisher` once the transaction commits or rolls back, like a lock release. */
  onFinish(finisher: () => void): void {
    this.#finishers.push(finisher);
  }

  commit(): void {
    this.#state = 'committed';
    for (const operation of this.#operations) {
      operation();
    }
    this.#finish();
  }

  rollback(): void {
    this.#state = 'rolled-back';
    this.#finish();
  }

  #finish(): void {
    for (const finisher of this.#finishers.splice(0)) {
      finisher();
    }
  }
}
