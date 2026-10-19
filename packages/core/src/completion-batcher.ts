import type { Clock } from './clock.js';
import type { EventStore, LeaseReference } from './event-store.js';

interface PendingCompletion {
  readonly lease: LeaseReference;
  readonly resolve: (completed: boolean) => void;
  readonly reject: (error: unknown) => void;
}

export interface CompletionBatcherSettings {
  /** Longest a completion waits for company before it is written alone. */
  readonly windowMs: number;
  /** A batch is written as soon as it holds this many completions. */
  readonly maxBatchSize: number;
}

/**
 * Groups the completions of deliveries that finish close together into one
 * `completeMany` call. A completion is written at once when no other delivery
 * is still running (nothing to wait for), when the batch is full, or when the
 * window ends, so a lone delivery never pays the window and a busy dispatcher
 * writes one statement for many deliveries.
 *
 * Every lease is fenced by the store individually: the result of each
 * submission says whether that lease was still held.
 */
export class CompletionBatcher {
  readonly #store: Required<Pick<EventStore, 'completeMany'>>;
  readonly #clock: Clock;
  readonly #settings: CompletionBatcherSettings;
  readonly #runningCount: () => number;
  #pending: PendingCompletion[] = [];
  #timer: NodeJS.Timeout | undefined;
  #closing = false;

  /**
   * @param runningCount Number of deliveries the dispatcher is running right
   * now, including the ones waiting in this batcher.
   */
  constructor(
    store: Required<Pick<EventStore, 'completeMany'>>,
    clock: Clock,
    settings: CompletionBatcherSettings,
    runningCount: () => number,
  ) {
    this.#store = store;
    this.#clock = clock;
    this.#settings = settings;
    this.#runningCount = runningCount;
  }

  /**
   * Queues a completion. Resolves with whether the lease was still held;
   * rejects with the store error when the batch could not be written.
   */
  submit(lease: LeaseReference): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this.#pending.push({ lease, resolve, reject });
      if (
        this.#closing ||
        this.#pending.length >= this.#settings.maxBatchSize ||
        this.#pending.length >= this.#runningCount()
      ) {
        this.#flush();
      } else {
        this.#timer ??= setTimeout(() => this.#flush(), this.#settings.windowMs);
      }
    });
  }

  /**
   * Shutdown has begun (or ended): while closing, nothing waits for company any more, so the completions
   * already queued are written now and later ones at once.
   */
  setClosing(closing: boolean): void {
    this.#closing = closing;
    if (closing && this.#pending.length > 0) {
      this.#flush();
    }
  }

  #flush(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    const batch = this.#pending;
    this.#pending = [];
    Promise.resolve()
      .then(() =>
        this.#store.completeMany(
          batch.map((entry) => entry.lease),
          this.#clock.now(),
        ),
      )
      .then(
        (outcomes) => batch.forEach((entry, index) => entry.resolve(outcomes[index]!)),
        (error: unknown) => batch.forEach((entry) => entry.reject(error)),
      );
  }
}
