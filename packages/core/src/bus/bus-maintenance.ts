import type { BusObservation } from '../bus-observation.js';
import type { Clock } from '../clock.js';
import type { EventStore, EventStoreStatistics, PurgeResult } from '../event-store.js';
import { schedule, type CancelTimer } from '../internal/timers.js';
import type { Notifier } from '../notifier.js';

/** How long completed and dead deliveries are kept (null: for ever), and how often that is applied. */
export type RetentionSettings = {
  readonly completedMs: number | null;
  readonly deadMs: number | null;
  readonly intervalMs: number;
};

/** What the background jobs of a running bus need. */
export type BusMaintenanceParts = {
  readonly store: EventStore;
  readonly clock: Clock;
  readonly notifier: Notifier;
  readonly observation: BusObservation | undefined;
  readonly retention: RetentionSettings;
  /** How often the backlog is reported; undefined when nobody wants it. */
  readonly backlogIntervalMs: number | undefined;
  readonly statistics: () => Promise<EventStoreStatistics>;
};

/** The background jobs of a running bus: the retention purge and the backlog report. */
export class BusMaintenance {
  readonly #parts: BusMaintenanceParts;
  #retentionTimer: CancelTimer | undefined;
  #backlogTimer: CancelTimer | undefined;

  constructor(parts: BusMaintenanceParts) {
    this.#parts = parts;
  }

  /** Applies the retention policy once. */
  async purgeExpired(): Promise<PurgeResult> {
    const { store, clock, notifier, retention } = this.#parts;
    const now = clock.now().getTime();
    const { completedMs, deadMs } = retention;
    try {
      const result = await store.purge({
        ...(completedMs === null ? {} : { completedBefore: new Date(now - completedMs) }),
        ...(deadMs === null ? {} : { deadBefore: new Date(now - deadMs) }),
      });
      notifier.notify('onPurged', { result });
      return result;
    } catch (error) {
      notifier.failure('purge', error);
      throw error;
    }
  }

  /** Starts the retention purge and the backlog report, each when it has anything to do. */
  start(): void {
    this.#scheduleRetention();
    this.#scheduleBacklog();
  }

  stop(): void {
    this.#retentionTimer?.();
    this.#backlogTimer?.();
  }

  /** Applies the retention policy every `retention.interval` while the bus runs, when it deletes anything. */
  #scheduleRetention(): void {
    const { clock, retention } = this.#parts;
    if (retention.completedMs !== null || retention.deadMs !== null) {
      this.#retentionTimer = schedule(clock, {
        run: () => {
          this.purgeExpired().catch(() => undefined);
        },
        delayMs: retention.intervalMs,
        repeat: true,
        unref: true,
      });
    }
  }

  /** Reports the backlog every `backlogInterval` while the bus runs, when an observer wants it. */
  #scheduleBacklog(): void {
    const { observation, backlogIntervalMs, statistics } = this.#parts;
    if (backlogIntervalMs !== undefined) {
      this.#backlogTimer = observation?.startBacklog({ intervalMs: backlogIntervalMs, statistics });
    }
  }
}
