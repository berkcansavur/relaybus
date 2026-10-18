import { Inject, Injectable } from '@nestjs/common';
import { RelayBus, type EventStoreStatistics } from '@relaybus/core';

/** Result of {@link RelayBusHealth.check}. */
export interface RelayBusHealthReport {
  /** `down` when the bus is not running or the store cannot be queried. */
  readonly status: 'up' | 'down';
  readonly running: boolean;
  /** Number of registered handlers and routes. */
  readonly handlers: number;
  /** Delivery counts; absent when the bus is not running or the store failed. */
  readonly statistics?: EventStoreStatistics;
  /** Milliseconds the oldest due delivery has waited; 0 when nothing is due. */
  readonly lagMilliseconds?: number;
  /** Message of the store error, when the statistics query failed. */
  readonly error?: string;
}

/**
 * Health probe of the bus without a dependency on `@nestjs/terminus`. Wrap
 * {@link check} in a terminus indicator (see the README).
 */
@Injectable()
export class RelayBusHealth {
  constructor(@Inject(RelayBus) private readonly bus: RelayBus) {}

  async check(): Promise<RelayBusHealthReport> {
    const running = this.bus.isRunning;
    const handlers = this.bus.handlers().length;
    if (!running) {
      return { status: 'down', running, handlers };
    }
    try {
      const statistics = await this.bus.statistics();
      const lagMilliseconds =
        statistics.oldestDueAt === null
          ? 0
          : Math.max(0, Date.now() - statistics.oldestDueAt.getTime());
      return { status: 'up', running, handlers, statistics, lagMilliseconds };
    } catch (error) {
      return {
        status: 'down',
        running,
        handlers,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}
