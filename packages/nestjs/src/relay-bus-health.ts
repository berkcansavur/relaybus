import { Inject, Injectable } from '@nestjs/common';
import { RelayBus, systemClock, type EventStoreStatistics } from '@relaybus/core';
import { RELAYBUS_MODULE_OPTIONS, type RelayBusModuleOptions } from './module-options.js';

/** Result of {@link RelayBusHealth.check}. */
export interface RelayBusHealthReport {
  /**
   * `down` when the bus is not running, a transport reports itself not ready
   * (dead Kafka consumer, disconnected RabbitMQ) or the store cannot be queried.
   */
  readonly status: 'up' | 'down';
  readonly running: boolean;
  /** `bus.isReady()`: running and every transport ready. */
  readonly ready: boolean;
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
 * {@link check} in a terminus indicator (see the README). The lag is measured
 * with the `clock` given to the module options (the system clock by default),
 * the same clock the bus uses.
 */
@Injectable()
export class RelayBusHealth {
  constructor(
    @Inject(RelayBus) private readonly bus: RelayBus,
    @Inject(RELAYBUS_MODULE_OPTIONS) private readonly options: RelayBusModuleOptions,
  ) {}

  async check(): Promise<RelayBusHealthReport> {
    const running = this.bus.isRunning;
    const ready = this.bus.isReady();
    const handlers = this.bus.handlers().length;
    if (!running) {
      return { status: 'down', running, ready, handlers };
    }
    try {
      const statistics = await this.bus.statistics();
      const clock = this.options.clock ?? systemClock;
      const lagMilliseconds =
        statistics.oldestDueAt === null
          ? 0
          : Math.max(0, clock.now().getTime() - statistics.oldestDueAt.getTime());
      return {
        status: ready ? 'up' : 'down',
        running,
        ready,
        handlers,
        statistics,
        lagMilliseconds,
      };
    } catch (error) {
      return {
        status: 'down',
        running,
        ready,
        handlers,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}
