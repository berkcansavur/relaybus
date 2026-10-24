import {
  Inject,
  Injectable,
  type BeforeApplicationShutdown,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import { RelayBus } from '@relaybus/core';
import { registerBus, unregisterBus } from './relay-bus-registry.js';

/**
 * Starts the bus once every handler is registered and the application has
 * bootstrapped, and stops it (draining running handlers) on shutdown. A start
 * failure rejects the hook, so the application does not boot. `@RelayBusTransaction()`
 * methods find the bus from module initialisation until the application has shut down.
 */
@Injectable()
export class RelayBusLifecycle
  implements OnModuleInit, OnApplicationBootstrap, BeforeApplicationShutdown, OnApplicationShutdown
{
  constructor(@Inject(RelayBus) private readonly bus: RelayBus) {}

  onModuleInit(): void {
    registerBus(this.bus);
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.bus.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.bus.stop();
  }

  /** After the bus stopped, so handlers still draining at shutdown can open transactions. */
  onApplicationShutdown(): void {
    unregisterBus(this.bus);
  }
}
