import {
  Inject,
  Injectable,
  type BeforeApplicationShutdown,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { RelayBus } from '@relaybus/core';

/**
 * Starts the bus once every handler is registered and the application has
 * bootstrapped, and stops it (draining running handlers) on shutdown. A start
 * failure rejects the hook, so the application does not boot.
 */
@Injectable()
export class RelayBusLifecycle implements OnApplicationBootstrap, BeforeApplicationShutdown {
  constructor(@Inject(RelayBus) private readonly bus: RelayBus) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.bus.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.bus.stop();
  }
}
