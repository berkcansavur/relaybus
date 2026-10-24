import { Logger, Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { RelayBus } from '@relaybus/core';
import { RelayBusHandlerExplorer } from './handler-explorer.js';
import {
  RELAYBUS_MODULE_OPTIONS,
  RelayBusConfigurableModule,
  type RelayBusModuleOptions,
} from './module-options.js';
import { RelayBusHealth } from './relay-bus-health.js';
import { RelayBusLifecycle } from './relay-bus-lifecycle.js';
import { RelayBusTransactionHost } from './relay-bus-transaction-host.js';

/**
 * Registers one {@link RelayBus} for the application, discovers `@RelayBusHandler` and `@RelayBusConsumer`
 * handlers in every module, runs `@RelayBusTransaction()` methods on the bus, starts the bus after bootstrap and stops it on
 * shutdown. Global unless `isGlobal: false`.
 *
 * ```ts
 * RelayBusModule.forRoot({ store: new PostgresEventStore({ pool }), source: 'orders' })
 * ```
 */
@Module({
  imports: [DiscoveryModule],
  providers: [
    {
      provide: RelayBus,
      inject: [RELAYBUS_MODULE_OPTIONS],
      useFactory: (options: RelayBusModuleOptions) =>
        new RelayBus({ ...options, logger: new Logger('RelayBus') }),
    },
    RelayBusHandlerExplorer,
    RelayBusLifecycle,
    RelayBusHealth,
    RelayBusTransactionHost,
  ],
  exports: [RelayBus, RelayBusHealth, RelayBusTransactionHost],
})
export class RelayBusModule extends RelayBusConfigurableModule {}
