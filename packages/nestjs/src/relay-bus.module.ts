import { Logger, Module, type DynamicModule } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { RelayBus } from '@relaybus/core';
import { RelayBusHandlerExplorer } from './handler-explorer.js';
import {
  RELAYBUS_MODULE_OPTIONS,
  RelayBusConfigurableModule,
  type RelayBusModuleOptions,
} from './module-options.js';
import {
  RelayBusFeatureContract,
  type RelayBusFeatureOptions,
} from './relay-bus-feature-contract.js';
import { RelayBusFeatureModule } from './relay-bus-feature.module.js';
import { RelayBusHealth } from './relay-bus-health.js';
import { RelayBusLifecycle } from './relay-bus-lifecycle.js';
import { RelayBusTransactionHost } from './relay-bus-transaction-host.js';

/**
 * Registers one {@link RelayBus} for the application, discovers `@RelayBusConsumer` and `@RelayBusCatalogConsumer`
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
export class RelayBusModule extends RelayBusConfigurableModule {
  /**
   * The relaybus contract of a feature module: import it in the module whose providers consume
   * and produce the events. When the application initialises, every consumed event needs a
   * `@RelayBusConsumer` method (or a `@RelayBusCatalogConsumer` class) among the providers of
   * that module, spread over any classes and method names, and every produced event needs a
   * route to a transport; otherwise the application fails to start naming the module and each
   * missing event. Needs `RelayBusModule.forRoot` in the application.
   *
   * ```ts
   * @Module({
   *   imports: [RelayBusModule.forFeature({ consumes: [OrderEvents], produces: [produce(BillingEvents)] })],
   *   providers: [OrderPlacedHandlers, OrderCancelledHandlers],
   * })
   * export class BillingModule {}
   * ```
   *
   * @throws ConfigurationError for a `produce(...)` in `consumes` or a `consume(...)` in `produces`.
   */
  static forFeature(options: RelayBusFeatureOptions): DynamicModule {
    return {
      module: RelayBusFeatureModule,
      providers: [
        { provide: RelayBusFeatureContract, useValue: new RelayBusFeatureContract(options) },
      ],
    };
  }
}
