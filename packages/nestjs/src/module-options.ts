import { ConfigurableModuleBuilder } from '@nestjs/common';
import type { RelayBusOptions } from '@relaybus/core';

/**
 * Options of {@link RelayBusModule}: everything {@link RelayBusOptions} accepts
 * except `logger`, which is always a Nest `Logger` with context `RelayBus`
 * (configure it with `app.useLogger`). Only `store` is required.
 */
export type RelayBusModuleOptions<TTransaction = unknown> = Omit<
  RelayBusOptions<TTransaction>,
  'logger'
>;

/** Module-level extras accepted next to the options by `forRoot` and `forRootAsync`. */
export interface RelayBusModuleExtras {
  /** Make `RelayBus` and `RelayBusHealth` injectable everywhere. Default true. */
  readonly isGlobal?: boolean;
}

/** Implemented by classes passed as `useClass` / `useExisting` to `forRootAsync`. */
export interface RelayBusModuleOptionsFactory {
  create(): Promise<RelayBusModuleOptions> | RelayBusModuleOptions;
}

export const {
  ConfigurableModuleClass: RelayBusConfigurableModule,
  MODULE_OPTIONS_TOKEN: RELAYBUS_MODULE_OPTIONS,
} = new ConfigurableModuleBuilder<RelayBusModuleOptions>()
  .setClassMethodName('forRoot')
  .setExtras<RelayBusModuleExtras>({ isGlobal: true }, (definition, extras) => ({
    ...definition,
    global: extras.isGlobal === true,
  }))
  .build();
