import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { ConfigurationError, RelayBus, type EventHandler } from '@relaybus/core';
import { readSubscribeMetadata } from './subscribe.decorator.js';

/**
 * Finds every `@Subscribe` method of every singleton provider in the
 * application and registers it on the bus before the bus starts.
 */
@Injectable()
export class RelayBusHandlerExplorer implements OnModuleInit {
  constructor(
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Inject(MetadataScanner) private readonly metadataScanner: MetadataScanner,
    @Inject(RelayBus) private readonly bus: RelayBus,
  ) {}

  onModuleInit(): void {
    const registeredBy = new Map<string, string>();
    for (const wrapper of this.discovery.getProviders()) {
      const instance = wrapper.instance as unknown;
      if (typeof instance !== 'object' || instance === null) {
        continue;
      }
      const prototype = Object.getPrototypeOf(instance) as object | null;
      if (prototype === null) {
        continue;
      }
      const className = instance.constructor.name;
      for (const methodName of this.metadataScanner.getAllMethodNames(prototype)) {
        const method = (instance as Record<string, unknown>)[methodName] as (
          ...args: Parameters<EventHandler>
        ) => unknown;
        for (const { event, options } of readSubscribeMetadata(method)) {
          const location = `${className}.${methodName}`;
          assertSingleton(wrapper, location);
          const id = options.id ?? location;
          const previous = registeredBy.get(id);
          if (previous !== undefined) {
            throw new ConfigurationError(
              `Handler id '${id}' is used by both ${previous} and ${location}; ` +
                `pass a unique { id } to @Subscribe`,
            );
          }
          registeredBy.set(id, location);
          // Core resolves both classes and type strings; the overloads cannot express the union.
          this.bus.subscribe(
            event as never,
            (envelope, context) => method.call(instance, envelope, context),
            { ...options, id },
          );
        }
      }
    }
  }
}

function assertSingleton(wrapper: InstanceWrapper, location: string): void {
  if (wrapper.isTransient || !wrapper.isDependencyTreeStatic()) {
    throw new ConfigurationError(
      `${location} uses @Subscribe but its provider is request-scoped or transient ` +
        `(directly or through a dependency); handlers must be singleton providers`,
    );
  }
}
