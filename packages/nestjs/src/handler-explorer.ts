import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { ConfigurationError, RelayBus, describeCatalog, type EventHandler } from '@relaybus/core';
import { readEventConsumerMetadata } from './event-consumer.decorator.js';
import { readSubscribeMetadata, type SubscribeMetadata } from './subscribe.decorator.js';

interface SubscribedMethod {
  readonly methodName: string;
  readonly declarations: readonly SubscribeMetadata[];
}

/**
 * Finds every `@Subscribe` method and every `@EventConsumer` class among the
 * singleton providers of the application and registers them on the bus before
 * the bus starts. Providers that are request-scoped or transient, directly or
 * through a dependency, and use either decorator fail the boot. A request-scoped provider whose
 * token is neither a class nor an instance carrying the decorated prototype
 * (for example a string-token factory) cannot be inspected and is skipped.
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
    const visited = new Map<object, Set<string>>();
    const consumers = new WeakSet<object>();
    for (const wrapper of this.discovery.getProviders()) {
      if (wrapper.isTransient || !wrapper.isDependencyTreeStatic()) {
        this.assertNoHandlers(wrapper);
        continue;
      }
      const instance = wrapper.instance as unknown;
      if (typeof instance !== 'object' || instance === null) {
        continue;
      }
      const prototype = Object.getPrototypeOf(instance) as object | null;
      if (prototype === null) {
        continue;
      }
      const className = instance.constructor.name;
      for (const { methodName, declarations } of this.subscribedMethods(prototype)) {
        // A `useExisting` alias exposes the same instance under another token.
        const seen = visited.get(instance) ?? new Set<string>();
        visited.set(instance, seen);
        if (seen.has(methodName)) {
          continue;
        }
        seen.add(methodName);
        const method = (instance as Record<string, unknown>)[methodName] as (
          ...args: Parameters<EventHandler>
        ) => unknown;
        const location = `${className}.${methodName}`;
        for (const { event, options } of declarations) {
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
      if (!consumers.has(instance)) {
        consumers.add(instance);
        this.registerEventConsumers(instance, className, registeredBy);
      }
    }
  }

  private registerEventConsumers(
    instance: object,
    className: string,
    registeredBy: Map<string, string>,
  ): void {
    for (const { catalog, options } of readEventConsumerMetadata(instance.constructor)) {
      const { name, entries } = describeCatalog(catalog);
      const label = name ?? 'catalog';
      const handlers = entries.filter(
        (entry) => typeof (instance as Record<string, unknown>)[entry.handlerName] === 'function',
      );
      if (handlers.length === 0) {
        throw new ConfigurationError(
          `${className} uses @EventConsumer(${label}) but defines none of its handler ` +
            `methods (${entries.map((entry) => entry.handlerName).join(', ')})`,
        );
      }
      for (const entry of handlers) {
        const id = `${className}.${entry.handlerName}`;
        const previous = registeredBy.get(id);
        if (previous !== undefined) {
          throw new ConfigurationError(
            `Handler id '${id}' of @EventConsumer(${label}) is already used by ${previous}; ` +
              `give the @Subscribe a unique { id } or drop it`,
          );
        }
        registeredBy.set(id, `${className}.${entry.handlerName}`);
        const method = (instance as Record<string, unknown>)[entry.handlerName] as (
          ...args: Parameters<EventHandler>
        ) => unknown;
        this.bus.subscribe(
          entry.eventClass as never,
          (envelope, context) => method.call(instance, envelope, context),
          { ...options, id },
        );
      }
    }
  }

  private subscribedMethods(prototype: object): SubscribedMethod[] {
    const methods: SubscribedMethod[] = [];
    for (const methodName of this.metadataScanner.getAllMethodNames(prototype)) {
      const declarations = readSubscribeMetadata(prototype, methodName);
      if (declarations.length > 0) {
        methods.push({ methodName, declarations });
      }
    }
    return methods;
  }

  /**
   * Request-scoped and transient wrappers have no usable instance (a factory
   * wrapper has none at all), so the decorated prototype is taken from the
   * provider class, or from the provider token for factories when it is a class.
   */
  private assertNoHandlers(wrapper: InstanceWrapper): void {
    const type = (wrapper.isFactory ? wrapper.token : wrapper.metatype) as unknown;
    if (typeof type === 'function' && readEventConsumerMetadata(type).length > 0) {
      throw new ConfigurationError(
        `${type.name} uses @EventConsumer but its provider is request-scoped or transient ` +
          `(directly or through a dependency); handlers must be singleton providers`,
      );
    }
    const prototype = typeof type === 'function' ? (type.prototype as object) : null;
    const [first] = prototype === null ? [] : this.subscribedMethods(prototype);
    if (first !== undefined) {
      throw new ConfigurationError(
        `${(prototype as { constructor: { name: string } }).constructor.name}.${first.methodName} ` +
          `uses @Subscribe but its provider is request-scoped or transient ` +
          `(directly or through a dependency); handlers must be singleton providers`,
      );
    }
  }
}
