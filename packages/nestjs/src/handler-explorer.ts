import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import {
  ConfigurationError,
  RelayBus,
  describeCatalog,
  type CatalogEntry,
  type EventHandler,
} from '@relaybus/core';
import { readConsumerMetadata } from './relay-bus-consumer.decorator.js';
import { bindInstance } from './relay-bus-registry.js';
import {
  readHandlerMetadata,
  type RelayBusHandlerMetadata,
} from './relay-bus-handler.decorator.js';

/** What one discovery pass has registered so far. */
type Discovery = {
  /** Where each handler id was registered, to report duplicates. */
  readonly registeredBy: Map<string, string>;
  /** Methods already registered per instance: a `useExisting` alias shows an instance twice. */
  readonly visited: Map<object, Set<string>>;
  readonly consumers: WeakSet<object>;
};

interface SubscribedMethod {
  readonly methodName: string;
  readonly declarations: readonly RelayBusHandlerMetadata[];
}

/**
 * Finds every `@RelayBusHandler` method and every `@RelayBusConsumer` class among the
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
    const discovery: Discovery = {
      registeredBy: new Map<string, string>(),
      visited: new Map<object, Set<string>>(),
      consumers: new WeakSet<object>(),
    };
    for (const wrapper of this.discovery.getProviders()) {
      this.registerProvider(wrapper, discovery);
    }
  }

  private registerProvider(wrapper: InstanceWrapper, discovery: Discovery): void {
    if (wrapper.isTransient || !wrapper.isDependencyTreeStatic()) {
      this.assertNoHandlers(wrapper);
      return;
    }
    const instance = wrapper.instance as unknown;
    if (typeof instance !== 'object' || instance === null) {
      return;
    }
    const prototype = Object.getPrototypeOf(instance) as object | null;
    if (prototype === null) {
      return;
    }
    bindInstance(instance, this.bus);
    const className = instance.constructor.name;
    for (const subscribed of this.subscribedMethods(prototype)) {
      this.registerSubscribedMethod(instance, { subscribedMethod: subscribed, discovery });
    }
    if (!discovery.consumers.has(instance)) {
      discovery.consumers.add(instance);
      this.registerEventConsumers(instance, { className, registeredBy: discovery.registeredBy });
    }
  }

  private registerSubscribedMethod(
    instance: object,
    {
      subscribedMethod: { methodName, declarations },
      discovery: { registeredBy, visited },
    }: { readonly subscribedMethod: SubscribedMethod; readonly discovery: Discovery },
  ): void {
    // A `useExisting` alias exposes the same instance under another token.
    const seen = visited.get(instance) ?? new Set<string>();
    visited.set(instance, seen);
    if (seen.has(methodName)) {
      return;
    }
    seen.add(methodName);
    const method = (instance as Record<string, unknown>)[methodName] as (
      ...args: Parameters<EventHandler>
    ) => unknown;
    const location = `${instance.constructor.name}.${methodName}`;
    for (const { event, options } of declarations) {
      const id = options.id ?? location;
      const previous = registeredBy.get(id);
      if (previous !== undefined) {
        throw new ConfigurationError(
          `Handler id '${id}' is used by both ${previous} and ${location}; ` +
            `pass a unique { id } to @RelayBusHandler`,
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

  private registerEventConsumers(
    instance: object,
    {
      className,
      registeredBy,
    }: { readonly className: string; readonly registeredBy: Map<string, string> },
  ): void {
    const lookup = instance as Record<string, unknown>;
    for (const { catalog, options, selectedEntries } of readConsumerMetadata(
      instance.constructor,
    )) {
      const { name, entries } = describeCatalog(catalog);
      const label = name ?? 'catalog';
      const handlers = selectedEntries ?? entries.filter((entry) => isMethod(lookup, entry));
      const missing = handlers.filter((entry) => !isMethod(lookup, entry));
      if (missing.length > 0) {
        throw new ConfigurationError(
          `${className} uses @RelayBusConsumer(${label}) but lacks the handler methods of events ` +
            `it consumes: ${missing.map((entry) => entry.handlerName).join(', ')}`,
        );
      }
      if (handlers.length === 0) {
        throw new ConfigurationError(
          `${className} uses @RelayBusConsumer(${label}) but defines none of its handler ` +
            `methods (${entries.map((entry) => entry.handlerName).join(', ')})`,
        );
      }
      for (const entry of handlers) {
        const id = `${className}.${entry.handlerName}`;
        const previous = registeredBy.get(id);
        if (previous !== undefined) {
          throw new ConfigurationError(
            `Handler id '${id}' of @RelayBusConsumer(${label}) is already used by ${previous}; ` +
              `give the @RelayBusHandler a unique { id } or drop it`,
          );
        }
        registeredBy.set(id, `${className}.${entry.handlerName}`);
        const method = lookup[entry.handlerName] as (...args: Parameters<EventHandler>) => unknown;
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
      const declarations = readHandlerMetadata(prototype, methodName);
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
    if (typeof type === 'function' && readConsumerMetadata(type).length > 0) {
      throw new ConfigurationError(
        `${type.name} uses @RelayBusConsumer but its provider is request-scoped or transient ` +
          `(directly or through a dependency); handlers must be singleton providers`,
      );
    }
    const prototype = typeof type === 'function' ? (type.prototype as object) : null;
    const [first] = prototype === null ? [] : this.subscribedMethods(prototype);
    if (first !== undefined) {
      throw new ConfigurationError(
        `${(prototype as { constructor: { name: string } }).constructor.name}.${first.methodName} ` +
          `uses @RelayBusHandler but its provider is request-scoped or transient ` +
          `(directly or through a dependency); handlers must be singleton providers`,
      );
    }
  }
}

function isMethod(instance: Record<string, unknown>, entry: CatalogEntry): boolean {
  return typeof instance[entry.handlerName] === 'function';
}
