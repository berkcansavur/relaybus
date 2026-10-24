import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner, ModulesContainer } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import type { Module } from '@nestjs/core/injector/module.js';
import {
  ConfigurationError,
  RelayBus,
  describeCatalog,
  type CatalogEntry,
  type EventHandler,
} from '@relaybus/core';
import { readCatalogConsumerMetadata } from './relay-bus-catalog-consumer.decorator.js';
import { RelayBusFeatureContract } from './relay-bus-feature-contract.js';
import { bindInstance } from './relay-bus-registry.js';
import {
  readConsumerMetadata,
  type RelayBusConsumerMetadata,
} from './relay-bus-consumer.decorator.js';

/** What one discovery pass has registered so far. */
type Discovery = {
  /** Where each handler id was registered, to report duplicates. */
  readonly registeredBy: Map<string, string>;
  /** Methods already registered per instance: a `useExisting` alias shows an instance twice. */
  readonly visited: Map<object, Set<string>>;
  readonly consumers: WeakSet<object>;
  /** Event types the providers of each module consume, for the `forFeature` contracts. */
  readonly consumedByModule: Map<Module | undefined, Set<string>>;
};

interface SubscribedMethod {
  readonly methodName: string;
  readonly declarations: readonly RelayBusConsumerMetadata[];
}

/**
 * Finds every `@RelayBusConsumer` method and every `@RelayBusCatalogConsumer` class among the
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
    @Inject(ModulesContainer) private readonly modules: ModulesContainer,
  ) {}

  onModuleInit(): void {
    const discovery: Discovery = {
      registeredBy: new Map<string, string>(),
      visited: new Map<object, Set<string>>(),
      consumers: new WeakSet<object>(),
      consumedByModule: new Map<Module | undefined, Set<string>>(),
    };
    for (const wrapper of this.discovery.getProviders()) {
      this.registerProvider(wrapper, discovery);
    }
    this.assertModuleContracts(discovery.consumedByModule);
  }

  /** Checks the `RelayBusModule.forFeature` contract of every module that imports one. */
  private assertModuleContracts(consumedByModule: Map<Module | undefined, Set<string>>): void {
    const routed = new Set(
      this.bus
        .handlers()
        .filter((handler) => handler.forwardsToTransport)
        .map((handler) => handler.eventType),
    );
    const violations: string[] = [];
    for (const wrapper of this.discovery.getProviders()) {
      const contract = wrapper.instance as unknown;
      if (!(contract instanceof RelayBusFeatureContract)) {
        continue;
      }
      for (const module of this.importersOf(wrapper.host)) {
        // Every module has a set: Nest registers the module class as one of its providers.
        const consumed = consumedByModule.get(module)!;
        for (const event of contract.consumes.filter(({ type }) => !consumed.has(type))) {
          violations.push(`${module.name}: ${event.label} (${event.type}) has no consumer`);
        }
        for (const event of contract.produces.filter(({ type }) => !routed.has(type))) {
          violations.push(
            `${module.name}: ${event.label} (${event.type}) has no route to a transport`,
          );
        }
      }
    }
    if (violations.length > 0) {
      throw new ConfigurationError(
        `[RelayBusHandlerExplorer](assertModuleContracts) ${violations.join('; ')}`,
      );
    }
  }

  private importersOf(contractModule: Module | undefined): Module[] {
    // A provider always has a host module; `has(undefined)` is false for the type's sake.
    return [...this.modules.values()].filter((module) => module.imports.has(contractModule!));
  }

  /** Records the event types the provider of `wrapper` consumes under the provider's module. */
  private recordConsumedTypes(
    wrapper: InstanceWrapper,
    { instance, discovery }: { readonly instance: object; readonly discovery: Discovery },
  ): void {
    const types = discovery.consumedByModule.get(wrapper.host) ?? new Set<string>();
    discovery.consumedByModule.set(wrapper.host, types);
    const lookup = instance as Record<string, unknown>;
    for (const { declarations } of this.subscribedMethods(
      Object.getPrototypeOf(instance) as object,
    )) {
      for (const { event } of declarations) {
        types.add(typeof event === 'string' ? event : event.type);
      }
    }
    for (const { catalog, selectedEntries } of readCatalogConsumerMetadata(instance.constructor)) {
      const entries =
        selectedEntries ??
        describeCatalog(catalog).entries.filter((entry) => isMethod(lookup, entry));
      for (const entry of entries) {
        types.add(entry.eventClass.type);
      }
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
    this.recordConsumedTypes(wrapper, { instance, discovery });
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
            `pass a unique { id } to @RelayBusConsumer`,
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
    for (const { catalog, options, selectedEntries } of readCatalogConsumerMetadata(
      instance.constructor,
    )) {
      const { name, entries } = describeCatalog(catalog);
      const label = name ?? 'catalog';
      const handlers = selectedEntries ?? entries.filter((entry) => isMethod(lookup, entry));
      const missing = handlers.filter((entry) => !isMethod(lookup, entry));
      if (missing.length > 0) {
        throw new ConfigurationError(
          `${className} uses @RelayBusCatalogConsumer(${label}) but lacks the handler methods of events ` +
            `it consumes: ${missing.map((entry) => entry.handlerName).join(', ')}`,
        );
      }
      if (handlers.length === 0) {
        throw new ConfigurationError(
          `${className} uses @RelayBusCatalogConsumer(${label}) but defines none of its handler ` +
            `methods (${entries.map((entry) => entry.handlerName).join(', ')})`,
        );
      }
      for (const entry of handlers) {
        const id = `${className}.${entry.handlerName}`;
        const previous = registeredBy.get(id);
        if (previous !== undefined) {
          throw new ConfigurationError(
            `Handler id '${id}' of @RelayBusCatalogConsumer(${label}) is already used by ${previous}; ` +
              `give the @RelayBusConsumer a unique { id } or drop it`,
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
      const declarations = readConsumerMetadata(prototype, methodName);
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
    if (typeof type === 'function' && readCatalogConsumerMetadata(type).length > 0) {
      throw new ConfigurationError(
        `${type.name} uses @RelayBusCatalogConsumer but its provider is request-scoped or transient ` +
          `(directly or through a dependency); handlers must be singleton providers`,
      );
    }
    const prototype = typeof type === 'function' ? (type.prototype as object) : null;
    const [first] = prototype === null ? [] : this.subscribedMethods(prototype);
    if (first !== undefined) {
      throw new ConfigurationError(
        `${(prototype as { constructor: { name: string } }).constructor.name}.${first.methodName} ` +
          `uses @RelayBusConsumer but its provider is request-scoped or transient ` +
          `(directly or through a dependency); handlers must be singleton providers`,
      );
    }
  }
}

function isMethod(instance: Record<string, unknown>, entry: CatalogEntry): boolean {
  return typeof instance[entry.handlerName] === 'function';
}
